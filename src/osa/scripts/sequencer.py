#!/usr/bin/env python3
"""
Sequencer: orchestrates r0->dl1 arrays, per-run CatB/tailcuts pilots and dl1ab arrays.

Behavior:
 - For each DATA run:
   * submit r0->dl1 array job if not completed/active
   * submit CatB/tailcuts pilot dependent on r0 (if needed)
   * submit dl1ab array dependent on CatB (if needed) or r0 (if no CatB)
 - Keeps per-subrun history entries as before
 - Produces a textual sequencer snapshot
 - Array stdout/stderr filenames include subrun (%a) and array job id (%A)
 - Honors --simulate, --test and --force-submit
"""

import datetime
import errno
import logging
import os
import re
import subprocess as sp
import sys
import warnings
from decimal import Decimal
from pathlib import Path
from typing import Optional

from osa.configs import options
from osa.configs.config import cfg
from osa.job import (
    are_all_jobs_correctly_finished,
    determine_array_job_status,
    get_sacct_output,
    get_squeue_output,
    job_is_active,
    pilot_job_is_active,
    run_sacct,
    run_squeue,
    set_queue_values,
    write_catb_pilot_script,
    write_dl1ab_wrapper_script,
    write_r0_script,
)
from osa.nightsummary.extract import build_sequences
from osa.nightsummary.nightsummary import run_summary_table
from osa.paths import (
    analysis_path,
    catB_closed_file_exists,
    destination_dir,
    get_dl1_prod_id_and_config,
    get_drive_file,
    get_major_version,
    get_summary_file,
)
from osa.processing_plan import build_processing_plan
from osa.utils.cliopts import sequencer_cli_parsing
from osa.utils.logging import myLogger
from osa.utils.utils import date_to_iso, date_to_dir, get_lstchain_version, gettag
from osa.veto import get_closed_list, get_veto_list

warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API.*",
    category=UserWarning,
)

log = myLogger(logging.getLogger(__name__))


def _safe_write_text(path: Path, content: str, mode: str = "w", encoding: str = "utf-8"):
    """
    Ensure parent exists and write content to path. Log and raise on failure.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.exception(f"Could not create directory for {path.parent}: {e}")
        raise

    try:
        with path.open(mode, encoding=encoding) as fh:
            fh.write(content)
    except Exception as e:
        log.exception(f"Failed to write file {path}: {e}")
        raise


def _sbatch_submit(script_path: Path, dependency: Optional[str] = None, simulate: bool = False) -> Optional[str]:
    """
    Submit a script via sbatch with atomic protection against duplicate submissions
    for the same script path.
    """
    marker = Path(options.directory) / f".{script_path.name}.pending"

    # Atomic reservation
    try:
        fd = os.open(str(marker), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # Another process is already submitting this same script.
            # Read the marker if it contains a jobid; if so, check it is still active.
            try:
                data = marker.read_text(encoding="utf-8").splitlines()
                if len(data) >= 2 and data[0].strip():
                    jobid = data[0].strip()
                    state = get_sacct_output(run_sacct(job_id=jobid))["State"].iloc[0]
                    if state in ("RUNNING", "PENDING", "COMPLETING"):
                        log.info(f"Script {script_path.name} already pending/running (job {jobid}); skipping duplicate submission.")
                        return jobid
            except Exception:
                pass

            # if marker exists but is stale or unreadable, do not allow duplicate submits
            log.info(f"Script {script_path.name} already reserved; skipping duplicate submission.")
            return None
        else:
            log.exception(f"Could not create marker for {script_path.name}: {e}")
            return None

    try:
        cmd = ["sbatch", "--parsable"]
        if dependency:
            cmd.append(f"--dependency=afterok:{dependency}")
        cmd.append(str(script_path))

        if simulate:
            log.info(f"[SIMULATE] Would run: {' '.join(cmd)}")
            marker.write_text("SIMULATE\n", encoding="utf-8")
            return None

        proc = sp.run(cmd, capture_output=True, text=True, check=True)
        jobid = proc.stdout.strip()
        log.info(f"sbatch submitted: {script_path.name} -> job {jobid}")
        marker.write_text(f"{jobid}\n", encoding="utf-8")
        return jobid

    except sp.CalledProcessError as e:
        log.exception(f"sbatch failed for {script_path}: {e}; stdout: {e.stdout}; stderr: {e.stderr}")
        try:
            marker.unlink()
        except Exception:
            pass
        return None

    except Exception:
        log.exception(f"Unexpected error submitting {script_path}")
        try:
            marker.unlink()
        except Exception:
            pass
        return None


def _job_active_in_sacct(jobname_pattern: str) -> bool:
    """
    Generic SLURM-active check for a given job name.
    Kept as requested.
    """
    try:
        squeue_output = run_squeue()
        squeue_info = get_squeue_output(squeue_output)

        jobs = squeue_info[squeue_info["JobName"] == jobname_pattern]
        if not jobs.empty:
            return True

    except Exception:
        pass

    try:
        sacct_output = run_sacct()
        sacct_info = get_sacct_output(sacct_output)

        jobs = sacct_info[sacct_info["JobName"] == jobname_pattern]
        states = set(jobs["State"].astype(str))
        return any(s in ("RUNNING", "PENDING", "COMPLETING") for s in states)

    except Exception:
        return True

    return False


def format_sequence_table(sequence_list) -> str:
    """
    Build the same table as report_sequences but return it as a formatted string.
    """
    header = [
        "Tel",
        "Seq",
        "Parent",
        "Type",
        "Run",
        "Subruns",
        "Source",
        "Action",
        "Tries",
        "JobID",
        "State",
        "CPU_time",
        "Exit",
    ]
    if options.tel_id in ["LST1", "LST2"]:
        header.extend(("DL1%", "MUONS%", "CAT-B", "DL1AB%", "DATACHECK%", "DL2%"))

    matrix = [header]

    for sequence in sequence_list:
        row_list = [
            getattr(sequence, "telescope", None),
            getattr(sequence, "seq", None),
            getattr(sequence, "parent", None),
            getattr(sequence, "type", None),
            getattr(sequence, "run", None),
            getattr(sequence, "subruns", None),
            getattr(sequence, "source_name", None),
            getattr(sequence, "action", None),
            getattr(sequence, "tries", None),
            getattr(sequence, "jobid", None),
            getattr(sequence, "state", None),
            getattr(sequence, "cputime", None),
            getattr(sequence, "exit", None),
        ]

        if getattr(sequence, "type", None) in ["DRS4", "PEDCALIB"]:
            row_list.extend((None, None, None, None, None, None))
        elif getattr(sequence, "type", None) == "DATA":
            dl1s = getattr(sequence, "dl1status", None)
            muons = getattr(sequence, "muonstatus", None)
            catb = getattr(sequence, "catbstatus", None)
            dl1ab = getattr(sequence, "dl1abstatus", None)
            datacheck = getattr(sequence, "datacheckstatus", None)
            dl2 = getattr(sequence, "dl2status", None)
            row_list.extend((dl1s, muons, catb, dl1ab, datacheck, dl2))

        matrix.append(row_list)

    padding = int(cfg.get("OUTPUT", "PADDING"))
    max_field_length = []

    for row in matrix:
        for j, col in enumerate(row):
            col_str = "" if col is None else str(col)
            length = len(col_str)
            if len(max_field_length) <= j:
                max_field_length.append(length)
            elif length > max_field_length[j]:
                max_field_length[j] = length

    out_lines = []
    for row in matrix:
        stringrow = ""
        rpadding = padding * " "
        for j, col in enumerate(row):
            col_str = "" if col is None else str(col)
            lpad = (max_field_length[j] - len(col_str)) * " "
            if isinstance(col, int):
                stringrow += f"{lpad}{col}{rpadding}"
            else:
                stringrow += f"{col_str}{lpad}{rpadding}"
        out_lines.append(stringrow)

    return "\n".join(out_lines) + "\n"


def update_job_info(sequence_list):
    """
    Update SLURM information associated with each sequence.
    """
    if options.test:
        return

    try:
        sacct_output = run_sacct()
        squeue_output = run_squeue()
        set_queue_values(
            sacct_info=get_sacct_output(sacct_output),
            squeue_info=get_squeue_output(squeue_output),
            sequence_list=sequence_list,
        )
    except Exception:
        log.exception("Failed to update SLURM job information")


def get_status_for_sequence(sequence, data_level) -> int:
    """
    Get number of files produced for a given sequence and data level.
    """
    try:
        if data_level == "DL1AB":
            directory = options.directory / sequence.dl1_prod_id
            files = list(directory.glob(f"dl1_LST-1*{sequence.run}*.h5"))
        elif data_level == "DL2":
            directory = destination_dir(concept="DL2", create_dir=False, dl2_prod_id=sequence.dl2_prod_id)
            files = list(directory.glob(f"dl2_LST-1*{sequence.run}*.h5"))
        elif data_level == "DATACHECK":
            try:
                directory = options.directory / sequence.dl1_prod_id
                files = list(directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))
            except Exception:
                files = []
            try:
                alternative_directory = destination_dir(
                    concept="DATACHECK",
                    create_dir=False,
                    dl1_prod_id=sequence.dl1_prod_id,
                )
                files += list(alternative_directory.glob(f"datacheck_dl1_LST-1*{sequence.run}*.h5"))
            except Exception:
                pass
        else:
            prefix = cfg.get("PATTERN", f"{data_level}PREFIX")
            suffix = cfg.get("PATTERN", f"{data_level}SUFFIX")
            files = list(options.directory.glob(f"{prefix}*{sequence.run}*{suffix}"))
    except AttributeError:
        return 0
    except Exception:
        log.debug(
            f"get_status_for_sequence: unexpected error for run {getattr(sequence,'run',None)} and level {data_level}",
            exc_info=True,
        )
        return 0

    return len(files)


def check_catB_status(seq):
    """
    Determine catB status for a DATA sequence:
      - CLOSED if a catB*<run>*.closed file exists in options.directory
      - otherwise, if a catB log exists in options.log_directory, extract job id and query sacct to get job state.
    """
    catbstatus = "None"

    if seq.type == "DATA":
        directory = options.directory
        closed_files = list(directory.glob(f"catB*{seq.run}*.closed"))
        if closed_files:
            catbstatus = "CLOSED"
        else:
            log_files = list(options.log_directory.glob(f"catB_calibration_{seq.run}_*.err"))
            if log_files:
                filename = sorted(log_files)[-1].name
                match = re.search(f"catB_calibration_{seq.run}_(\\d+).err", filename)
                if match:
                    job_id = match.group(1)
                    try:
                        sacct_output = run_sacct(job_id)
                        sacct_info = get_sacct_output(sacct_output)
                        if not sacct_info.empty:
                            catbstatus = sacct_info.iloc[0]["State"]
                    except Exception:
                        log.debug(f"check_catB_status: could not query sacct for job {job_id}", exc_info=True)

    return catbstatus


def update_sequence_status(seq_list):
    """
    Update the percentage of files produced of each type for every run.
    """
    for seq in seq_list:
        try:
            if seq.type == "PEDCALIB":
                denom = seq.subruns if seq.subruns else 1
                seq.calibstatus = int(Decimal(get_status_for_sequence(seq, "CALIB") * 100) / denom)
            elif seq.type == "DATA":
                denom = seq.subruns if seq.subruns else 1
                seq.dl1status = int(Decimal(get_status_for_sequence(seq, "DL1") * 100) / denom)
                seq.dl1abstatus = int(Decimal(get_status_for_sequence(seq, "DL1AB") * 100) / denom)
                seq.datacheckstatus = int(Decimal(get_status_for_sequence(seq, "DATACHECK") * 100) / denom)
                seq.muonstatus = int(Decimal(get_status_for_sequence(seq, "MUON") * 100) / denom)
                seq.dl2status = int(Decimal(get_status_for_sequence(seq, "DL2") * 100))
                seq.catbstatus = check_catB_status(seq)
        except Exception:
            log.exception(f"Could not update status for sequence run {getattr(seq,'run',None)}")


def _write_run_summary_line(run_dir: Path, tel: str, run: int, kind: str, status: int):
    """
    Helper to append a run summary line for array statuses.
    """
    try:
        summary_file = run_dir / f"{kind.lower()}_{tel}_{run:05d}.status"
        with summary_file.open("a", encoding="utf-8") as fh:
            fh.write(f"{status}\n")
    except Exception:
        log.debug(f"Could not write run summary line for {kind} {tel} {run}")


def single_process(telescope: str):
    sequencer_cli_parsing()
    options.tel_id = telescope
    options.directory = analysis_path(options.tel_id)
    options.log_directory = options.directory / "log"

    options.directory.mkdir(parents=True, exist_ok=True)
    if not options.simulate:
        options.log_directory.mkdir(parents=True, exist_ok=True)

    log.debug(f"options.directory = {options.directory} (exists={os.access(str(options.directory), os.W_OK)})")
    log.info(f"Starting sequencer for {options.tel_id} on date {date_to_iso(options.date)} (input_state={options.input_state})")

    summary_table = run_summary_table(options.date)
    if len(summary_table) == 0:
        log.warning("No runs found for this date. Nothing to do.")
        return []

    sequence_list = build_sequences(options.date)
    get_veto_list(sequence_list)
    get_closed_list(sequence_list)

    try:
        update_job_info(sequence_list)
    except Exception:
        log.exception("Could not update job info")

    try:
        update_sequence_status(sequence_list)
    except Exception:
        log.exception("Could not update sequence status")

    try:
        sacct_output = run_sacct()
        sacct_info = get_sacct_output(sacct_output)
    except Exception:
        sacct_info = None

    for seq in sequence_list:
        if seq.type != "DATA":
            continue

        run = seq.run
        tel = options.tel_id
        run_dir = options.directory

        jobname_r0 = f"{tel}_{run:05d}"
        status_r0 = determine_array_job_status(sacct_info, jobname_r0)
        if status_r0 is not None:
            _write_run_summary_line(run_dir, tel, run, "R0_ARRAY", status_r0)

        jobname_dl1ab = f"{tel}_dl1ab_{run:05d}"
        status_dl1ab = determine_array_job_status(sacct_info, jobname_dl1ab)
        if status_dl1ab is not None:
            _write_run_summary_line(run_dir, tel, run, "DL1AB_ARRAY", status_dl1ab)

    account = cfg.get("SLURM", "ACCOUNT")

    for seq in sequence_list:
        if seq.type != "DATA":
            continue

        run_id = seq.run
        jobname_r0 = f"{options.tel_id}_{run_id:05d}"
        jobname_catb = f"{options.tel_id}_catB_tailcuts_{run_id:05d}"
        jobname_dl1ab = f"{options.tel_id}_dl1ab_{run_id:05d}"

        history_files = sorted(options.directory.glob(f"sequence_{options.tel_id}_{run_id:05d}.*.history"))
        r0_completed = True
        if not history_files:
            r0_completed = False
        else:
            for hf in history_files:
                try:
                    lines = hf.read_text(encoding="utf-8").splitlines()
                except Exception:
                    r0_completed = False
                    break
                found = any("lstchain_data_r0_to_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    r0_completed = False
                    break

        jobid_r0 = None
        if not r0_completed:
            if job_is_active(jobname_r0):
                log.info(f"r0->dl1 already active for run {run_id:05d} (jobname {jobname_r0}), skipping r0 submit.")
                try:
                    sacct_output = run_sacct()
                    sacct_df = get_sacct_output(sacct_output)
                    jobs_run = sacct_df[sacct_df["JobName"] == jobname_r0]
                    jobid_r0 = str(int(jobs_run["JobID"].max())) if not jobs_run.empty else None
                except Exception:
                    jobid_r0 = None
            else:
                r0_script = write_r0_script(seq, work_dir=options.directory, simulate=options.simulate)
                jobid_r0 = _sbatch_submit(r0_script, dependency=None, simulate=options.simulate)
        else:
            log.debug(f"r0->dl1 already completed for run {run_id:05d}.")

        need_catb = cfg.getboolean("lstchain", "apply_catB_calibration") and not catB_closed_file_exists(run_id)
        tailcuts_cfg = Path(cfg.get(options.tel_id, "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run_id:05d}.json"
        need_tailcuts = (not cfg.getboolean("lstchain", "apply_standard_dl1b_config")) and (not tailcuts_cfg.exists())

        jobid_catb = None
        if need_catb or need_tailcuts:
            if pilot_job_is_active(run_id):
                log.info(f"CatB pilot already active for run {run_id:05d}, skipping CatB submit.")
                try:
                    sacct_output = run_sacct()
                    sacct_df = get_sacct_output(sacct_output)
                    jobs_run = sacct_df[sacct_df["JobName"] == jobname_catb]
                    jobid_catb = str(int(jobs_run["JobID"].max())) if not jobs_run.empty else None
                except Exception:
                    jobid_catb = None
            else:
                dep = jobid_r0
                if dep is None and not options.force_submit and not r0_completed:
                    log.info(f"No r0 job visible yet for run {run_id:05d}; skipping CatB submission until r0 is present (or use --force-submit).")
                else:
                    catb_script = write_catb_pilot_script(run_id)
                    jobid_catb = _sbatch_submit(catb_script, dependency=dep, simulate=options.simulate)
        else:
            log.debug(f"No CatB/tailcuts needed for run {run_id:05d}.")

        fully_processed = True
        if not history_files:
            fully_processed = False
        else:
            for hf in history_files:
                try:
                    lines = hf.read_text(encoding="utf-8").splitlines()
                except Exception:
                    fully_processed = False
                    break
                found = any("lstchain_check_dl1" in l and l.strip().endswith(" 0") for l in lines)
                if not found:
                    fully_processed = False
                    break

        if fully_processed:
            log.info(f"Run {run_id:05d} already fully processed, skipping dl1ab.")
            continue

        if _job_active_in_sacct(jobname_dl1ab):
            log.info(f"dl1ab already active for run {run_id:05d}, skipping dl1ab submit.")
            continue

        dep_for_dl1 = None
        if need_catb:
            if catB_closed_file_exists(run_id):
                dep_for_dl1 = None
                log.debug(f"CatB already closed for run {run_id:05d}; submitting dl1ab without dependency.")
            elif jobid_catb:
                dep_for_dl1 = jobid_catb
            else:
                if options.force_submit and jobid_r0:
                    dep_for_dl1 = jobid_r0
                    log.warning(f"Force-submitting dl1ab for run {run_id:05d} with dependency on r0 ({jobid_r0}) even though CatB is not yet closed/active.")
                else:
                    log.info(f"CatB required for run {run_id:05d} but no catB job or .closed found; skipping dl1ab.")
                    continue
        else:
            if jobid_r0:
                dep_for_dl1 = jobid_r0
            elif r0_completed:
                dep_for_dl1 = None
                log.debug(f"r0 already completed for run {run_id:05d}; submitting dl1ab without dependency.")
            else:
                if options.force_submit:
                    dep_for_dl1 = None
                    log.warning(f"Force-submitting dl1ab for run {run_id:05d} without dependency.")
                else:
                    log.info(f"No r0 job available and r0 not completed for run {run_id:05d}; skipping dl1ab.")
                    continue

        dl1_prod_id, dl1b_config = get_dl1_prod_id_and_config(run_id)
        dl1ab_script = write_dl1ab_wrapper_script(
            run_id=run_id,
            work_dir=options.directory,
            simulate=options.simulate,
            subruns=seq.subruns,
            dl1_prod_id=dl1_prod_id,
            dl1b_config=dl1b_config,
        )
        _sbatch_submit(dl1ab_script, dependency=dep_for_dl1, simulate=options.simulate)

    try:
        try:
            update_sequence_status(sequence_list)
        except Exception:
            log.exception("Could not refresh sequence status before writing table")

        table_str = format_sequence_table(sequence_list)
        print(table_str)

        table_file = options.directory / "sequencer_table.txt"
        if not options.simulate:
            with open(table_file, "w", encoding="utf-8") as fh:
                fh.write(table_str)
            stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            logfile = options.log_directory / f"sequencer_table_{stamp}.log"
            with open(logfile, "w", encoding="utf-8") as fh:
                fh.write(table_str)
            log.info(f"Saved sequencer table to {table_file} and {logfile}")
        else:
            log.info("[SIMULATE] Would write sequencer table to disk")
    except Exception:
        log.exception("Could not write sequencer table to disk")

    return sequence_list


def main():
    sequencer_cli_parsing()
    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    single_array = ["LST1", "LST2"]
    tag = gettag()
    log.info(
        f"=================================== Starting sequencer.py at "
        f"{datetime.datetime.utcnow():%Y-%m-%d %H:%M} UTC for LST, "
        f"Telescope: {options.tel_id}, Date: {date_to_iso(options.date)} "
        f"==================================="
    )

    if options.tel_id in single_array:
        single_process(options.tel_id)
    else:
        log.error("Process mode not supported yet")


if __name__ == "__main__":
    main()
