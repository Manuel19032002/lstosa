
def _write_r0_script(seq, work_dir: Path, account: str, simulate: bool) -> Path:
    """Write r0->dl1 array script. Force --no-dl1ab and pass --input-state. Append per-subrun history entry."""
    run_id = seq.run
    job_name = f"{options.tel_id}_{run_id:05d}"
    script_path = work_dir / f"sequence_{options.tel_id}_{run_id:05d}.py"
    # ensure work_dir exists
    script_path.parent.mkdir(parents=True, exist_ok=True)

    subruns_count = max(0, seq.subruns - 1)
    array_spec = f"0-{subruns_count}" if subruns_count >= 0 else None

    flat_date = date_to_dir(options.date)
    args = ["datasequence"]
    if options.verbose:
        args.append("-v")
    if simulate:
        args.append("-s")
    if options.configfile:
        args.extend(["--config", str(Path(options.configfile).resolve())])

    args.append(f"--input-state={options.input_state}")
    args.append("--no-dl1ab")  # ensure r0 array does not run dl1ab

    args.extend(
        (
            f"--date={date_to_iso(options.date)}",
            f"--prod-id={options.prod_id}",
            f"--drs4-pedestal-file={seq.drs4_file}",
            f"--time-calib-file={seq.time_calibration_file}",
            f"--pedcal-file={seq.calibration_file}",
            f"--systematic-correction-file={seq.systematic_correction_file}",
            f"--drive-file={get_drive_file(flat_date)}",
            f"--run-summary={get_summary_file(flat_date)}",
        )
    )

    try:
        from osa.paths import pedestal_ids_file_exists, get_pedestal_ids_file

        if pedestal_ids_file_exists(run_id):
            pedfile = get_pedestal_ids_file(run_id, flat_date)
            args.append(f"--pedestal-ids-file={pedfile}")
    except Exception:
        pass

    header = _make_script_header(job_name, work_dir, account, array_spec=array_spec)
    content = header
    content += "import os, subprocess, sys, tempfile, datetime\n\n"
    content += "from pathlib import Path\n\n"
    content += "if 'SLURM_ARRAY_TASK_ID' in os.environ:\n"
    content += "    subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    content += "else:\n"
    content += "    subruns = 0\n\n"
    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += "    os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    content += "    proc = subprocess.run([\n"
    for a in args:
        content += f"        {a!r},\n"
    content += f"        f'{run_id:05d}.{{subruns:04d}}',\n"
    content += f"        {options.tel_id!r}\n"
    content += "    ])\n"
    content += "    rc = proc.returncode\n"
    # Append per-subrun history to the per-subrun history file
    content += "    try:\n"
    content += "        _hist = Path(os.getcwd()) / f'sequence_{options.tel_id}_{run_id:05d}.{subruns:04d}.history'\n"
    content += "        _ts = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')\n"
    content += "        try:\n"
    content += "            from osa.utils.utils import get_lstchain_version\n"
    content += "            from osa.paths import get_major_version\n"
    content += "            _ver = get_major_version(get_lstchain_version())\n"
    content += "        except Exception:\n"
    content += "            _ver = 'unknown'\n"
    content += "        with open(_hist, 'a') as _fh:\n"
    content += "            _fh.write(f'{run_id:05d} lstchain_data_r0_to_dl1 {_ver} {_ts} None None {rc}\\n')\n"
    content += "    except Exception as _e:\n"
    content += "        print(f'WARNING: could not append subrun history: {_e}', file=sys.stderr)\n"
    content += "sys.exit(rc)\n"

    # write script safely
    _safe_write_text(script_path, content)
    try:
        script_path.chmod(0o755)
    except Exception:
        log.warning(f"Could not chmod {script_path}")
    log.debug(f"Wrote r0 script {script_path}")
    return script_path












def _write_dl1ab_wrapper_script(run_id: int, work_dir: Path, account: str, simulate: bool, subruns: int) -> Path:
    """
    Write dl1ab wrapper script that:
     - loads sequencer cfg (if provided) so TailCuts dir is correct,
     - initializes osa.configs.options for runtime,
     - resolves dl1_prod_id and dl1b_config at runtime (single check, fallback to auxiliary/TailCuts),
     - calls datasequence with flags expected by datasequence for dl1ab,
     - appends per-subrun entry to per-subrun history file (lstchain_check_dl1).
    """
    job_name = f"{options.tel_id}_dl1ab_{run_id:05d}"
    script_path = work_dir / f"sequence_{options.tel_id}_{run_id:05d}_dl1ab.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)

    array_spec = f"0-{max(0, subruns - 1)}" if subruns > 0 else "0-0"
    header = _make_script_header(job_name, work_dir, account, array_spec=array_spec)

    date_literal = date_to_iso(options.date)
    prod_id_literal = options.prod_id if hasattr(options, "prod_id") else ""

    content = header
    content += "import os, subprocess, sys, tempfile, datetime, json\n\n"
    content += "from pathlib import Path\n"
    # If sequencer provided a config file, load it so cfg.get(...) returns expected paths
    if options.configfile:
        cfg_path = str(Path(options.configfile).resolve())
        content += "try:\n"
        content += f"    from osa.configs import config as config_module\n"
        content += f"    config_module.cfg.read({cfg_path!r})\n"
        content += "except Exception as e:\n"
        content += "    sys.stderr.write(f'WARNING: could not read cfg in wrapper: {e}\\n')\n\n"
    content += "try:\n"
    content += "    from osa.paths import get_dl1_prod_id_and_config, get_dl1_prod_id\n"
    content += "except Exception:\n"
    content += "    get_dl1_prod_id_and_config = None\n    get_dl1_prod_id = None\n\n"
    content += "if 'SLURM_ARRAY_TASK_ID' in os.environ:\n"
    content += "    subruns = int(os.getenv('SLURM_ARRAY_TASK_ID'))\n"
    content += "else:\n"
    content += "    subruns = 0\n\n"
    content += f"run = {run_id}\n"
    content += f"tel = {options.tel_id!r}\n\n"
    content += "# Initialize osa.configs.options in this runtime so osa.paths can use options.tel_id etc.\n"
    content += "try:\n"
    content += "    from osa.configs import options as osa_options\n"
    content += "    osa_options.tel_id = tel\n"
    content += "    osa_options.directory = Path(os.getcwd())\n"
    content += f"    osa_options.prod_id = {prod_id_literal!r}\n"
    content += f"    osa_options.date = datetime.datetime.strptime({date_literal!r}, '%Y-%m-%d')\n"
    content += "except Exception:\n"
    content += "    pass\n\n"
    content += "if get_dl1_prod_id_and_config is None:\n"
    content += "    raise RuntimeError('Could not import osa.paths.get_dl1_prod_id_and_config in dl1ab wrapper')\n\n"
    # Determine dl1b config path (single check; primary configured dir then auxiliary)
    content += "filename = f'dl1ab_Run{run:05d}.json'\n"
    content += "found = None\n"
    content += "tried = []\n"
    content += "try:\n"
    content += "    tailcuts_dir = Path(__import__('osa').configs.config.cfg.get(tel, 'TAILCUTS_FINDER_DIR'))\n"
    content += "    candidate = tailcuts_dir / filename\n"
    content += "    tried.append(candidate)\n"
    content += "    if candidate.exists():\n"
    content += "        found = candidate\n"
    content += "except Exception:\n"
    content += "    pass\n\n"
    content += "if not found:\n"
    content += "    cwd = Path(os.getcwd())\n"
    content += "    for ancestor in list(cwd.parents)[:6]:\n"
    content += "        alt = ancestor / 'auxiliary' / 'TailCuts' / filename\n"
    content += "        tried.append(alt)\n"
    content += "        if alt.exists():\n"
    content += "            found = alt\n"
    content += "            break\n\n"
    content += "if not found:\n"
    content += "    sys.stderr.write('Wrapper: dl1b config not found. Paths tried:\\n')\n"
    content += "    for p in tried:\n"
    content += "        sys.stderr.write(str(p) + '\\n')\n"
    content += "    raise RuntimeError(f'The dl1b config file was not created yet for run {run:05d}.')\n\n"
    content += "dl1b_config = found.resolve()\n"
    content += "try:\n"
    content += "    dl1_prod_id = get_dl1_prod_id(dl1b_config)\n"
    content += "except Exception:\n"
    content += "    with open(dl1b_config) as fh:\n"
    content += "        data = json.load(fh)\n"
    content += "    pic = data['tailcuts_clean_with_pedestal_threshold']['picture_thresh']\n"
    content += "    bnd = data['tailcuts_clean_with_pedestal_threshold']['boundary_thresh']\n"
    content += "    prefix = __import__('osa').configs.config.cfg.get(tel, 'DL1_PROD_ID') if __import__('osa').configs.config.cfg.has_option(tel, 'DL1_PROD_ID') else __import__('osa').configs.config.cfg.get('LST1','DL1_PROD_ID')\n"
    content += "    if bnd == 4:\n"
    content += "        dl1_prod_id = f'{prefix}{pic}{bnd}'\n"
    content += "    else:\n"
    content += "        dl1_prod_id = f'{prefix}{pic}{bnd:02d}'\n\n"
    content += "run_str = f'{run:05d}.{subruns:04d}'\n"
    content += "cmd = [\n"
    content += "    'datasequence',\n"
    if options.verbose:
        content += "    '-v',\n"
    if simulate:
        content += "    '-s',\n"
    if options.configfile:
        content += f"    '--config', {repr(str(Path(options.configfile).resolve()))},\n"
    content += f"    '--input-state={options.input_state}',\n"
    content += f"    '--date={date_literal}',\n"
    content += f"    '--prod-id={prod_id_literal}',\n"
    content += "    f'--dl1b-config={dl1b_config}',\n"
    content += "    f'--dl1-prod-id={dl1_prod_id}',\n"
    content += "    run_str,\n"
    content += "    tel,\n"
    content += "]\n\n"
    content += "with tempfile.TemporaryDirectory() as tmpdirname:\n"
    content += "    os.environ['NUMBA_CACHE_DIR'] = tmpdirname\n"
    content += "    proc = subprocess.run(cmd)\n"
    content += "    rc = proc.returncode\n"
    # Append per-subrun check_dl1 entry to per-subrun history
    content += "    try:\n"
    content += "        _hist = Path(os.getcwd()) / f'sequence_{tel}_{run:05d}.{subruns:04d}.history'\n"
    content += "        _ts = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')\n"
    content += "        try:\n"
    content += "            from osa.utils.utils import get_lstchain_version\n"
    content += "            from osa.paths import get_major_version\n"
    content += "            _ver = get_major_version(get_lstchain_version())\n"
    content += "        except Exception:\n"
    content += "            _ver = 'unknown'\n"
    content += "        with open(_hist, 'a') as _fh:\n"
    content += "            _fh.write(f'{run:05d} lstchain_check_dl1 {_ver} {_ts} None None {rc}\\n')\n"
    content += "    except Exception as _e:\n"
    content += "        print(f'WARNING: could not append subrun history: {_e}', file=sys.stderr)\n"
    content += "sys.exit(rc)\n"

    _safe_write_text(script_path, content)
    try:
        script_path.chmod(0o755)
    except Exception:
        log.warning(f"Could not chmod {script_path}")
    log.debug(f"Wrote dl1ab wrapper script {script_path}")
    return script_path


