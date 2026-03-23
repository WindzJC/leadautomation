from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = PROJECT_ROOT / "state"
STATE_PATH = STATE_DIR / "runner_state.json"
RUN_FORM_STATE_PATH = STATE_DIR / "run_form_state.json"
LEGACY_STATE_DIR = PROJECT_ROOT / ".dashboard_state"
LEGACY_STATE_PATH = LEGACY_STATE_DIR / "runner_state.json"
LEGACY_RUN_FORM_STATE_PATH = LEGACY_STATE_DIR / "run_form_state.json"

DEFAULT_RUN_CONFIG = {
    "validation_profile": "strict_full",
    "listing_strict": True,
    "goal_final": 20,
    "max_runs": 1,
    "max_stale_runs": 1,
    "target": 80,
    "min_candidates": 80,
    "max_candidates": 80,
    "batch_min": 20,
    "batch_max": 20,
    "queries_file": "",
}


def suggested_run_folder_name() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"browser_run_{stamp}"


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_run_state() -> dict[str, Any]:
    path = STATE_PATH if STATE_PATH.is_file() else LEGACY_STATE_PATH
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_run_state(payload: dict[str, Any]) -> None:
    _ensure_state_dir()
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_run_form_state() -> dict[str, Any]:
    path = RUN_FORM_STATE_PATH if RUN_FORM_STATE_PATH.is_file() else LEGACY_RUN_FORM_STATE_PATH
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_run_form_state(payload: dict[str, Any]) -> None:
    _ensure_state_dir()
    RUN_FORM_STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def process_is_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.is_file():
        try:
            stat_text = proc_stat.read_text(encoding="utf-8", errors="replace")
            parts = stat_text.split()
            if len(parts) >= 3 and parts[2] == "Z":
                return False
        except OSError:
            pass
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def build_run_paths(run_root: Path) -> dict[str, Path]:
    outputs_root = run_root / "outputs"
    csv_root = outputs_root / "csv"
    logs_root = outputs_root / "logs"
    return {
        "root": run_root,
        "outputs_root": outputs_root,
        "csv_root": csv_root,
        "logs_root": logs_root,
        "master_output": csv_root / "leads_full.csv",
        "minimal_output": csv_root / "author_email_source.csv",
        "scouted_output": csv_root / "scouted_leads.csv",
        "contact_queue_output": csv_root / "contact_queue.csv",
        "near_miss_location_output": csv_root / "near_miss_location.csv",
        "verified_output": csv_root / "fully_verified_leads.csv",
        "runs_dir": outputs_root / "runs",
        "log_path": logs_root / "dashboard_run.log",
        "child_pid_path": logs_root / "dashboard_run.child.pid",
        "exit_code_path": logs_root / "dashboard_run.exitcode",
        "finished_at_path": logs_root / "dashboard_run.finished_at",
    }


def _read_pid(path: Path | None) -> int | None:
    if path is None or not path.is_file():
        return None
    try:
        value = int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    return value if value > 0 else None


def _kill_pid(pid: int | None) -> None:
    if not pid or pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def _kill_process_group(pid: int | None) -> None:
    if not pid or pid <= 0:
        return
    try:
        pgid = os.getpgid(pid)
    except OSError:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except OSError:
        return


def build_run_command(config: dict[str, Any], run_root: Path) -> list[str]:
    paths = build_run_paths(run_root)
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "run_lead_finder_loop.py"),
        "--validation-profile",
        str(config.get("validation_profile") or DEFAULT_RUN_CONFIG["validation_profile"]),
        "--goal-final",
        str(int(config.get("goal_final") or DEFAULT_RUN_CONFIG["goal_final"])),
        "--max-runs",
        str(int(config.get("max_runs") or DEFAULT_RUN_CONFIG["max_runs"])),
        "--max-stale-runs",
        str(int(config.get("max_stale_runs") or DEFAULT_RUN_CONFIG["max_stale_runs"])),
        "--target",
        str(int(config.get("target") or DEFAULT_RUN_CONFIG["target"])),
        "--min-candidates",
        str(int(config.get("min_candidates") or DEFAULT_RUN_CONFIG["min_candidates"])),
        "--max-candidates",
        str(int(config.get("max_candidates") or DEFAULT_RUN_CONFIG["max_candidates"])),
        "--batch-min",
        str(int(config.get("batch_min") or DEFAULT_RUN_CONFIG["batch_min"])),
        "--batch-max",
        str(int(config.get("batch_max") or DEFAULT_RUN_CONFIG["batch_max"])),
        "--master-output",
        str(paths["master_output"]),
        "--minimal-output",
        str(paths["minimal_output"]),
        "--scouted-output",
        str(paths["scouted_output"]),
        "--contact-queue-output",
        str(paths["contact_queue_output"]),
        "--near-miss-location-output",
        str(paths["near_miss_location_output"]),
        "--verified-output",
        str(paths["verified_output"]),
        "--runs-dir",
        str(paths["runs_dir"]),
    ]
    if config.get("listing_strict", True):
        cmd.append("--listing-strict")
    queries_file = str(config.get("queries_file") or "").strip()
    if queries_file:
        cmd.extend(["--queries-file", queries_file])
    return cmd


def start_run(config: dict[str, Any]) -> dict[str, Any]:
    current = get_run_status()
    if current.get("active"):
        raise RuntimeError("A dashboard-started run is already active.")

    run_folder_name = str(config.get("run_folder_name") or "").strip() or suggested_run_folder_name()
    run_root = (PROJECT_ROOT / run_folder_name).resolve()
    paths = build_run_paths(run_root)
    paths["csv_root"].mkdir(parents=True, exist_ok=True)
    paths["logs_root"].mkdir(parents=True, exist_ok=True)
    paths["runs_dir"].mkdir(parents=True, exist_ok=True)
    for cleanup_key in ("child_pid_path", "exit_code_path", "finished_at_path"):
        try:
            paths[cleanup_key].unlink()
        except OSError:
            pass

    cmd = build_run_command(config, run_root)
    launcher_code = (
        "import datetime, json, pathlib, subprocess, sys; "
        "cmd=json.loads(sys.argv[1]); "
        "cwd=sys.argv[2]; "
        "exit_path=pathlib.Path(sys.argv[3]); "
        "finished_path=pathlib.Path(sys.argv[4]); "
        "log_path=pathlib.Path(sys.argv[5]); "
        "child_pid_path=pathlib.Path(sys.argv[6]); "
        "exit_path.parent.mkdir(parents=True, exist_ok=True); "
        "log_path.parent.mkdir(parents=True, exist_ok=True); "
        "code=0; "
        "handle=log_path.open('w', encoding='utf-8'); "
        "child=subprocess.Popen(cmd, cwd=cwd, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True); "
        "child_pid_path.write_text(str(child.pid), encoding='utf-8'); "
        "code=child.wait(); "
        "handle.close(); "
        "exit_path.write_text(str(code), encoding='utf-8'); "
        "finished_path.write_text(datetime.datetime.now(datetime.timezone.utc).isoformat(), encoding='utf-8'); "
        "sys.exit(code)"
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            launcher_code,
            json.dumps(cmd),
            str(PROJECT_ROOT),
            str(paths["exit_code_path"]),
            str(paths["finished_at_path"]),
            str(paths["log_path"]),
            str(paths["child_pid_path"]),
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    state = {
        "pid": process.pid,
        "started_at": _now_iso(),
        "run_root": str(run_root),
        "log_path": str(paths["log_path"]),
        "runs_dir": str(paths["runs_dir"]),
        "command": cmd,
        "config": config,
    }
    save_run_state(state)
    return state


def stop_run() -> dict[str, Any]:
    state = load_run_state()
    run_root = Path(state["run_root"]) if state.get("run_root") else None
    paths = build_run_paths(run_root) if run_root else {}
    launcher_pid = int(state.get("pid") or 0)
    child_pid = _read_pid(paths.get("child_pid_path"))
    if process_is_running(child_pid):
        _kill_process_group(child_pid)
    if process_is_running(launcher_pid):
        _kill_pid(launcher_pid)
    state["stopped_at"] = _now_iso()
    save_run_state(state)
    return state


def get_run_status() -> dict[str, Any]:
    state = load_run_state()
    if not state:
        return {"active": False}

    run_root = Path(state["run_root"]) if state.get("run_root") else None
    paths = build_run_paths(run_root) if run_root else {}
    launcher_pid = int(state.get("pid") or 0)
    child_pid = _read_pid(paths.get("child_pid_path"))
    active_pid = child_pid or launcher_pid
    active = process_is_running(active_pid)
    exit_code = None
    if paths.get("exit_code_path") and paths["exit_code_path"].is_file():
        try:
            exit_code = int(paths["exit_code_path"].read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            exit_code = None
    finished_at = ""
    if paths.get("finished_at_path") and paths["finished_at_path"].is_file():
        try:
            finished_at = paths["finished_at_path"].read_text(encoding="utf-8").strip()
        except OSError:
            finished_at = ""

    status_label = "idle"
    if active:
        status_label = "running"
    elif state.get("stopped_at"):
        status_label = "stopped"
    elif exit_code == 0:
        status_label = "finished"
    elif exit_code is not None:
        status_label = "failed"

    log_updated_at = ""
    log_path = paths.get("log_path")
    if log_path and log_path.is_file():
        try:
            log_updated_at = datetime.fromtimestamp(log_path.stat().st_mtime, tz=timezone.utc).isoformat()
        except OSError:
            log_updated_at = ""
    return {
        **state,
        "pid": active_pid,
        "launcher_pid": launcher_pid,
        "child_pid": child_pid,
        "active": active,
        "status_label": status_label,
        "exit_code": exit_code,
        "finished_at": finished_at,
        "log_updated_at": log_updated_at,
    }


def tail_log(path_value: str | Path | None, *, max_lines: int = 200) -> str:
    if not path_value:
        return ""
    path = Path(path_value)
    if not path.is_file():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-max_lines:])
