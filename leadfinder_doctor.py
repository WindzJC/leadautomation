#!/usr/bin/env python3
"""Simple repo and environment health check."""

from __future__ import annotations

import os
import subprocess
from typing import Dict

from local_env import LOCAL_ENV_PATH, SUPPORTED_LOCAL_ENV_NAMES, load_local_env
from pipeline_paths import repo_path


def _git_output(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_path(),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def _git_dirty() -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_path(),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return bool(result.stdout.strip())


def _venv_exists() -> bool:
    venv_root = repo_path(".venv")
    return venv_root.is_dir() and (
        (venv_root / "bin" / "python").exists()
        or (venv_root / "Scripts" / "python.exe").exists()
        or (venv_root / "Scripts" / "Activate.ps1").exists()
    )


def collect_status() -> Dict[str, object]:
    load_local_env()
    return {
        "repo_root": str(repo_path()),
        "branch": _git_output("rev-parse", "--abbrev-ref", "HEAD"),
        "latest_commit": _git_output("log", "-1", "--oneline"),
        "repo_dirty": _git_dirty(),
        "venv_exists": _venv_exists(),
        "local_env_path": str(LOCAL_ENV_PATH),
        "local_env_exists": LOCAL_ENV_PATH.is_file(),
        "env_present": {name: bool(os.environ.get(name, "").strip()) for name in SUPPORTED_LOCAL_ENV_NAMES},
    }


def format_status(status: Dict[str, object]) -> str:
    env_present = dict(status.get("env_present", {}) or {})
    lines = [
        f"RepoRoot: {status.get('repo_root', '')}",
        f"Branch: {status.get('branch', 'unknown')}",
        f"LatestCommit: {status.get('latest_commit', 'unknown')}",
        f"RepoState: {'dirty' if status.get('repo_dirty') else 'clean'}",
        f"Venv: {'present' if status.get('venv_exists') else 'missing'}",
        f"LocalEnvFile: {'present' if status.get('local_env_exists') else 'missing'} ({status.get('local_env_path', '')})",
        f"GOOGLE_API_KEY: {'present' if env_present.get('GOOGLE_API_KEY') else 'missing'}",
        f"GOOGLE_CSE_CX: {'present' if env_present.get('GOOGLE_CSE_CX') else 'missing'}",
    ]
    return "\n".join(lines)


def main() -> int:
    print(format_status(collect_status()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
