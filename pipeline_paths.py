#!/usr/bin/env python3
"""Shared runtime/output path helpers."""

from __future__ import annotations

from pathlib import Path

OUTPUT_ROOT = Path("outputs")
OUTPUT_CSV_DIR = OUTPUT_ROOT / "csv"
OUTPUT_JSON_DIR = OUTPUT_ROOT / "json"
OUTPUT_LOGS_DIR = OUTPUT_ROOT / "logs"
STATE_DIR = Path("state")
LEGACY_STATE_DIR = Path(".dashboard_state")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_runtime_dirs() -> None:
    for path in (OUTPUT_CSV_DIR, OUTPUT_JSON_DIR, OUTPUT_LOGS_DIR, STATE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def csv_output(name: str) -> str:
    return str(OUTPUT_CSV_DIR / name)


def json_output(name: str) -> str:
    return str(OUTPUT_JSON_DIR / name)


def log_output(name: str) -> str:
    return str(OUTPUT_LOGS_DIR / name)


def state_output(name: str) -> str:
    return str(STATE_DIR / name)


def resolve_with_legacy(preferred: Path, legacy: Path) -> Path:
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy
