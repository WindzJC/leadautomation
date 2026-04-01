#!/usr/bin/env python3
"""Local per-machine environment loader."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable

from pipeline_paths import repo_path

LOCAL_ENV_PATH = repo_path(".env.local")
SUPPORTED_LOCAL_ENV_NAMES = (
    "GOOGLE_API_KEY",
    "GOOGLE_CSE_CX",
)


def parse_local_env_text(
    text: str,
    *,
    allowed_names: Iterable[str] = SUPPORTED_LOCAL_ENV_NAMES,
) -> Dict[str, str]:
    allowed = set(allowed_names)
    parsed: Dict[str, str] = {}
    for raw_line in (text or "").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].lstrip()
        if "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if not key or key not in allowed:
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        parsed[key] = value
    return parsed


def load_local_env(
    path: Path | None = None,
    *,
    override: bool = False,
    allowed_names: Iterable[str] = SUPPORTED_LOCAL_ENV_NAMES,
) -> Dict[str, str]:
    env_path = path or LOCAL_ENV_PATH
    if not env_path.is_file():
        return {}
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return {}

    active: Dict[str, str] = {}
    for key, value in parse_local_env_text(text, allowed_names=allowed_names).items():
        if override or key not in os.environ:
            os.environ[key] = value
        active[key] = os.environ.get(key, "")
    return active
