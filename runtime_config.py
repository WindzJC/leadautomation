#!/usr/bin/env python3
"""Runtime configuration loader for threshold and rule defaults."""

from __future__ import annotations

import ast
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Tuple

from pipeline_paths import repo_path

DEFAULT_CONFIG_PATH = repo_path("config", "default.yaml")

DEFAULT_CONFIG: Dict[str, Any] = {
    "validation_profile": "strict_full",
    "goal_final": 20,
    "max_runs": 1,
    "max_stale_runs": 1,
    "target": 80,
    "min_candidates": 80,
    "max_candidates": 80,
    "batch_min": 20,
    "batch_max": 20,
    "merge_policy": "strict",
    "listing_strict": True,
    "target_final": 20,
    "harvest_minimum": 80,
    "allowed_years": 4,
    "max_social_followers": 100000,
    "confidence_threshold": "weak",
    "require_visible_email": False,
    "require_us_only": False,
    "require_listing_buyable": False,
}


def _parse_value(raw: str) -> Any:
    value = (raw or "").strip()
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", ""}:
        return None
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return value


def load_runtime_config(path: Path | None = None) -> Dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    cfg_path = path or DEFAULT_CONFIG_PATH
    if not cfg_path.is_file():
        return config

    try:
        lines = cfg_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return config

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            continue
        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        if not key:
            continue
        config[key] = _parse_value(raw_value)

    return config


def allowed_year_bounds(config: Dict[str, Any], *, now_year: int | None = None) -> Tuple[int, int]:
    current_year = int(now_year or dt.datetime.now(dt.timezone.utc).year)
    raw_value = config.get("allowed_years", DEFAULT_CONFIG["allowed_years"])

    if isinstance(raw_value, (list, tuple)) and len(raw_value) == 2:
        try:
            lower = int(raw_value[0])
            upper = int(raw_value[1])
            if lower <= upper:
                return lower, upper
        except (TypeError, ValueError):
            pass

    try:
        lookback = max(0, int(raw_value))
    except (TypeError, ValueError):
        lookback = int(DEFAULT_CONFIG["allowed_years"])
    return current_year - lookback, current_year


def config_arg_default(config: Dict[str, Any], key: str, fallback: Any, *, aliases: Tuple[str, ...] = ()) -> Any:
    for candidate in (key, *aliases):
        value = config.get(candidate)
        if value is not None:
            return value
    return fallback
