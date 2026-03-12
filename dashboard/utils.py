from __future__ import annotations

from pathlib import Path
from typing import Any


def path_to_uri(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return path.resolve().as_uri()
    except (OSError, ValueError):
        return str(path)


def safe_get(mapping: dict[str, Any] | None, key: str, default: Any = None) -> Any:
    if not mapping:
        return default
    return mapping.get(key, default)


def maybe_dataframe(rows: list[dict[str, Any]]) -> Any:
    try:
        import pandas as pd  # type: ignore

        return pd.DataFrame(rows)
    except Exception:
        return rows


def normalize_display_rows(rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({column: row.get(column, "") for column in columns})
    return normalized


def metric_value(value: Any, default: int | float | str = 0) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        nested = value.get("total", default)
        if isinstance(nested, dict):
            return default
        return nested
    return value
