from __future__ import annotations

import datetime as dt
from collections import Counter
from pathlib import Path
from typing import Any, Callable

from dashboard.data_loader import (
    PROJECT_ROOT,
    RunContext,
    discover_run_contexts,
    list_run_tags,
    load_run_bundle,
    read_json_file,
    run_file_map,
)
from dashboard.runner import get_run_status
from pipeline_paths import LEGACY_STATE_DIR, STATE_DIR, resolve_with_legacy

LIVE_STATUS_FILENAME = "live_status.json"
RUN_ID_SEPARATOR = "~"
RUN_ARTIFACT_PATH_KEYS = {
    "manifest": "run_manifest",
    "validated": "validated_export",
    "rejected": "rejected_export",
    "final": "final",
    "log": "pipeline_stdout",
}
RUN_ARTIFACT_LABELS = {
    "manifest": "Manifest JSON",
    "validated": "Validated CSV",
    "rejected": "Rejected CSV",
    "final": "Final Leads CSV",
    "log": "Run Log",
}


def compose_api_run_id(context_name: str, run_tag: str) -> str:
    return f"{context_name}{RUN_ID_SEPARATOR}{run_tag}"


def parse_api_run_id(api_run_id: str) -> tuple[str, str]:
    if RUN_ID_SEPARATOR not in api_run_id:
        raise ValueError("invalid api run id")
    context_name, run_tag = api_run_id.rsplit(RUN_ID_SEPARATOR, 1)
    context_name = context_name.strip()
    run_tag = run_tag.strip()
    if not context_name or not run_tag:
        raise ValueError("invalid api run id")
    return context_name, run_tag


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _path_mtime(*paths: Path) -> float:
    latest = 0.0
    for path in paths:
        if not path.exists():
            continue
        try:
            latest = max(latest, path.stat().st_mtime)
        except OSError:
            continue
    return latest


def _format_timestamp(timestamp: float) -> str:
    if timestamp <= 0:
        return ""
    return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp_score(value: str) -> float:
    if not value:
        return 0.0
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return 0.0


def _normalize_counts(manifest: dict[str, Any], stats: dict[str, Any]) -> dict[str, int]:
    manifest_counts = dict(manifest.get("counts", {}) or {})
    stats_counts = dict(stats.get("counts", {}) or {})
    return {
        "harvested_candidates": _coerce_int(
            manifest_counts.get("harvested_candidates", stats_counts.get("harvested_candidates", stats.get("harvested_candidates", 0)))
        ),
        "filtered_candidates": _coerce_int(manifest_counts.get("filtered_candidates", stats_counts.get("candidates", 0))),
        "validated": _coerce_int(manifest_counts.get("validated", stats_counts.get("validated", 0))),
        "final": _coerce_int(manifest_counts.get("final", stats_counts.get("final", 0))),
        "kept_rows_after_gate": _coerce_int(manifest_counts.get("kept_rows_after_gate", stats_counts.get("kept_rows_after_gate", 0))),
        "added_to_master": _coerce_int(manifest_counts.get("added_to_master", stats_counts.get("added_to_master", 0))),
        "added_to_queue": _coerce_int(manifest_counts.get("added_to_queue", stats_counts.get("added_to_queue", 0))),
        "rejected": _coerce_int(manifest_counts.get("rejected", 0)),
        "duplicates": _coerce_int(manifest_counts.get("duplicates", stats_counts.get("duplicate_count", 0))),
        "near_miss_location_rows": _coerce_int(
            manifest_counts.get("near_miss_location_rows", stats_counts.get("near_miss_location_rows", 0))
        ),
    }


def _normalize_status(manifest: dict[str, Any], stats: dict[str, Any]) -> str:
    status = str(manifest.get("status", "") or "").strip().lower()
    if status:
        return status
    pipeline_exit_code = stats.get("pipeline_exit_code")
    if pipeline_exit_code is None:
        return "unknown"
    return "completed" if _coerce_int(pipeline_exit_code) == 0 else "failed"


def build_run_summary(context: RunContext, run_tag: str) -> dict[str, Any]:
    paths = run_file_map(context, run_tag)
    manifest = read_json_file(paths["run_manifest"])
    stats = read_json_file(paths["stats"])
    validate_stats = read_json_file(paths["validate_stats"])
    generated_at = (
        str(manifest.get("generated_at_utc", "") or "").strip()
        or str(stats.get("generated_at_utc", "") or "").strip()
        or _format_timestamp(_path_mtime(paths["run_manifest"], paths["stats"], paths["validate_stats"]))
    )
    rejected_reason_counts = dict(
        manifest.get("rejected_reason_counts")
        or validate_stats.get("reject_reasons")
        or {}
    )
    counts = _normalize_counts(manifest, stats)
    summary = {
        "api_run_id": compose_api_run_id(context.name, run_tag),
        "run_id": str(manifest.get("run_id", "") or f"run_{run_tag}"),
        "run_tag": run_tag,
        "context_name": context.name,
        "context_path": str(context.root_path),
        "runs_path": str(context.runs_path),
        "generated_at_utc": generated_at,
        "status": _normalize_status(manifest, stats),
        "validation_profile": str(manifest.get("validation_profile", "") or stats.get("validation_profile", "")),
        "pipeline_exit_code": _coerce_int(manifest.get("pipeline_exit_code", stats.get("pipeline_exit_code", 0))),
        "counts": counts,
        "rejected_reason_counts": rejected_reason_counts,
        "artifacts": dict(manifest.get("artifacts", {}) or {}),
    }
    summary["_sort_timestamp"] = _timestamp_score(generated_at) or _path_mtime(paths["run_manifest"], paths["stats"], paths["validate_stats"])
    return summary


def collect_run_summaries(base_dir: Path | None = None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    context_by_runs_path: dict[Path, RunContext] = {}
    for context in discover_run_contexts(base_dir or PROJECT_ROOT):
        resolved_runs_path = context.runs_path.resolve()
        existing = context_by_runs_path.get(resolved_runs_path)
        if existing is None:
            context_by_runs_path[resolved_runs_path] = context
            continue
        existing_key = (0 if existing.name == "project-root" else 1, len(existing.root_path.parts), existing.name.lower())
        candidate_key = (0 if context.name == "project-root" else 1, len(context.root_path.parts), context.name.lower())
        if candidate_key < existing_key:
            context_by_runs_path[resolved_runs_path] = context
    for context in context_by_runs_path.values():
        for run_tag in list_run_tags(context.runs_path):
            records.append(build_run_summary(context, run_tag))
    records.sort(
        key=lambda item: (
            float(item.get("_sort_timestamp", 0.0) or 0.0),
            str(item.get("context_name", "") or "").lower(),
            str(item.get("run_tag", "") or ""),
        ),
        reverse=True,
    )
    for record in records:
        record.pop("_sort_timestamp", None)
    return records


def build_overview(run_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    reject_counter: Counter[str] = Counter()
    harvested = 0
    validated = 0
    rejected = 0
    duplicates = 0
    for record in run_summaries:
        counts = dict(record.get("counts", {}) or {})
        harvested += _coerce_int(counts.get("harvested_candidates", 0))
        validated += _coerce_int(counts.get("validated", 0))
        rejected += _coerce_int(counts.get("rejected", 0))
        duplicates += _coerce_int(counts.get("duplicates", 0))
        for label, count in dict(record.get("rejected_reason_counts", {}) or {}).items():
            reject_counter[str(label)] += _coerce_int(count)
    return {
        "total_runs": len(run_summaries),
        "harvested_count": harvested,
        "validated_count": validated,
        "rejected_count": rejected,
        "duplicate_count": duplicates,
        "latest_run_summary": run_summaries[0] if run_summaries else {},
        "top_reject_reasons": [
            {"label": label, "count": count}
            for label, count in reject_counter.most_common(10)
        ],
    }


def _run_has_any_artifact(context: RunContext, run_tag: str) -> bool:
    paths = run_file_map(context, run_tag)
    return any(path.exists() for path in paths.values())


def resolve_run_context_and_tag(api_run_id: str, base_dir: Path | None = None) -> tuple[RunContext, str]:
    context_name, run_tag = parse_api_run_id(api_run_id)
    contexts = {context.name: context for context in discover_run_contexts(base_dir or PROJECT_ROOT)}
    context = contexts.get(context_name)
    if context is None:
        raise KeyError(api_run_id)
    if not _run_has_any_artifact(context, run_tag):
        raise KeyError(api_run_id)
    return context, run_tag


def load_run_detail(api_run_id: str, base_dir: Path | None = None) -> dict[str, Any]:
    context, run_tag = resolve_run_context_and_tag(api_run_id, base_dir=base_dir)
    bundle = load_run_bundle(context, run_tag)
    return {
        "context": context,
        "run_tag": run_tag,
        "bundle": bundle,
        "summary": build_run_summary(context, run_tag),
    }


def load_run_manifest(api_run_id: str, base_dir: Path | None = None) -> dict[str, Any]:
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    manifest = dict(detail["bundle"].get("run_manifest", {}) or {})
    if manifest:
        return manifest
    summary = dict(detail["summary"])
    return {
        "run_id": summary["run_id"],
        "generated_at_utc": summary["generated_at_utc"],
        "validation_profile": summary["validation_profile"],
        "status": summary["status"],
        "pipeline_exit_code": summary["pipeline_exit_code"],
        "counts": summary["counts"],
        "rejected_reason_counts": summary["rejected_reason_counts"],
        "artifacts": summary["artifacts"],
    }


def load_validated_rows(api_run_id: str, base_dir: Path | None = None) -> list[dict[str, str]]:
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    bundle = detail["bundle"]
    return list(bundle.get("validated_export_rows") or bundle.get("validated_rows") or [])


def load_rejected_rows(api_run_id: str, base_dir: Path | None = None) -> list[dict[str, str]]:
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    return list(detail["bundle"].get("rejected_rows") or [])


def _project_minimal_lead_rows(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    projected: list[dict[str, str]] = []
    for row in rows:
        source_url = (
            str(row.get("SourceURL", "") or "").strip()
            or str(row.get("AuthorEmailSourceURL", "") or "").strip()
            or str(row.get("CandidateURL", "") or "").strip()
        )
        projected.append(
            {
                "AuthorName": str(row.get("AuthorName", "") or "").strip(),
                "AuthorEmail": str(row.get("AuthorEmail", "") or "").strip(),
                "SourceURL": source_url,
            }
        )
    return projected


def load_lead_output(api_run_id: str, base_dir: Path | None = None) -> dict[str, Any]:
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    bundle = detail["bundle"]
    final_rows = list(bundle.get("final_rows") or [])
    if final_rows:
        return {"source": "final", "rows": _project_minimal_lead_rows(final_rows)}
    validated_rows = list(bundle.get("validated_export_rows") or bundle.get("validated_rows") or [])
    if validated_rows:
        return {"source": "validated", "rows": _project_minimal_lead_rows(validated_rows)}
    return {"source": "final", "rows": []}


def load_run_artifact_catalog(api_run_id: str, base_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    paths = dict(detail["bundle"].get("paths", {}) or {})
    catalog: dict[str, dict[str, Any]] = {}
    for artifact_name, path_key in RUN_ARTIFACT_PATH_KEYS.items():
        path = paths.get(path_key)
        catalog[artifact_name] = {
            "label": RUN_ARTIFACT_LABELS[artifact_name],
            "filename": path.name if isinstance(path, Path) else "",
            "available": bool(isinstance(path, Path) and path.is_file()),
        }
    return catalog


def load_run_artifact_path(api_run_id: str, artifact_name: str, base_dir: Path | None = None) -> Path:
    artifact_key = str(artifact_name or "").strip().lower()
    path_key = RUN_ARTIFACT_PATH_KEYS.get(artifact_key)
    if path_key is None:
        raise KeyError(artifact_name)
    detail = load_run_detail(api_run_id, base_dir=base_dir)
    paths = dict(detail["bundle"].get("paths", {}) or {})
    path = paths.get(path_key)
    if not isinstance(path, Path) or not path.is_file():
        raise FileNotFoundError(f"{artifact_key} missing")
    return path


def _runner_status_to_live_payload(status: dict[str, Any]) -> dict[str, Any]:
    status_label = str(status.get("status_label", "") or "idle")
    updated_at = (
        str(status.get("log_updated_at", "") or "").strip()
        or str(status.get("finished_at", "") or "").strip()
        or str(status.get("started_at", "") or "").strip()
    )
    return {
        "active": bool(status.get("active", False)),
        "status": status_label,
        "stage": status_label,
        "updated_at_utc": updated_at,
        "started_at_utc": str(status.get("started_at", "") or ""),
        "finished_at_utc": str(status.get("finished_at", "") or ""),
        "run_root": str(status.get("run_root", "") or ""),
        "runs_dir": str(status.get("runs_dir", "") or ""),
        "log_path": str(status.get("log_path", "") or ""),
        "message": "Dashboard-started run state",
        "source": "runner_state",
    }


def _runner_state_is_stale(status: dict[str, Any]) -> bool:
    run_root = Path(str(status.get("run_root") or "")).expanduser() if status.get("run_root") else None
    log_path = Path(str(status.get("log_path") or "")).expanduser() if status.get("log_path") else None
    run_root_outside_repo = False
    if run_root is not None:
        try:
            run_root.resolve().relative_to(PROJECT_ROOT.resolve())
        except ValueError:
            run_root_outside_repo = True
    return (
        not bool(status.get("active", False))
        and str(status.get("status_label", "") or "").strip().lower() in {"", "idle"}
        and status.get("exit_code") is None
        and not str(status.get("finished_at", "") or "").strip()
        and (
            (run_root is not None and not run_root.exists() and (log_path is None or not log_path.exists()))
            or run_root_outside_repo
        )
    )


def load_live_status(
    base_dir: Path | None = None,
    runner_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = (base_dir or PROJECT_ROOT).resolve()
    preferred = root / STATE_DIR / LIVE_STATUS_FILENAME
    legacy = root / LEGACY_STATE_DIR / LIVE_STATUS_FILENAME
    path = resolve_with_legacy(preferred, legacy)
    payload = read_json_file(path)
    if payload:
        payload.setdefault("source", "live_status")
        payload.setdefault("active", str(payload.get("status", "") or "").lower() not in {"completed", "failed", "stopped", "idle"})
        payload.setdefault("stage", str(payload.get("status", "") or "idle"))
        return payload

    provider = runner_status_provider or get_run_status
    runner_status = provider()
    if runner_status and _runner_state_is_stale(runner_status):
        return {
            "active": False,
            "status": "idle",
            "stage": "idle",
            "updated_at_utc": "",
            "message": "No active run",
            "source": "none",
        }
    if runner_status and (
        bool(runner_status.get("active", False))
        or str(runner_status.get("status_label", "") or "") not in {"", "idle"}
    ):
        return _runner_status_to_live_payload(runner_status)

    return {
        "active": False,
        "status": "idle",
        "stage": "idle",
        "updated_at_utc": "",
        "message": "No active run",
        "source": "none",
    }
