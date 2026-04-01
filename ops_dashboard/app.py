from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

from local_env import load_local_env

try:
    from fastapi import Body, FastAPI, HTTPException
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise RuntimeError(
        "FastAPI dependencies are not installed. Install requirements.txt before launching the ops dashboard."
    ) from exc

from dashboard import runner
from ops_dashboard.data_access import (
    build_overview,
    collect_run_summaries,
    load_lead_output,
    load_live_status,
    load_rejected_rows,
    load_run_artifact_catalog,
    load_run_artifact_path,
    load_run_manifest,
    load_validated_rows,
)

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
load_local_env()
VALIDATION_PROFILES = [
    "agent_hunt",
    "email_only",
    "strict_full",
    "strict_interactive",
    "fully_verified",
    "verified_no_us",
    "astra_outbound",
]
ARTIFACT_MEDIA_TYPES = {
    "manifest": "application/json",
    "validated": "text/csv; charset=utf-8",
    "rejected": "text/csv; charset=utf-8",
    "final": "text/csv; charset=utf-8",
    "log": "text/plain; charset=utf-8",
}

app = FastAPI(title="Lead Finder Ops Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _coerce_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(1, parsed)


def _coerce_bool(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return fallback


def _normalize_run_config(payload: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(payload or {})
    defaults = dict(runner.DEFAULT_RUN_CONFIG)
    validation_profile = str(raw.get("validation_profile") or defaults["validation_profile"]).strip() or defaults["validation_profile"]
    if validation_profile not in VALIDATION_PROFILES:
        validation_profile = defaults["validation_profile"]
    return {
        "validation_profile": validation_profile,
        "listing_strict": _coerce_bool(raw.get("listing_strict"), bool(defaults["listing_strict"])),
        "goal_final": _coerce_int(raw.get("goal_final"), int(defaults["goal_final"])),
        "max_runs": _coerce_int(raw.get("max_runs"), int(defaults["max_runs"])),
        "max_stale_runs": _coerce_int(raw.get("max_stale_runs"), int(defaults["max_stale_runs"])),
        "target": _coerce_int(raw.get("target"), int(defaults["target"])),
        "min_candidates": _coerce_int(raw.get("min_candidates"), int(defaults["min_candidates"])),
        "max_candidates": _coerce_int(raw.get("max_candidates"), int(defaults["max_candidates"])),
        "batch_min": _coerce_int(raw.get("batch_min"), int(defaults["batch_min"])),
        "batch_max": _coerce_int(raw.get("batch_max"), int(defaults["batch_max"])),
        "queries_file": str(raw.get("queries_file") or "").strip(),
        "run_folder_name": str(raw.get("run_folder_name") or "").strip() or runner.suggested_run_folder_name(),
    }


def _sanitize_runner_status(status: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(status or {})
    if not payload:
        return {"active": False, "status_label": "idle"}
    run_root = Path(str(payload.get("run_root") or "")).expanduser() if payload.get("run_root") else None
    log_path = Path(str(payload.get("log_path") or "")).expanduser() if payload.get("log_path") else None
    run_root_outside_repo = False
    if run_root is not None:
        try:
            run_root.resolve().relative_to(runner.PROJECT_ROOT.resolve())
        except ValueError:
            run_root_outside_repo = True
    if (
        not bool(payload.get("active"))
        and str(payload.get("status_label") or "").strip().lower() in {"", "idle"}
        and payload.get("exit_code") is None
        and not str(payload.get("finished_at") or "").strip()
        and (
            (run_root is not None and not run_root.exists() and (log_path is None or not log_path.exists()))
            or run_root_outside_repo
        )
    ):
        return {"active": False, "status_label": "idle"}
    return payload


def _current_run_config_payload() -> dict[str, Any]:
    status = _sanitize_runner_status(runner.get_run_status())
    status_config = status.get("config") if isinstance(status.get("config"), dict) else {}
    draft_config = runner.load_run_form_state()
    merged = {**runner.DEFAULT_RUN_CONFIG, **draft_config, **status_config}
    normalized = _normalize_run_config(merged)
    return {
        "config": normalized,
        "validation_profiles": VALIDATION_PROFILES,
        "active_run": status,
    }


@app.get("/", include_in_schema=False)
def serve_index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/overview")
def overview() -> dict[str, object]:
    runs = collect_run_summaries()
    return build_overview(runs)


@app.get("/api/runs")
def runs() -> list[dict[str, object]]:
    return collect_run_summaries()


@app.get("/api/runs/{api_run_id}/manifest")
def run_manifest(api_run_id: str) -> dict[str, object]:
    try:
        return load_run_manifest(api_run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc


@app.get("/api/runs/{api_run_id}/validated")
def run_validated(api_run_id: str) -> list[dict[str, str]]:
    try:
        return load_validated_rows(api_run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc


@app.get("/api/runs/{api_run_id}/rejected")
def run_rejected(api_run_id: str) -> list[dict[str, str]]:
    try:
        return load_rejected_rows(api_run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc


@app.get("/api/runs/{api_run_id}/leads")
def run_leads(api_run_id: str) -> dict[str, object]:
    try:
        return load_lead_output(api_run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc


@app.get("/api/runs/{api_run_id}/artifacts")
def run_artifacts(api_run_id: str) -> dict[str, object]:
    try:
        catalog = load_run_artifact_catalog(api_run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="run not found") from exc
    encoded_run_id = quote(api_run_id, safe="")
    return {
        artifact_name: {
            **payload,
            "download_url": f"/api/runs/{encoded_run_id}/download/{artifact_name}" if payload.get("available") else "",
        }
        for artifact_name, payload in catalog.items()
    }


@app.get("/api/runs/{api_run_id}/download/{artifact_name}")
def run_artifact_download(api_run_id: str, artifact_name: str) -> FileResponse:
    try:
        path = load_run_artifact_path(api_run_id, artifact_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="artifact not found") from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="artifact missing") from exc
    media_type = ARTIFACT_MEDIA_TYPES.get(str(artifact_name or "").strip().lower(), "application/octet-stream")
    return FileResponse(path, media_type=media_type, filename=path.name)


@app.get("/api/live-status")
def live_status() -> dict[str, object]:
    return load_live_status()


@app.get("/api/run-config")
def run_config() -> dict[str, object]:
    return _current_run_config_payload()


@app.post("/api/run-config")
def save_run_config(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, object]:
    config = _normalize_run_config(payload)
    runner.save_run_form_state(config)
    return {
        "ok": True,
        "config": config,
    }


@app.post("/api/run/start")
def run_start(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, object]:
    config = _normalize_run_config(payload)
    runner.save_run_form_state(config)
    try:
        runner.start_run(config)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    status = _sanitize_runner_status(runner.get_run_status())
    return {
        "ok": True,
        "status": status,
        "config": config,
    }


@app.post("/api/run/stop")
def run_stop() -> dict[str, object]:
    status = _sanitize_runner_status(runner.stop_run())
    return {
        "ok": True,
        "status": status,
    }


@app.get("/api/run/log")
def run_log() -> dict[str, object]:
    status = _sanitize_runner_status(runner.get_run_status())
    return {
        "status": status,
        "text": runner.tail_log(status.get("log_path")),
    }
