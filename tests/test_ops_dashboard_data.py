from __future__ import annotations

import json
from pathlib import Path

import pytest

from ops_dashboard.data_access import (
    build_overview,
    collect_run_summaries,
    compose_api_run_id,
    load_lead_output,
    load_live_status,
    load_run_artifact_catalog,
    load_run_artifact_path,
    load_rejected_rows,
    load_run_manifest,
    load_validated_rows,
    render_copy_friendly_lead_output,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_collect_run_summaries_and_build_overview(tmp_path: Path) -> None:
    root_runs = tmp_path / "outputs" / "runs"
    root_json = tmp_path / "outputs" / "json"
    child_root = tmp_path / "batch_alpha"
    child_runs = child_root / "outputs" / "runs"
    child_json = child_root / "outputs" / "json"
    root_runs.mkdir(parents=True)
    child_runs.mkdir(parents=True)

    _write(
        root_runs / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "counts": {"harvested_candidates": 20, "validated": 4}}),
    )
    _write(
        root_json / "run_manifest_run_001.json",
        json.dumps(
            {
                "run_id": "run_001",
                "generated_at_utc": "2026-03-14T10:00:00Z",
                "validation_profile": "default",
                "status": "completed",
                "pipeline_exit_code": 0,
                "counts": {
                    "harvested_candidates": 20,
                    "filtered_candidates": 18,
                    "validated": 4,
                    "final": 3,
                    "rejected": 14,
                    "duplicates": 1,
                },
                "rejected_reason_counts": {"candidate_unreachable": 9, "missing_indie_proof": 5},
            }
        ),
    )

    _write(
        child_runs / "run_002_stats.json",
        json.dumps({"generated_at_utc": "2026-03-15T12:00:00Z", "counts": {"harvested_candidates": 30, "validated": 8}}),
    )
    _write(
        child_json / "run_manifest_run_002.json",
        json.dumps(
            {
                "run_id": "run_002",
                "generated_at_utc": "2026-03-15T12:00:00Z",
                "validation_profile": "strict_full",
                "status": "completed",
                "pipeline_exit_code": 0,
                "counts": {
                    "harvested_candidates": 30,
                    "filtered_candidates": 25,
                    "validated": 8,
                    "final": 6,
                    "rejected": 17,
                    "duplicates": 2,
                },
                "rejected_reason_counts": {"candidate_unreachable": 7, "insufficient_author_identity": 10},
            }
        ),
    )

    summaries = collect_run_summaries(base_dir=tmp_path)
    overview = build_overview(summaries)

    assert [summary["run_id"] for summary in summaries] == ["run_002", "run_001"]
    assert summaries[0]["context_name"] == "batch_alpha"
    assert overview["total_runs"] == 2
    assert overview["harvested_count"] == 50
    assert overview["validated_count"] == 12
    assert overview["rejected_count"] == 31
    assert overview["duplicate_count"] == 3
    assert overview["latest_run_summary"]["run_id"] == "run_002"
    assert overview["top_reject_reasons"][0] == {"label": "candidate_unreachable", "count": 16}


def test_run_detail_loaders_handle_missing_artifacts_gracefully(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    json_dir = tmp_path / "outputs" / "json"
    runs_dir.mkdir(parents=True)

    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0, "counts": {"validated": 2}}),
    )
    _write(
        json_dir / "run_manifest_run_001.json",
        json.dumps(
            {
                "run_id": "run_001",
                "generated_at_utc": "2026-03-14T10:00:00Z",
                "validation_profile": "default",
                "status": "completed",
                "pipeline_exit_code": 0,
                "counts": {"validated": 2, "rejected": 0},
            }
        ),
    )

    api_run_id = compose_api_run_id("project-root", "001")

    manifest = load_run_manifest(api_run_id, base_dir=tmp_path)
    validated_rows = load_validated_rows(api_run_id, base_dir=tmp_path)
    rejected_rows = load_rejected_rows(api_run_id, base_dir=tmp_path)
    lead_output = load_lead_output(api_run_id, base_dir=tmp_path)

    assert manifest["run_id"] == "run_001"
    assert validated_rows == []
    assert rejected_rows == []
    assert lead_output["source"] == "final"
    assert lead_output["rows"] == []
    assert lead_output["copy_text"] == "```\n\n```"


def test_run_detail_loaders_treat_plain_run_id_as_missing_instead_of_error(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    json_dir = tmp_path / "outputs" / "json"
    runs_dir.mkdir(parents=True)

    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )
    _write(tmp_path / "outputs" / "json" / "run_manifest_run_001.json", json.dumps({"run_id": "run_001"}))

    with pytest.raises(KeyError):
        load_run_manifest("run_001", base_dir=tmp_path)


def test_lead_output_and_artifact_catalog_prefer_final_rows(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    csv_dir = tmp_path / "outputs" / "csv"
    runs_dir.mkdir(parents=True)
    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )
    _write(
        runs_dir / "run_001_final.csv",
        "AuthorName,BookTitle,AuthorEmail,SourceURL,AuthorWebsite,ListingURL,Location\nJane Doe,Skyfall,jane@example.com,https://source.example,https://author.example,https://amazon.example,Texas\n",
    )
    _write(
        csv_dir / "validated_run_001.csv",
        "AuthorName,BookTitle,AuthorEmail\nJane Doe,Skyfall,jane@example.com\n",
    )
    _write(
        csv_dir / "rejected_run_001.csv",
        "AuthorName,BookTitle,CandidateDomain,PrimaryFailReason,FailReasons\nNovel Notions,Book,novelnotions.com,insufficient_author_identity,insufficient_author_identity\n",
    )
    _write(tmp_path / "outputs" / "json" / "run_manifest_run_001.json", json.dumps({"run_id": "run_001"}))
    _write(runs_dir / "run_001_pipeline.stdout.log", "done\n")

    api_run_id = compose_api_run_id("project-root", "001")

    lead_output = load_lead_output(api_run_id, base_dir=tmp_path)
    artifact_catalog = load_run_artifact_catalog(api_run_id, base_dir=tmp_path)
    artifact_path = load_run_artifact_path(api_run_id, "final", base_dir=tmp_path)

    assert lead_output["source"] == "final"
    assert lead_output["rows"] == [
        {
            "AuthorName": "Jane Doe",
            "AuthorEmail": "jane@example.com",
            "SourceURL": "https://source.example",
        }
    ]
    assert lead_output["copy_text"] == "```\nJane Doe,jane@example.com\n```\n\nJane Doe — https://source.example"
    assert artifact_catalog["final"]["available"] is True
    assert artifact_catalog["validated"]["available"] is True
    assert artifact_catalog["log"]["available"] is True
    assert artifact_path.name == "run_001_final.csv"


def test_load_lead_output_prefers_export_files_and_filters_blank_email_rows(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    csv_dir = tmp_path / "outputs" / "csv"
    runs_dir.mkdir(parents=True)
    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )
    _write(tmp_path / "outputs" / "json" / "run_manifest_run_001.json", json.dumps({"run_id": "run_001"}))
    _write(
        runs_dir / "run_001_final.csv",
        "AuthorName,BookTitle,AuthorEmail,SourceURL\n"
        "Jane Missing,Book,,https://missing.example\n"
        "Jane Good,Book,jane@example.com,https://good.example\n",
    )
    _write(
        csv_dir / "scouted_leads.csv",
        "Scout Writer,scout@example.com,https://scout.example\n",
    )

    api_run_id = compose_api_run_id("project-root", "001")
    lead_output = load_lead_output(api_run_id, base_dir=tmp_path)

    assert lead_output["source"] == "scouted_leads"
    assert lead_output["rows"] == [
        {
            "AuthorName": "Scout Writer",
            "AuthorEmail": "scout@example.com",
            "SourceURL": "https://scout.example",
        }
    ]
    assert lead_output["copy_text"] == "```\nScout Writer,scout@example.com\n```\n\nScout Writer — https://scout.example"


def test_load_lead_output_dedupes_duplicate_author_email_rows_from_export_file(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    csv_dir = tmp_path / "outputs" / "csv"
    runs_dir.mkdir(parents=True)
    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )
    _write(tmp_path / "outputs" / "json" / "run_manifest_run_001.json", json.dumps({"run_id": "run_001"}))
    _write(
        csv_dir / "scouted_leads.csv",
        "Scout Writer,scout@example.com,https://scout.example/about\n"
        "Scout Writer,scout@example.com,https://scout.example/contact\n",
    )

    api_run_id = compose_api_run_id("project-root", "001")
    lead_output = load_lead_output(api_run_id, base_dir=tmp_path)

    assert lead_output["source"] == "scouted_leads"
    assert lead_output["rows"] == [
        {
            "AuthorName": "Scout Writer",
            "AuthorEmail": "scout@example.com",
            "SourceURL": "https://scout.example/about",
        }
    ]


def test_load_lead_output_prefers_new_leads_artifact_for_selected_run(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    runs_dir.mkdir(parents=True)
    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )
    _write(tmp_path / "outputs" / "json" / "run_manifest_run_001.json", json.dumps({"run_id": "run_001"}))
    _write(
        runs_dir / "run_001_new_leads.csv",
        "New Writer,new@example.com,https://new.example/contact\n",
    )
    _write(
        tmp_path / "outputs" / "csv" / "scouted_leads.csv",
        "Old Writer,old@example.com,https://old.example/contact\n",
    )

    api_run_id = compose_api_run_id("project-root", "001")
    lead_output = load_lead_output(api_run_id, base_dir=tmp_path)

    assert lead_output["source"] == "new_leads"
    assert lead_output["rows"] == [
        {
            "AuthorName": "New Writer",
            "AuthorEmail": "new@example.com",
            "SourceURL": "https://new.example/contact",
        }
    ]


def test_render_copy_friendly_lead_output_quotes_commas_without_header() -> None:
    rendered = render_copy_friendly_lead_output(
        [
            {
                "AuthorName": "Doe, Jane",
                "AuthorEmail": "jane@example.com",
                "SourceURL": "https://source.example/contact",
            }
        ]
    )

    assert rendered == (
        "```\n"
        "\"Doe, Jane\",jane@example.com\n"
        "```\n\n"
        "Doe, Jane — https://source.example/contact"
    )


def test_unknown_run_in_existing_context_raises_key_error(tmp_path: Path) -> None:
    runs_dir = tmp_path / "outputs" / "runs"
    runs_dir.mkdir(parents=True)
    _write(
        runs_dir / "run_001_stats.json",
        json.dumps({"generated_at_utc": "2026-03-14T10:00:00Z", "pipeline_exit_code": 0}),
    )

    with pytest.raises(KeyError):
        load_run_manifest(compose_api_run_id("project-root", "999"), base_dir=tmp_path)


def test_load_live_status_prefers_file_and_falls_back_to_runner_state(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    _write(
        state_dir / "live_status.json",
        json.dumps(
            {
                "active": True,
                "status": "running",
                "stage": "validation_complete",
                "run_tag": "run_004",
            }
        ),
    )

    payload = load_live_status(base_dir=tmp_path, runner_status_provider=lambda: {"active": False})
    assert payload["run_tag"] == "run_004"
    assert payload["source"] == "live_status"

    (state_dir / "live_status.json").unlink()
    runner_payload = load_live_status(
        base_dir=tmp_path,
        runner_status_provider=lambda: {
            "active": True,
            "status_label": "running",
            "started_at": "2026-03-15T12:00:00Z",
            "run_root": str(tmp_path / "browser_run_1"),
            "runs_dir": str(tmp_path / "browser_run_1" / "outputs" / "runs"),
            "log_path": str(tmp_path / "browser_run_1" / "outputs" / "logs" / "dashboard_run.log"),
        },
    )

    assert runner_payload["active"] is True
    assert runner_payload["source"] == "runner_state"
    assert runner_payload["status"] == "running"


def test_load_live_status_ignores_stale_runner_state(tmp_path: Path) -> None:
    payload = load_live_status(
        base_dir=tmp_path,
        runner_status_provider=lambda: {
            "active": False,
            "status_label": "idle",
            "exit_code": None,
            "finished_at": "",
            "run_root": str(tmp_path / "missing_run"),
            "log_path": str(tmp_path / "missing_run" / "outputs" / "logs" / "dashboard_run.log"),
        },
    )

    assert payload["status"] == "idle"
    assert payload["source"] == "none"


def test_load_live_status_prefers_terminal_runner_state_over_stale_live_status_file(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    _write(
        state_dir / "live_status.json",
        json.dumps(
            {
                "active": True,
                "status": "running",
                "stage": "validation_complete",
                "run_tag": "run_004",
            }
        ),
    )

    payload = load_live_status(
        base_dir=tmp_path,
        runner_status_provider=lambda: {
            "active": False,
            "status_label": "stopped",
            "started_at": "2026-03-24T14:21:32Z",
            "stopped_at": "2026-03-24T14:23:26Z",
            "run_root": str(tmp_path / "browser_run_1"),
            "runs_dir": str(tmp_path / "browser_run_1" / "outputs" / "runs"),
            "log_path": str(tmp_path / "browser_run_1" / "outputs" / "logs" / "dashboard_run.log"),
        },
    )

    assert payload["status"] == "stopped"
    assert payload["source"] == "runner_state"
