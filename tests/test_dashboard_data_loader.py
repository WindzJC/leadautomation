from __future__ import annotations

import json
from pathlib import Path

from dashboard.data_loader import (
    SCOUTED_EXPORT_COLUMNS,
    VERIFIED_EXPORT_COLUMNS,
    build_candidate_index,
    discover_run_contexts,
    filter_rows,
    latest_run_tag,
    load_context_outputs,
    load_run_bundle,
    resolve_context,
)
from dashboard.utils import metric_value


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_discover_run_contexts_and_latest_tag(tmp_path: Path) -> None:
    root_runs = tmp_path / "runs"
    child_root = tmp_path / "strict_full_post_locationfix_20260312"
    child_runs = child_root / "runs"
    child_runs.mkdir(parents=True)
    root_runs.mkdir()

    _write(root_runs / "run_001_stats.json", "{}")
    _write(child_runs / "run_002_stats.json", "{}")

    contexts = discover_run_contexts(tmp_path)

    assert {context.name for context in contexts} == {
        "strict_full_post_locationfix_20260312",
        "project-root",
    }
    assert latest_run_tag(child_runs) == "002"
    resolved = resolve_context(str(child_root))
    assert resolved is not None
    assert resolved.runs_path == child_runs


def test_load_context_outputs_reads_verified_queue_and_near_miss(tmp_path: Path) -> None:
    run_root = tmp_path / "strict_full_post_locationfix_20260312"
    runs_dir = run_root / "runs"
    runs_dir.mkdir(parents=True)
    _write(run_root / "fully_verified_leads.csv", "A Writer,A Book,a@example.com,https://example.com/about\n")
    _write(run_root / "scouted_leads.csv", "Scout Writer,scout@example.com,https://example.com/scout\n")
    _write(
        run_root / "contact_queue.csv",
        "AuthorName,BookTitle,AuthorEmail,ContactURL,SourceURL\n"
        "A Writer,A Book,a@example.com,https://example.com/contact,https://example.com/about\n",
    )
    _write(
        run_root / "near_miss_location.csv",
        "AuthorName,BookTitle,AuthorEmail,SourceURL,RejectReason,BestLocationSnippet,CandidateRecoveryURLs\n"
        "A Writer,A Book,a@example.com,https://example.com/about,publisher_location_only,\"Bio text\",https://example.com/about\n",
    )
    _write(run_root / "author_email_source.csv", "A Writer,a@example.com,https://example.com/about\n")

    context = resolve_context(str(run_root))
    assert context is not None

    outputs = load_context_outputs(context)

    assert outputs["fully_verified_leads"] == [
        dict(zip(VERIFIED_EXPORT_COLUMNS, ["A Writer", "a@example.com", "https://example.com/about"]))
    ]
    assert outputs["scouted_leads"] == [
        dict(zip(SCOUTED_EXPORT_COLUMNS, ["Scout Writer", "scout@example.com", "https://example.com/scout"]))
    ]
    assert outputs["contact_queue"][0]["AuthorName"] == "A Writer"
    assert outputs["near_miss_location"][0]["RejectReason"] == "publisher_location_only"
    assert outputs["lead_export_rows"] == 1


def test_load_run_bundle_and_build_candidate_index(tmp_path: Path) -> None:
    run_root = tmp_path / "strict_full_post_locationfix_20260312"
    runs_dir = run_root / "runs"
    runs_dir.mkdir(parents=True)

    _write(runs_dir / "run_001_stats.json", json.dumps({"harvested_candidates": 80}))
    _write(
        runs_dir / "run_001_validate_stats.json",
        json.dumps({"reject_reasons": {"non_us_location": 3}, "verified_progress": 1}),
    )
    _write(
        runs_dir / "run_001_validated.csv",
        "AuthorName,SourceURL,AuthorWebsite,ContactURL,ListingURL\n"
        "A Writer,https://example.com/about,https://example.com,https://example.com/contact,\n",
    )
    _write(
        runs_dir / "run_001_validate_stats_listing_debug.jsonl",
        json.dumps(
            {
                "candidate_url": "https://example.com/about",
                "candidate_domain": "example.com",
                "reject_reason": "listing_not_found",
                "best_listing_candidate_url": "https://amazon.com/dp/123",
            }
        )
        + "\n",
    )
    _write(
        runs_dir / "run_001_validate_stats_location_debug.jsonl",
        json.dumps(
            {
                "candidate_url": "https://example.com/about",
                "candidate_domain": "example.com",
                "final_decision": "ok",
                "selected_source_url": "https://example.com/about",
            }
        )
        + "\n",
    )

    context = resolve_context(str(run_root))
    assert context is not None

    bundle = load_run_bundle(context, "001")
    candidate_index = build_candidate_index(bundle)

    assert bundle["stats"]["harvested_candidates"] == 80
    assert bundle["validate_stats"]["verified_progress"] == 1
    assert len(candidate_index) == 1
    assert candidate_index[0]["candidate_domain"] == "example.com"
    assert candidate_index[0]["listing_debug"]["reject_reason"] == "listing_not_found"
    assert candidate_index[0]["location_debug"]["final_decision"] == "ok"


def test_filter_rows_matches_requested_fields() -> None:
    rows = [
        {"AuthorName": "Zack Argyle", "BookTitle": "Voice of War", "SourceURL": "https://example.com/about"},
        {"AuthorName": "Tamika Reed", "BookTitle": "A Book", "SourceURL": "https://example.org/about"},
    ]

    filtered = filter_rows(rows, "zack", fields=["AuthorName", "BookTitle"])

    assert filtered == [rows[0]]


def test_metric_value_handles_dict_total_and_missing_values() -> None:
    assert metric_value({"total": 80, "per_source": {"directory": 80}}) == 80
    assert metric_value({"per_source": {"directory": 80}}) == 0
    assert metric_value(None) == 0
    assert metric_value(12) == 12
