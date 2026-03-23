from __future__ import annotations

from pathlib import Path

from run_lead_finder_loop import build_rejected_rows, build_run_manifest_payload, build_duplicate_rejected_rows


def test_build_rejected_rows_keeps_reviewable_fields() -> None:
    rows = build_rejected_rows(
        [
            {
                "author_name": "Jane Doe",
                "book_title": "Skyfall",
                "candidate_url": "https://author.example/about",
                "candidate_domain": "author.example",
                "source_url": "https://search.example/result",
                "book_url": "https://www.amazon.com/dp/B012345678",
                "email": "jane@example.com",
                "confidence": "weak",
                "primary_fail_reason": "non_us_author",
                "fail_reasons": ["non_us_author"],
                "reject_reason": "non_us_location",
                "current_state": "dead_end",
                "next_action": "stop_dead_end",
                "next_action_reason": "non_us_location",
                "email_snippet": "jane@example.com",
                "us_snippet": "Based in Dublin, Ireland.",
                "indie_snippet": "independently published",
                "listing_snippet": "Paperback available",
                "passed": False,
            }
        ],
        "run_001",
    )

    assert rows == [
        {
            "RunID": "run_001",
            "AuthorName": "Jane Doe",
            "BookTitle": "Skyfall",
            "CandidateURL": "https://author.example/about",
            "CandidateDomain": "author.example",
            "SourceURL": "https://search.example/result",
            "BookURL": "https://www.amazon.com/dp/B012345678",
            "Email": "jane@example.com",
            "Confidence": "weak",
            "PrimaryFailReason": "non_us_author",
            "FailReasons": "non_us_author",
            "RejectReason": "non_us_location",
            "CurrentState": "dead_end",
            "NextAction": "stop_dead_end",
            "NextActionReason": "non_us_location",
            "EmailSnippet": "jane@example.com",
            "USSnippet": "Based in Dublin, Ireland.",
            "IndieSnippet": "independently published",
            "ListingSnippet": "Paperback available",
        }
    ]


def test_build_duplicate_rejected_rows_marks_duplicate_reason() -> None:
    rows = build_duplicate_rejected_rows(
        [
            {
                "AuthorName": "Jane Doe",
                "BookTitle": "Skyfall",
                "AuthorEmail": "Jane@Example.com",
                "SourceURL": "https://author.example/about",
                "ListingURL": "https://www.amazon.com/dp/B012345678",
            }
        ],
        "run_002",
    )

    assert rows[0]["PrimaryFailReason"] == "duplicate_lead"
    assert rows[0]["FailReasons"] == "duplicate_lead"
    assert rows[0]["CandidateDomain"] == "author.example"


def test_build_run_manifest_payload_includes_counts_and_artifacts(tmp_path: Path) -> None:
    manifest = build_run_manifest_payload(
        run_tag="run_003",
        goal_target=20,
        validation_profile="strict_full",
        pipeline_exit_code=0,
        counts={
            "harvested_candidates": 80,
            "validated": 7,
            "rejected": 12,
            "duplicates": 2,
        },
        rejected_rows=[
            {"PrimaryFailReason": "non_us_author", "FailReasons": "non_us_author"},
            {"PrimaryFailReason": "missing_visible_email", "FailReasons": "missing_visible_email"},
            {"PrimaryFailReason": "non_us_author", "FailReasons": "non_us_author"},
        ],
        artifact_paths={
            "validated_csv": tmp_path / "validated_run_003.csv",
            "rejected_csv": tmp_path / "rejected_run_003.csv",
        },
    )

    assert manifest["run_id"] == "run_003"
    assert manifest["status"] == "completed"
    assert manifest["counts"]["validated"] == 7
    assert manifest["rejected_reason_counts"] == {
        "non_us_author": 2,
        "missing_visible_email": 1,
    }
    assert manifest["artifacts"]["validated_csv"].endswith("validated_run_003.csv")
