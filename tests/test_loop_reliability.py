from __future__ import annotations

from argparse import Namespace

from run_lead_finder_loop import determine_loop_stop_reason, update_reliability_counters


def test_update_reliability_counters_tracks_failures_and_empty_runs() -> None:
    counters = update_reliability_counters(
        {},
        pipeline_failed=True,
        filtered_candidates=0,
        validated_rows=0,
    )
    assert counters["consecutive_failures"] == 1
    assert counters["consecutive_empty_candidate_runs"] == 0
    assert counters["consecutive_zero_validated_runs"] == 0

    counters = update_reliability_counters(
        counters,
        pipeline_failed=False,
        filtered_candidates=0,
        validated_rows=0,
    )
    assert counters["consecutive_failures"] == 0
    assert counters["consecutive_empty_candidate_runs"] == 1
    assert counters["consecutive_zero_validated_runs"] == 1


def test_determine_loop_stop_reason_respects_priority_order() -> None:
    args = Namespace(
        validation_profile="strict_full",
        max_stale_runs=2,
        max_consecutive_failures=3,
        max_empty_candidate_runs=2,
        max_zero_validated_runs=3,
    )

    assert determine_loop_stop_reason(
        args,
        stale_runs=2,
        reliability_counters={
            "consecutive_failures": 3,
            "consecutive_empty_candidate_runs": 2,
            "consecutive_zero_validated_runs": 3,
        },
    ) == "stale_runs"
    assert determine_loop_stop_reason(
        args,
        stale_runs=0,
        reliability_counters={
            "consecutive_failures": 3,
            "consecutive_empty_candidate_runs": 0,
            "consecutive_zero_validated_runs": 0,
        },
    ) == "consecutive_failures"
    assert determine_loop_stop_reason(
        args,
        stale_runs=0,
        reliability_counters={
            "consecutive_failures": 0,
            "consecutive_empty_candidate_runs": 2,
            "consecutive_zero_validated_runs": 0,
        },
    ) == "empty_candidate_runs"


def test_determine_loop_stop_reason_ignores_stale_runs_for_recovery_profiles() -> None:
    args = Namespace(
        validation_profile="email_only",
        max_stale_runs=2,
        max_consecutive_failures=3,
        max_empty_candidate_runs=2,
        max_zero_validated_runs=3,
    )

    assert determine_loop_stop_reason(
        args,
        stale_runs=2,
        reliability_counters={
            "consecutive_failures": 0,
            "consecutive_empty_candidate_runs": 0,
            "consecutive_zero_validated_runs": 0,
        },
    ) == ""
