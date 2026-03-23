from __future__ import annotations

from prospect_validate import CandidateBudgetMetrics, map_stable_fail_reason, serialize_candidate_outcome


def test_map_stable_fail_reason_covers_strict_categories() -> None:
    assert map_stable_fail_reason("page_fetch_failed_404") == "candidate_unreachable"
    assert map_stable_fail_reason("non_author_website_after_name") == "candidate_not_author_site"
    assert map_stable_fail_reason("non_us_location") == "non_us_author"
    assert map_stable_fail_reason("publisher_location_only") == "missing_us_proof"
    assert map_stable_fail_reason("weak_strict_email_quality") == "email_not_visible_text"
    assert map_stable_fail_reason("listing_title_mismatch") == "listing_not_buyable"
    assert map_stable_fail_reason("listing_not_found") == "missing_listing_proof"
    assert map_stable_fail_reason("no_contact_or_subscribe_path") == "missing_contact_path"


def test_serialize_candidate_outcome_emits_stable_fail_reason_payload() -> None:
    payload = serialize_candidate_outcome(
        CandidateBudgetMetrics(
            candidate_url="https://author.example/about",
            candidate_domain="author.example",
            source_type="web_search",
            source_query='"indie author" "contact"',
            source_url="https://search.example/result",
            reject_reason="non_us_location",
            kept=False,
            current_state="dead_end",
            next_action="stop_dead_end",
            next_action_reason="non_us_location",
            email="Jane@Example.com",
            email_snippet="jane@example.com",
            us_snippet="Based in Dublin, Ireland.",
            book_url="https://www.amazon.com/dp/B012345678",
        )
    )

    assert payload["passed"] is False
    assert payload["primary_fail_reason"] == "non_us_author"
    assert payload["fail_reasons"] == ["non_us_author"]
    assert payload["confidence"] == "weak"
    assert payload["email"] == "jane@example.com"


def test_serialize_candidate_outcome_kept_rows_have_no_fail_reasons() -> None:
    payload = serialize_candidate_outcome(
        CandidateBudgetMetrics(
            candidate_url="https://author.example/about",
            candidate_domain="author.example",
            reject_reason="kept",
            kept=True,
            current_state="verified",
        )
    )

    assert payload["passed"] is True
    assert payload["primary_fail_reason"] == ""
    assert payload["fail_reasons"] == []
    assert payload["confidence"] == "strong"
