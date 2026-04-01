from __future__ import annotations

import csv
import json
from argparse import Namespace
from collections import Counter
from unittest.mock import patch

import requests
from bs4 import BeautifulSoup

from enrich_queue import is_staged_row, queue_row_candidate_url, queue_row_to_candidate
from prospect_dedupe import dedupe
from prospect_harvest import rotate_candidate_groups
from prospect_validate import (
    CandidateProofLedger,
    FetchResult,
    apply_validation_profile_defaults as apply_validator_profile_defaults,
    append_candidate_action,
    classify_location_evidence,
    classify_listing_fetch_failure,
    classify_listing_failure,
    collect_sitemap_urls,
    directory_indie_proof,
    discover_book_page_urls,
    discover_same_domain_listing_support_urls,
    extract_book_title_from_book_page,
    extract_book_title_from_links,
    extract_book_title_from_source_text,
    extract_listing_candidates_from_page,
    extract_page_email_records,
    is_actionable_contact_path,
    is_allowed_by_robots,
    is_likely_author_website,
    is_plausible_author_name,
    is_plausible_book_title,
    normalize_listing_fail_reason,
    normalize_book_title,
    parse_sitemap_xml,
    plan_candidate_next_action,
    row_meets_fully_verified_profile,
    resolve_listing_title_oracle,
    resolve_primary_book_title,
    should_track_listing_reject_reason,
    validate_candidates,
)
from run_lead_finder import apply_validation_profile_defaults as apply_single_run_profile_defaults
from run_lead_finder_loop import (
    assess_agent_hunt_row,
    assess_agent_hunt_listing_blocked_record,
    apply_validation_profile_defaults,
    build_agent_hunt_source_quality_feedback,
    build_agent_hunt_stats,
    build_agent_hunt_listing_feedback,
    build_email_only_source_yield_stats,
    build_rotating_queries,
    keep_verified_rows,
    orchestrate_candidate_replacements,
    order_candidates_for_strict_validation,
    recover_agent_hunt_listing_friction_email_record,
    row_is_agent_hunt_qualified,
    row_is_email_only_qualified,
    row_qualifies_for_master,
    score_candidate_intake,
    split_rows_by_merge_policy,
    suppress_agent_hunt_source_quality_candidates,
    suppress_pre_validate_candidates,
    summarize_candidate_intake_scores,
    throttle_agent_hunt_epic_directory_candidates,
    write_minimal_rows,
    write_scouted_rows,
    write_verified_rows,
)


class FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class FakeSession:
    def __init__(self, events: list[object]) -> None:
        self._events = list(events)
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, timeout: float):
        self.calls.append((url, timeout))
        if not self._events:
            raise AssertionError("No more fake events configured")
        event = self._events.pop(0)
        if isinstance(event, Exception):
            raise event
        return event


def make_fetch_result(
    url: str,
    payload: str | tuple[str, str] | tuple[str, str, str] | None,
    expect_html: bool = True,
) -> FetchResult:
    if payload is None:
        return FetchResult(url=url, failure_reason="404", status_code=404)

    text = payload if isinstance(payload, str) else payload[0]
    content_type = "text/html"
    final_url = url
    if isinstance(payload, tuple):
        content_type = payload[1]
        if len(payload) >= 3:
            final_url = payload[2]

    soup = BeautifulSoup(text, "html.parser") if expect_html and "html" in content_type else None
    return FetchResult(
        url=url,
        final_url=final_url,
        text=text,
        soup=soup,
        content_type=content_type,
        status_code=200,
    )


def fake_fetch_with_meta_factory(pages: dict[str, str | tuple[str, str] | tuple[str, str, str]]):
    def fake_fetch_with_meta(  # noqa: ANN001
        session,
        url,
        timeout,
        robots_cache,
        robots_text_cache,
        ignore_robots,
        retry_seconds,
        expect_html=True,
    ):
        return make_fetch_result(url, pages.get(url), expect_html=expect_html)

    return fake_fetch_with_meta


def counting_fetch_with_meta_factory(
    pages: dict[str, str | tuple[str, str] | tuple[str, str, str] | None],
    counts: dict[str, int],
):
    def fake_fetch_with_meta(  # noqa: ANN001
        session,
        url,
        timeout,
        robots_cache,
        robots_text_cache,
        ignore_robots,
        retry_seconds,
        expect_html=True,
    ):
        counts[url] = counts.get(url, 0) + 1
        return make_fetch_result(url, pages.get(url), expect_html=expect_html)

    return fake_fetch_with_meta


def make_validator_args(tmp_path, candidates_path, *, validation_profile: str = "fully_verified") -> Namespace:
    return Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=2,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile=validation_profile,
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )


def test_keep_verified_rows_balanced_and_strict() -> None:
    rows = [
        {"VerificationStatus": "deliverable", "HasMX": "True", "IsRoleAddress": "False", "id": "d"},
        {"VerificationStatus": "risky", "HasMX": "True", "IsRoleAddress": "False", "id": "r"},
        {"VerificationStatus": "risky", "HasMX": "False", "IsRoleAddress": "False", "id": "rmx"},
        {"VerificationStatus": "deliverable", "HasMX": "True", "IsRoleAddress": "True", "id": "role"},
        {"VerificationStatus": "blocked_role", "HasMX": "False", "IsRoleAddress": "True", "id": "blocked"},
    ]

    balanced = keep_verified_rows(rows, gate="balanced")
    strict = keep_verified_rows(rows, gate="strict")

    assert [row["id"] for row in balanced] == ["d", "r", "role"]
    assert [row["id"] for row in strict] == ["d", "role"]


def test_contact_path_strict_mode() -> None:
    author_site = "https://author.example"
    candidate = "https://author.example/book"

    assert is_actionable_contact_path("https://author.example/about", author_site, candidate, strict=False)
    assert not is_actionable_contact_path("https://author.example/about", author_site, candidate, strict=True)
    assert is_actionable_contact_path("https://author.example/contact", author_site, candidate, strict=True)


def test_plan_candidate_next_action_marks_verified_when_strict_proofs_exist() -> None:
    ledger = CandidateProofLedger(
        candidate_url="https://author.example",
        candidate_domain="author.example",
        current_state="triage_ready",
        best_email_source_url="https://author.example/contact",
        best_location_source_url="https://author.example/about",
        best_listing_source_url="https://www.amazon.com/dp/B012345678",
        best_recency_source_url="https://author.example/news",
        best_indie_source_url="https://author.example/about",
    )

    plan = plan_candidate_next_action(
        ledger,
        strict_verified_mode=True,
        require_us_location=True,
        effective_require_email=True,
        effective_listing_strict=True,
    )

    assert plan == {"state": "verified", "action": "mark_verified", "reason": "strict_proofs_satisfied"}


def test_plan_candidate_next_action_prefers_listing_recovery_for_listing_not_found() -> None:
    ledger = CandidateProofLedger(
        candidate_url="https://author.example",
        candidate_domain="author.example",
        current_state="triage_ready",
        reject_reason_if_any="listing_not_found",
        best_email_source_url="https://author.example/contact",
        best_location_source_url="https://author.example/about",
    )
    append_candidate_action(ledger, "try_email_proof", detail="verified")

    plan = plan_candidate_next_action(
        ledger,
        strict_verified_mode=True,
        require_us_location=True,
        effective_require_email=True,
        effective_listing_strict=True,
    )

    assert plan == {"state": "needs_listing_proof", "action": "try_listing_proof", "reason": "listing_not_found"}


def test_plan_candidate_next_action_prefers_location_recovery_for_publisher_location_only() -> None:
    ledger = CandidateProofLedger(
        candidate_url="https://author.example",
        candidate_domain="author.example",
        current_state="triage_ready",
        reject_reason_if_any="publisher_location_only",
        best_email_source_url="https://author.example/contact",
    )

    plan = plan_candidate_next_action(
        ledger,
        strict_verified_mode=True,
        require_us_location=True,
        effective_require_email=True,
        effective_listing_strict=True,
    )

    assert plan == {
        "state": "needs_location_proof",
        "action": "try_location_proof",
        "reason": "publisher_location_only",
    }


def test_plan_candidate_next_action_stops_on_bad_author_name_dead_end() -> None:
    ledger = CandidateProofLedger(
        candidate_url="https://author.example",
        candidate_domain="author.example",
        current_state="triage_ready",
        reject_reason_if_any="bad_author_name",
    )

    plan = plan_candidate_next_action(
        ledger,
        strict_verified_mode=True,
        require_us_location=True,
        effective_require_email=True,
        effective_listing_strict=True,
    )

    assert plan == {"state": "dead_end", "action": "stop_dead_end", "reason": "bad_author_name"}


def test_validate_candidates_planner_drives_listing_recovery(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>jane@janedoeauthor.com</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Kindle $4.99 Buy now Available</p></body></html>
        """,
    }
    counts: dict[str, int] = {}
    args = make_validator_args(tmp_path, candidates_path, validation_profile="fully_verified")

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert len(rows) == 1
    assert reject_counts == Counter()
    assert counts.get("https://janedoeauthor.com/extras", 0) == 1
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is True
    assert runtime_stats["top_candidate_budget_burn"][0]["current_state"] == "verified"
    assert runtime_stats["top_candidate_budget_burn"][0]["next_action"] == "mark_verified"


def test_validate_candidates_planner_drives_location_recovery(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>jane@janedoeauthor.com</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/about-author": """
            <html><body>
              <p>Jane Doe is a fantasy author based in Austin, Texas.</p>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now Available</p></body></html>
        """,
    }
    counts: dict[str, int] = {}
    args = make_validator_args(tmp_path, candidates_path, validation_profile="fully_verified")

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert len(rows) == 1
    assert reject_counts == Counter()
    assert counts.get("https://janedoeauthor.com/about-author", 0) >= 1
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["location_recovery_attempted"] is True
    assert runtime_stats["top_candidate_budget_burn"][0]["current_state"] == "verified"


def test_validate_candidates_planner_stops_on_dead_end_before_listing_recovery(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Toronto, Canada.</p>
              <p>jane@janedoeauthor.com</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}
    args = make_validator_args(tmp_path, candidates_path, validation_profile="fully_verified")

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["non_us_location"] == 1
    assert counts.get("https://janedoeauthor.com/extras", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["current_state"] == "dead_end"
    assert runtime_stats["top_candidate_budget_burn"][0]["next_action"] == "stop_dead_end"


def test_validate_candidates_planner_marks_verified_without_extra_recovery(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>jane@janedoeauthor.com</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Kindle $4.99 Buy now Available</p></body></html>
        """,
    }
    counts: dict[str, int] = {}
    args = make_validator_args(tmp_path, candidates_path, validation_profile="fully_verified")

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert len(rows) == 1
    assert reject_counts == Counter()
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["current_state"] == "verified"
    assert runtime_stats["top_candidate_budget_burn"][0]["next_action"] == "mark_verified"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is False
    assert runtime_stats["top_candidate_budget_burn"][0]["location_recovery_attempted"] is False


def test_robots_200_policy_is_enforced() -> None:
    robots_txt = "User-agent: *\nDisallow: /private\n"
    session = FakeSession([FakeResponse(200, robots_txt)])
    cache = {}

    disallowed = is_allowed_by_robots(
        session=session,
        url="https://example.com/private/page",
        timeout=5.0,
        robots_cache=cache,
        robots_text_cache={},
        ignore_robots=False,
        retry_seconds=60.0,
    )
    allowed = is_allowed_by_robots(
        session=session,
        url="https://example.com/public/page",
        timeout=5.0,
        robots_cache=cache,
        robots_text_cache={},
        ignore_robots=False,
        retry_seconds=60.0,
    )

    assert not disallowed
    assert allowed
    assert len(session.calls) == 1


def test_robots_4xx_treated_as_unavailable_allow() -> None:
    session = FakeSession([FakeResponse(404, "")])
    cache = {}

    allowed = is_allowed_by_robots(
        session=session,
        url="https://example.com/anything",
        timeout=5.0,
        robots_cache=cache,
        robots_text_cache={},
        ignore_robots=False,
        retry_seconds=60.0,
    )

    assert allowed
    assert len(session.calls) == 1


def test_robots_5xx_temporary_disallow_then_retry() -> None:
    session = FakeSession([FakeResponse(503, ""), FakeResponse(404, "")])
    cache = {}

    with patch("prospect_validate.time.time", return_value=1000.0):
        first = is_allowed_by_robots(
            session=session,
            url="https://example.com/path",
            timeout=5.0,
            robots_cache=cache,
            robots_text_cache={},
            ignore_robots=False,
            retry_seconds=60.0,
        )

    with patch("prospect_validate.time.time", return_value=1001.0):
        second = is_allowed_by_robots(
            session=session,
            url="https://example.com/path",
            timeout=5.0,
            robots_cache=cache,
            robots_text_cache={},
            ignore_robots=False,
            retry_seconds=60.0,
        )

    with patch("prospect_validate.time.time", return_value=1061.0):
        third = is_allowed_by_robots(
            session=session,
            url="https://example.com/path",
            timeout=5.0,
            robots_cache=cache,
            robots_text_cache={},
            ignore_robots=False,
            retry_seconds=60.0,
        )

    assert not first
    assert not second
    assert third
    assert len(session.calls) == 2


def test_robots_unreachable_temporary_disallow() -> None:
    session = FakeSession([requests.RequestException("boom")])
    cache = {}

    allowed = is_allowed_by_robots(
        session=session,
        url="https://example.com/path",
        timeout=5.0,
        robots_cache=cache,
        robots_text_cache={},
        ignore_robots=False,
        retry_seconds=60.0,
    )

    assert not allowed
    assert len(session.calls) == 1


def test_author_website_quality_filter() -> None:
    assert is_likely_author_website("https://nicolaslietzau.com")
    assert not is_likely_author_website("https://www.imdb.com")
    assert not is_likely_author_website("https://www.goodreads.com/author/show/123")


def test_author_name_quality_filter() -> None:
    assert is_plausible_author_name("Nicolas Lietzau")
    assert not is_plausible_author_name("Advanced search")
    assert not is_plausible_author_name("Unknown Author")
    assert not is_plausible_author_name("d.mammina@yahoo.com")
    assert not is_plausible_author_name("Tao Websites & Graphic Design")
    assert not is_plausible_author_name("SHORT STORY ANTHOLOGIES")


def test_book_title_quality_filter() -> None:
    assert is_plausible_book_title("Dreams of the Dying", author_name="Nicolas Lietzau")
    assert not is_plausible_book_title("Advanced search", author_name="Nicolas Lietzau")
    assert not is_plausible_book_title("Nicolas Lietzau", author_name="Nicolas Lietzau")
    assert not is_plausible_book_title("Author | Lee C. Conley", author_name="Lee C. Conley")
    assert not is_plausible_book_title("+5", author_name="Nicolas Lietzau")
    assert not is_plausible_book_title("Daan Katz Author", author_name="Daan Katz", context={"source_method": "source_title"})
    assert not is_plausible_book_title("A.R. Henle", author_name="Alea Henle", context={"source_method": "source_title"})
    assert not is_plausible_book_title("Writer | Lee C. Conley", author_name="Lee C. Conley", context={"source_method": "source_title"})
    assert not is_plausible_book_title("Lee C. Conley | Books", author_name="Lee C. Conley", context={"source_method": "source_title"})
    assert not is_plausible_book_title("Follow the author", author_name="Virtuous Cornwall", context={"source_method": "listing_title_oracle"})
    assert not is_plausible_book_title("Skip to", author_name="C.T. Phipps", context={"source_method": "listing_title_oracle"})
    assert not is_plausible_book_title("Navigation Menu", author_name="Jane Doe", context={"source_method": "listing_title_oracle"})
    assert is_plausible_book_title("Skyfall", author_name="Jane Doe", context={"source_method": "jsonld_book"})
    assert is_plausible_book_title(
        "It",
        author_name="Stephen King",
        context={"source_method": "listing_title_oracle", "strong_book_evidence": True},
    )


def test_listing_title_oracle_rejects_nav_boilerplate() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <h1>Follow the author</h1>
          <h2>Skip to</h2>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_listing_title_oracle(
        "https://www.amazon.com/dp/B0G2KWM2P2/",
        soup.get_text(" ", strip=True),
        soup,
        author_name="C.T. Phipps",
    )

    assert result == {}


def test_book_title_can_be_recovered_from_same_site_links() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <a href="/news-and-events">News and Events</a>
          <a href="/a-ritual-of-bone">A Ritual of Bone</a>
          <a href="/contact">Contact</a>
        </body></html>
        """,
        "html.parser",
    )

    title = extract_book_title_from_links("https://www.leeconleyauthor.com", soup, author_name="Lee C. Conley")

    assert title == "A Ritual of Bone"


def test_book_title_can_be_recovered_from_directory_snippet() -> None:
    snippet = (
        "M.S. Olney The website of indie scifi and fantasy author and EPIC founder M.S. Olney. "
        "Author of the Sundered Crown Saga and the Empowered Ones series plus much more!"
    )

    title = extract_book_title_from_source_text(snippet)

    assert title == "The Sundered Crown Saga"


def test_book_title_can_be_recovered_from_recency_snippet() -> None:
    snippet = "Wizards of Dragon Keep is now available February 7, 2026."

    title = extract_book_title_from_source_text(snippet)

    assert title == "Wizards of Dragon Keep"


def test_book_title_source_text_strips_search_for_prefix() -> None:
    snippet = "Search for: Wizards of Dragon Keep is now available February 7, 2026."

    title = extract_book_title_from_source_text(snippet)

    assert title == "Wizards of Dragon Keep"


def test_pre_validate_suppression_uses_listing_domain_and_author_name() -> None:
    master_rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoeauthor.com",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "SubscribeURL": "",
            "ListingURL": "https://www.amazon.com/dp/B012345678",
        }
    ]
    candidate_rows = [
        {
            "CandidateURL": "https://janedoeauthor.com/about",
            "SourceQuery": "goodreads:author-outbound",
            "DiscoveredAtUTC": "2026-03-07T00:00:00Z",
        },
        {
            "CandidateURL": "https://www.amazon.com/dp/B012345678",
            "SourceQuery": "search",
            "DiscoveredAtUTC": "2026-03-07T00:00:00Z",
        },
        {
            "CandidateURL": "https://example.com/author/Jane-Doe",
            "SourceQuery": "search",
            "DiscoveredAtUTC": "2026-03-07T00:00:00Z",
        },
        {
            "CandidateURL": "https://freshauthor.com/contact",
            "SourceQuery": "search",
            "DiscoveredAtUTC": "2026-03-07T00:00:00Z",
        },
    ]

    kept, stats = suppress_pre_validate_candidates(candidate_rows, master_rows)

    assert [row["CandidateURL"] for row in kept] == ["https://freshauthor.com/contact"]
    assert stats["suppressed_pre_validate_total"] == 3
    assert stats["suppressed_pre_validate_by_reason"] == {
        "author_name": 1,
        "author_site_domain": 1,
        "listing_key": 1,
    }
    assert stats["new_unique_candidates_by_source"] == {"web_search": 1}
    assert stats["top_repeat_sources"][0] == {"source": "web_search", "duplicates": 2}
    assert stats["top_repeat_sources"][1] == {"source": "goodreads", "duplicates": 1}
    assert stats["duplicate_hit_rate_by_source"][0]["source"] == "web_search"
    assert stats["duplicate_hit_rate_by_source"][0]["duplicate_hit_rate"] == round(2 / 3, 3)


def test_pre_validate_suppression_reports_duplicate_rates_by_source_and_query() -> None:
    master_rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoeauthor.com",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "SubscribeURL": "",
            "ListingURL": "",
        }
    ]
    candidate_rows = [
        {
            "CandidateURL": "https://janedoeauthor.com/about",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
        },
        {
            "CandidateURL": "https://newauthor.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
        },
        {
            "CandidateURL": "https://fresh.example/contact",
            "SourceType": "web_search",
            "SourceQuery": "\"indie author\" \"contact\"",
        },
    ]

    kept, stats = suppress_pre_validate_candidates(candidate_rows, master_rows)

    assert [row["CandidateURL"] for row in kept] == [
        "https://newauthor.example/contact",
        "https://fresh.example/contact",
    ]
    assert stats["new_unique_candidates_by_source"] == {
        "epic_directory": 1,
        "web_search": 1,
    }
    assert stats["duplicate_hit_rate_by_source"][0] == {
        "source": "epic_directory",
        "duplicates": 1,
        "new": 1,
        "total": 2,
        "duplicate_hit_rate": 0.5,
    }
    assert stats["duplicate_hit_rate_by_query"][0] == {
        "query": "epic:author-directory",
        "duplicates": 1,
        "total": 2,
        "duplicate_hit_rate": 0.5,
    }


def test_pre_validate_suppression_allows_revisit_for_weak_title_rows() -> None:
    master_rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoeauthor.com",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "SubscribeURL": "",
            "ListingURL": "",
            "BookTitleStatus": "missing_or_weak",
        }
    ]
    candidate_rows = [
        {
            "CandidateURL": "https://janedoeauthor.com/about",
            "SourceQuery": "search",
            "DiscoveredAtUTC": "2026-03-07T00:00:00Z",
        }
    ]

    kept, stats = suppress_pre_validate_candidates(candidate_rows, master_rows)

    assert [row["CandidateURL"] for row in kept] == ["https://janedoeauthor.com/about"]
    assert stats["suppressed_pre_validate_total"] == 0


def test_dedupe_prefers_stronger_row_for_same_author() -> None:
    rows = [
        {
            "AuthorName": "Jane Doe",
            "BookTitle": "",
            "BookTitleStatus": "missing_or_weak",
            "BookTitleScore": "0",
            "AuthorEmail": "",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "SubscribeURL": "",
            "Location": "Unknown",
            "ListingStatus": "missing",
            "RecencyStatus": "missing",
        },
        {
            "AuthorName": "Jane Doe",
            "BookTitle": "Skyfall",
            "BookTitleStatus": "ok",
            "BookTitleScore": "42",
            "AuthorEmail": "",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "SubscribeURL": "",
            "Location": "Austin, TX",
            "ListingStatus": "verified",
            "RecencyStatus": "verified",
        },
    ]

    result = dedupe(rows)

    assert len(result) == 1
    assert result[0]["BookTitle"] == "Skyfall"
    assert result[0]["BookTitleStatus"] == "ok"


def test_row_qualifies_for_master_respects_merge_policy() -> None:
    staged_row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "source_title",
        "RecencyStatus": "missing",
    }
    balanced_row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "fallback",
        "RecencyStatus": "missing",
    }
    strict_row = {
        **balanced_row,
        "RecencyStatus": "verified",
    }
    fully_verified_row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "Location": "Austin, TX",
        "IndieProofStrength": "onsite",
        "ListingStatus": "verified",
        "RecencyStatus": "verified",
        "BookTitle": "",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "fallback",
    }

    assert not row_qualifies_for_master(staged_row, policy="strict")
    assert not row_qualifies_for_master(staged_row, policy="balanced")
    assert row_qualifies_for_master(staged_row, policy="open")
    assert row_qualifies_for_master(balanced_row, policy="balanced")
    assert not row_qualifies_for_master(balanced_row, policy="strict")
    assert row_qualifies_for_master(strict_row, policy="strict")
    assert row_qualifies_for_master(strict_row, policy="balanced")
    assert not row_qualifies_for_master(strict_row, policy="strict", validation_profile="fully_verified")
    assert row_qualifies_for_master(fully_verified_row, policy="strict", validation_profile="fully_verified")
    assert row_qualifies_for_master(fully_verified_row, policy="strict", validation_profile="astra_outbound")
    assert row_qualifies_for_master(fully_verified_row, policy="strict", validation_profile="strict_interactive")
    assert row_qualifies_for_master(fully_verified_row, policy="strict", validation_profile="strict_full")
    missing_location_row = dict(fully_verified_row)
    missing_location_row["Location"] = ""
    assert not row_qualifies_for_master(missing_location_row, policy="strict", validation_profile="fully_verified")
    assert not row_qualifies_for_master(missing_location_row, policy="strict", validation_profile="strict_interactive")
    assert not row_qualifies_for_master(missing_location_row, policy="strict", validation_profile="strict_full")
    assert row_qualifies_for_master(missing_location_row, policy="strict", validation_profile="verified_no_us")


def test_agent_hunt_accepts_scoutable_row_that_fails_strict_full() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://janedoeauthor.com/contact",
        "BookTitle": "",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "fallback",
        "BookTitleConfidence": "weak",
        "Location": "",
        "IndieProofStrength": "directory",
        "ListingStatus": "missing",
        "RecencyStatus": "missing",
    }

    assert row_is_agent_hunt_qualified(row)
    assert row_qualifies_for_master(row, policy="strict", validation_profile="agent_hunt")
    assert not row_qualifies_for_master(row, policy="strict", validation_profile="strict_full")


def test_email_only_accepts_lightweight_row_that_fails_strict_full_and_preserves_source_url(tmp_path) -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://directory.example/jane-doe",
        "BookTitle": "",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "fallback",
        "BookTitleConfidence": "weak",
        "Location": "",
        "IndieProofStrength": "",
        "ListingStatus": "missing",
        "RecencyStatus": "missing",
    }

    assert row_is_email_only_qualified(row)
    assert row_qualifies_for_master(row, policy="strict", validation_profile="email_only")
    assert not row_qualifies_for_master(row, policy="strict", validation_profile="strict_full")

    output_path = tmp_path / "author_email_source.csv"
    written = write_minimal_rows(output_path, [row], with_header=True, validation_profile="email_only")

    assert written == 1
    with output_path.open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows == [
        ["AuthorName", "AuthorEmail", "SourceURL"],
        ["Jane Doe", "jane@janedoeauthor.com", "https://directory.example/jane-doe"],
    ]


def test_agent_hunt_rejects_obvious_dead_end_non_us_row() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://janedoeauthor.com/contact",
        "BookTitle": "Skyfall",
        "BookTitleStatus": "ok",
        "Location": "Dublin, Ireland",
    }

    assert not row_is_agent_hunt_qualified(row)
    assert not row_qualifies_for_master(row, policy="strict", validation_profile="agent_hunt")


def test_agent_hunt_requires_source_url() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "",
        "BookTitle": "",
        "BookTitleStatus": "missing_or_weak",
    }

    assert not row_is_agent_hunt_qualified(row)
    assert not row_qualifies_for_master(row, policy="strict", validation_profile="agent_hunt")


def test_agent_hunt_marks_plausible_contactable_no_email_row_as_scoutworthy_not_outreach_ready() -> None:
    row = {
        "AuthorName": "Mary Roe",
        "AuthorWebsite": "https://maryroeauthor.com",
        "ContactPageURL": "https://maryroeauthor.com/contact",
        "AuthorEmail": "",
        "AuthorEmailSourceURL": "",
        "EmailQuality": "",
        "SourceURL": "https://maryroeauthor.com/about",
        "Location": "",
    }

    assessment = assess_agent_hunt_row(row)

    assert assessment == {
        "qualified": False,
        "status": "scoutworthy_not_outreach_ready",
        "reason": "scoutworthy_missing_visible_email",
        "decision": "RECHECK",
    }
    assert not row_is_agent_hunt_qualified(row)


def test_agent_hunt_marks_strong_row_as_qualified() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://janedoeauthor.com/about",
        "Location": "",
    }

    assessment = assess_agent_hunt_row(row)

    assert assessment == {
        "qualified": True,
        "status": "qualified",
        "reason": "",
        "decision": "KEEP",
        "outreach_tier": "tier_1_safest",
        "tier_reason": "tier_1_same_domain_clear_match",
        "email_match": "partial",
    }


def test_agent_hunt_accepts_public_email_row_without_contact_path_as_weaker_tier() -> None:
    row = {
        "AuthorName": "Rune S. Nielsen",
        "AuthorWebsite": "",
        "ContactPageURL": "",
        "AuthorEmail": "rune@runesnielsen.com",
        "AuthorEmailSourceURL": "https://runesnielsen.com",
        "EmailQuality": "same_domain",
        "SourceURL": "https://runesnielsen.com",
        "Location": "",
    }

    assessment = assess_agent_hunt_row(row)

    assert assessment["qualified"] is True
    assert assessment["status"] == "qualified"
    assert assessment["outreach_tier"] in {"tier_2_usable_with_caution", "tier_3_weak_but_usable"}
    assert assessment["decision"] == "RECHECK"


def test_agent_hunt_rejects_rep_or_agency_only_off_domain_email() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "rights@bigagency.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "labeled_off_domain",
        "SourceURL": "https://janedoeauthor.com/about",
        "Location": "",
    }

    assessment = assess_agent_hunt_row(row)

    assert assessment["qualified"] is False
    assert assessment["status"] == "rejected"
    assert assessment["reason"] == "rep_or_agency_only_email"
    assert assessment["decision"] == "REPLACE"


def test_agent_hunt_listing_blocked_record_is_classified_distinctly_from_hard_dead_end() -> None:
    listing_blocked_record = {
        "author_name": "Jane Doe",
        "candidate_url": "https://janedoeauthor.com/about",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "jane@janedoeauthor.com",
        "reject_reason": "listing_amazon_interstitial_no_bn_candidate",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
        "listing_snippet": "Available on Amazon",
    }
    listing_dead_end_record = {
        "author_name": "Jane Doe",
        "candidate_url": "https://janedoeauthor.com/about",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "jane@janedoeauthor.com",
        "reject_reason": "listing_not_found",
    }

    blocked = assess_agent_hunt_listing_blocked_record(listing_blocked_record)
    dead_end = assess_agent_hunt_listing_blocked_record(listing_dead_end_record)

    assert blocked["qualified"] is True
    assert blocked["status"] == "qualified_listing_blocked"
    assert blocked["listing_failure_category"] == "listing_retailer_friction_recoverable"
    assert dead_end["qualified"] is False
    assert dead_end["listing_failure_category"] == "listing_absent"


def test_agent_hunt_listing_blocked_record_routes_plausible_no_email_row_to_caution() -> None:
    record = {
        "author_name": "Jane Doe",
        "candidate_url": "https://janedoeauthor.com/about",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "",
        "reject_reason": "listing_amazon_interstitial_bn_blocked",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
        "listing_snippet": "Find the books on Amazon",
    }

    assessment = assess_agent_hunt_listing_blocked_record(record)

    assert assessment["qualified"] is False
    assert assessment["status"] == "scoutworthy_not_outreach_ready"
    assert assessment["reason"] == "listing_blocked:listing_amazon_interstitial_bn_blocked"
    assert assessment["promotion_blocker_reason"] == "listing_blocked:listing_amazon_interstitial_bn_blocked"


def test_agent_hunt_listing_blocked_record_still_rejects_obvious_low_value_row() -> None:
    record = {
        "author_name": "- Inspiring Creative Writing -",
        "candidate_url": "https://tansielexington.com",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "tansie@tansielexington.com",
        "reject_reason": "listing_amazon_interstitial_bn_blocked",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
    }

    assessment = assess_agent_hunt_listing_blocked_record(record)

    assert assessment["qualified"] is False
    assert assessment["reason"] == "bad_author_name_generic_phrase"


def test_recover_agent_hunt_listing_friction_email_record_promotes_visible_email_row() -> None:
    record = {
        "author_name": "Mary Roe",
        "candidate_url": "https://maryroeauthor.com/about",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "",
        "reject_reason": "listing_amazon_interstitial_no_bn_candidate",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
        "listing_snippet": "Available on Amazon",
    }
    pages = {
        "https://maryroeauthor.com/about": """
            <html><body>
              <a href="/contact">Contact Mary</a>
            </body></html>
        """,
        "https://maryroeauthor.com/contact": """
            <html><body>
              <p>For media inquiries email mary@maryroeauthor.com</p>
            </body></html>
        """,
    }

    def fake_fetch(url: str) -> dict[str, object]:
        normalized = url.rstrip("/")
        html = pages.get(normalized)
        if html is None:
            return {
                "ok": False,
                "url": normalized,
                "text": "",
                "soup": None,
                "failure_reason": "404",
            }
        return {
            "ok": True,
            "url": normalized,
            "text": BeautifulSoup(html, "html.parser").get_text(" ", strip=True),
            "soup": BeautifulSoup(html, "html.parser"),
            "failure_reason": "",
        }

    recovered = recover_agent_hunt_listing_friction_email_record(record, fetch_page=fake_fetch)
    assessment = assess_agent_hunt_listing_blocked_record(recovered)

    assert recovered["agent_hunt_email_recovery_attempted"] is True
    assert recovered["agent_hunt_email_recovery_success"] is True
    assert recovered["email"] == "mary@maryroeauthor.com"
    assert recovered["agent_hunt_email_recovery_source_url"] == "https://maryroeauthor.com/contact"
    assert assessment["qualified"] is True
    assert assessment["status"] == "qualified_listing_blocked"


def test_recover_agent_hunt_listing_friction_email_record_without_visible_email_stays_caution() -> None:
    record = {
        "author_name": "Mary Roe",
        "candidate_url": "https://maryroeauthor.com/about",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "",
        "reject_reason": "listing_amazon_interstitial_bn_blocked",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
        "listing_snippet": "Available on Barnes & Noble",
    }
    pages = {
        "https://maryroeauthor.com/about": """
            <html><body>
              <a href="/contact">Contact Mary</a>
            </body></html>
        """,
        "https://maryroeauthor.com/contact": """
            <html><body>
              <a href="mailto:mary@maryroeauthor.com">Contact me</a>
            </body></html>
        """,
    }

    def fake_fetch(url: str) -> dict[str, object]:
        normalized = url.rstrip("/")
        html = pages.get(normalized)
        if html is None:
            return {
                "ok": False,
                "url": normalized,
                "text": "",
                "soup": None,
                "failure_reason": "404",
            }
        return {
            "ok": True,
            "url": normalized,
            "text": BeautifulSoup(html, "html.parser").get_text(" ", strip=True),
            "soup": BeautifulSoup(html, "html.parser"),
            "failure_reason": "",
        }

    recovered = recover_agent_hunt_listing_friction_email_record(record, fetch_page=fake_fetch)
    assessment = assess_agent_hunt_listing_blocked_record(recovered)

    assert recovered["agent_hunt_email_recovery_attempted"] is True
    assert recovered["agent_hunt_email_recovery_success"] is False
    assert recovered["agent_hunt_email_recovery_fail_reason"] == "no_visible_text_email"
    assert assessment["qualified"] is False
    assert assessment["status"] == "scoutworthy_not_outreach_ready"
    assert assessment["promotion_blocker_reason"] == "listing_friction_email_recovery_failed:no_visible_text_email"


def test_recover_agent_hunt_listing_friction_email_record_skips_obvious_junk() -> None:
    record = {
        "author_name": "- Inspiring Creative Writing -",
        "candidate_url": "https://tansielexington.com",
        "source_url": "https://www.epicindie.net/authordirectory",
        "source_type": "epic_directory",
        "source_query": "epic:author-directory",
        "email": "",
        "reject_reason": "listing_amazon_interstitial_bn_blocked",
        "listing_recovery_attempted": True,
        "next_action": "try_listing_proof",
    }

    def fake_fetch(url: str) -> dict[str, object]:
        raise AssertionError(f"unexpected fetch for {url}")

    recovered = recover_agent_hunt_listing_friction_email_record(record, fetch_page=fake_fetch)

    assert recovered["agent_hunt_email_recovery_attempted"] is False
    assert recovered["agent_hunt_email_recovery_skipped_reason"] == "not_listing_friction_caution"


def test_agent_hunt_rejects_junk_branding_author_name() -> None:
    row = {
        "AuthorName": "- Inspiring Creative Writing -",
        "AuthorWebsite": "https://tansielexington.com",
        "ContactPageURL": "https://tansielexington.com",
        "AuthorEmail": "tansie@tansielexington.com",
        "AuthorEmailSourceURL": "https://tansielexington.com",
        "EmailQuality": "same_domain",
        "SourceURL": "https://tansielexington.com",
        "Location": "",
    }

    assert not row_is_agent_hunt_qualified(row)
    assert not row_qualifies_for_master(row, policy="strict", validation_profile="agent_hunt")


def test_agent_hunt_profile_defaults_apply_goal_preset() -> None:
    args = Namespace(
        validation_profile="agent_hunt",
        goal_total=100,
        goal_final=0,
        max_runs=20,
        max_stale_runs=5,
        batch_min=10,
        batch_max=20,
        target=40,
        min_candidates=20,
        require_location_proof=False,
        us_only=False,
        listing_strict=False,
        merge_policy="strict",
    )

    apply_validation_profile_defaults(args)

    assert args.validation_profile == "agent_hunt"
    assert args.goal_final == 20


def test_email_only_profile_defaults_apply_lightweight_runtime_preset() -> None:
    args = Namespace(
        validation_profile="email_only",
        goal_total=100,
        goal_final=0,
        max_runs=20,
        max_stale_runs=5,
        batch_min=10,
        batch_max=20,
        target=40,
        min_candidates=20,
        max_fetches_per_domain=16,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        location_recovery_mode="same_domain",
        location_recovery_pages=6,
        require_location_proof=False,
        us_only=False,
        listing_strict=True,
        merge_policy="balanced",
    )

    apply_validation_profile_defaults(args)

    assert args.validation_profile == "email_only"
    assert args.listing_strict is False
    assert args.max_fetches_per_domain == 6
    assert args.max_seconds_per_domain == 8.0
    assert args.max_total_runtime == 90.0
    assert args.max_pages_for_title == 1
    assert args.max_pages_for_contact == 2
    assert args.max_total_fetches_per_domain_per_run == 6
    assert args.location_recovery_mode == "off"
    assert args.location_recovery_pages == 0
    assert args.merge_policy == "balanced"


def test_write_scouted_rows_outputs_three_columns_without_header(tmp_path) -> None:
    output_path = tmp_path / "scouted.csv"
    rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorEmail": "Jane@Example.com",
            "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
            "SourceURL": "https://janedoeauthor.com/about",
        }
    ]

    count = write_scouted_rows(output_path, rows)

    assert count == 1
    assert output_path.read_text(encoding="utf-8").strip() == (
        "Jane Doe,jane@example.com,https://janedoeauthor.com/contact"
    )


def test_build_agent_hunt_stats_reports_scout_progress_and_domains() -> None:
    scout_row = {
        "AuthorName": "Jane Doe",
        "AuthorWebsite": "https://janedoeauthor.com",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorEmail": "jane@janedoeauthor.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://janedoeauthor.com/about",
        "BookTitle": "",
        "BookTitleStatus": "missing_or_weak",
        "BookTitleMethod": "fallback",
        "Location": "",
        "IndieProofStrength": "directory",
        "ListingStatus": "missing",
        "RecencyStatus": "missing",
    }
    dead_row = {
        "AuthorName": "Dead Row",
        "SourceURL": "https://dead.example/about",
        "BookTitle": "Nope",
        "BookTitleStatus": "ok",
        "Location": "Dublin, Ireland",
        "AuthorEmail": "dead@example.com",
        "AuthorEmailSourceURL": "https://dead.example/about",
        "EmailQuality": "same_domain",
    }

    result = build_agent_hunt_stats(
        validated_rows=[scout_row, dead_row],
        validator_reject_reasons={"page_fetch_failed_404": 2},
        target=20,
    )

    assert result["scouted_progress"] == 1
    assert result["scouted_rows_written"] == 1
    assert result["strict_rows_written"] == 0
    assert result["routing_counts"] == {
        "qualified": 1,
        "scoutworthy_not_outreach_ready": 0,
        "rejected": 1,
    }
    assert result["outreach_tier_counts"] == {
        "tier_1_safest": 1,
        "tier_2_usable_with_caution": 0,
        "tier_3_weak_but_usable": 0,
    }
    assert result["outreach_decision_counts"]["KEEP"] == 1
    assert result["top_tier_1_reasons"] == {"tier_1_same_domain_clear_match": 1}
    assert result["scouted_rows_by_tier"]["tier_1_safest"][0]["AuthorName"] == "Jane Doe"
    assert result["top_reject_reasons"]["page_fetch_failed_404"] == 2
    assert result["top_reject_reasons"]["non_us_location"] == 1
    assert result["top_rejected_reasons"] == {"non_us_location": 1}
    assert result["scouted_source_domains"][0]["domain"] == "janedoeauthor.com"


def test_build_agent_hunt_stats_reports_scoutworthy_and_author_convergence() -> None:
    existing_master_rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoeauthor.com",
            "ContactPageURL": "https://janedoeauthor.com/contact",
        }
    ]
    validated_rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoe.substack.com",
            "ContactPageURL": "https://janedoe.substack.com/about",
            "AuthorEmail": "jane@janedoe.substack.com",
            "AuthorEmailSourceURL": "https://janedoe.substack.com/about",
            "EmailQuality": "same_domain",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "Location": "",
        },
        {
            "AuthorName": "Mary Roe",
            "AuthorWebsite": "https://maryroeauthor.com",
            "ContactPageURL": "https://maryroeauthor.com/contact",
            "AuthorEmail": "",
            "AuthorEmailSourceURL": "",
            "EmailQuality": "",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "Location": "",
        },
        {
            "AuthorName": "John Smith",
            "AuthorWebsite": "https://johnsmithwrites.com",
            "ContactPageURL": "https://johnsmithwrites.com/contact",
            "AuthorEmail": "john@johnsmithwrites.com",
            "AuthorEmailSourceURL": "https://johnsmithwrites.com/contact",
            "EmailQuality": "same_domain",
            "SourceURL": "https://www.iabx.org/author-directory",
            "Location": "",
        },
        {
            "AuthorName": "John Smith",
            "AuthorWebsite": "https://johnsmithauthor.com",
            "ContactPageURL": "https://johnsmithauthor.com/contact",
            "AuthorEmail": "john@johnsmithauthor.com",
            "AuthorEmailSourceURL": "https://johnsmithauthor.com/contact",
            "EmailQuality": "same_domain",
            "SourceURL": "https://www.iabx.org/author-directory",
            "Location": "",
        },
    ]
    candidate_outcome_records = [
        {
            "candidate_domain": "janedoe.substack.com",
            "candidate_url": "https://janedoe.substack.com",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
        },
        {
            "candidate_domain": "maryroeauthor.com",
            "candidate_url": "https://maryroeauthor.com",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
        },
        {
            "candidate_domain": "johnsmithwrites.com",
            "candidate_url": "https://johnsmithwrites.com",
            "source_type": "iabx_directory",
            "source_query": "iabx:author-directory",
            "source_url": "https://www.iabx.org/author-directory",
        },
        {
            "candidate_domain": "johnsmithauthor.com",
            "candidate_url": "https://johnsmithauthor.com",
            "source_type": "iabx_directory",
            "source_query": "iabx:author-directory",
            "source_url": "https://www.iabx.org/author-directory",
        },
        {
            "candidate_domain": "brand.example",
            "candidate_url": "https://brand.example/about",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "reject_reason": "enterprise_or_famous",
        },
    ]

    result = build_agent_hunt_stats(
        validated_rows=validated_rows,
        validator_reject_reasons={"enterprise_or_famous": 2},
        target=20,
        existing_master_rows=existing_master_rows,
        candidate_outcome_records=candidate_outcome_records,
    )

    assert result["routing_counts"] == {
        "qualified": 3,
        "scoutworthy_not_outreach_ready": 1,
        "rejected": 1,
    }
    assert result["outreach_tier_counts"] == {
        "tier_1_safest": 3,
        "tier_2_usable_with_caution": 0,
        "tier_3_weak_but_usable": 0,
    }
    assert result["scouted_rows_by_tier"]["tier_1_safest"][0]["AuthorName"] == "Jane Doe"
    assert result["top_caution_reasons"] == {"scoutworthy_missing_visible_email": 1}
    assert result["top_reasons_caution_fail_promotion_to_qualified"] == {"scoutworthy_missing_visible_email": 1}
    assert result["scoutworthy_not_outreach_ready_count"] == 1
    assert result["scoutworthy_not_outreach_ready_reasons"] == {"scoutworthy_missing_visible_email": 1}
    assert result["top_reject_reasons"]["enterprise_or_famous"] == 2
    assert result["caution_source_types"] == [{"type": "epic_directory", "count": 1}]
    assert result["caution_source_queries"] == [{"query": "epic:author-directory", "count": 1}]
    assert result["rejected_source_domains"] == [{"domain": "epicindie.net", "count": 1}]
    assert result["rejected_source_types"] == [{"type": "epic_directory", "count": 1}]
    assert result["rejected_source_queries"] == [{"query": "epic:author-directory", "count": 1}]
    convergence = result["author_convergence"]
    assert convergence["candidate_url_vs_author_novelty"]["novel_candidate_url_existing_author"] == 1
    assert convergence["novel_candidate_url_existing_author_count"] == 1
    assert convergence["top_fail_reasons_for_novel_candidate_urls"] == {
        "enterprise_or_famous": 1,
        "existing_author_identity": 1,
        "scoutworthy_missing_visible_email": 1,
        "duplicate_author_identity_in_batch": 1,
    }
    assert convergence["top_fail_reasons_for_novel_authors"] == {"scoutworthy_missing_visible_email": 1}
    assert convergence["top_converged_source_domains"] == [{"domain": "epicindie.net", "count": 1}]
    assert convergence["top_converged_source_types"] == [{"source": "epic_directory", "count": 1}]
    assert convergence["top_converged_source_queries"] == [{"query": "epic:author-directory", "count": 1}]
    assert convergence["repeated_author_identities"][0] == {
        "author_identity": "john smith",
        "count": 2,
        "already_known": False,
    }
    assert convergence["repeated_author_identities"][1] == {
        "author_identity": "jane doe",
        "count": 1,
        "already_known": True,
    }


def test_build_agent_hunt_stats_reports_listing_friction_and_retains_blocked_rows() -> None:
    validated_rows = []
    candidate_outcome_records = [
        {
            "candidate_domain": "janedoeauthor.com",
            "candidate_url": "https://janedoeauthor.com/about",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "author_name": "Jane Doe",
            "email": "jane@janedoeauthor.com",
            "reject_reason": "listing_amazon_interstitial_no_bn_candidate",
            "listing_recovery_attempted": True,
            "next_action": "try_listing_proof",
            "listing_snippet": "Available on Amazon",
        },
        {
            "candidate_domain": "maryroeauthor.com",
            "candidate_url": "https://maryroeauthor.com/about",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "author_name": "Mary Roe",
            "email": "",
            "reject_reason": "listing_amazon_interstitial_bn_blocked",
            "listing_recovery_attempted": True,
            "next_action": "try_listing_proof",
            "listing_snippet": "Available on Barnes & Noble",
        },
        {
            "candidate_domain": "harddeadend.com",
            "candidate_url": "https://harddeadend.com/about",
            "source_type": "iabx_directory",
            "source_query": "iabx:author-directory",
            "source_url": "https://www.iabx.org/author-directory",
            "author_name": "Hard Deadend",
            "email": "hard@harddeadend.com",
            "reject_reason": "listing_not_found",
        },
    ]

    result = build_agent_hunt_stats(
        validated_rows=validated_rows,
        validator_reject_reasons={
            "listing_amazon_interstitial_no_bn_candidate": 1,
            "listing_amazon_interstitial_bn_blocked": 1,
            "listing_not_found": 1,
        },
        target=20,
        existing_master_rows=[],
        candidate_outcome_records=candidate_outcome_records,
    )

    assert result["scouted_rows_written"] == 1
    assert result["scouted_rows"][0]["AuthorName"] == "Jane Doe"
    assert result["scouted_rows"][0]["SourceURL"] == "https://www.epicindie.net/authordirectory"
    assert result["routing_counts"] == {
        "qualified": 1,
        "scoutworthy_not_outreach_ready": 1,
        "rejected": 1,
    }
    assert result["outreach_tier_counts"]["tier_1_safest"] == 1
    assert result["top_reject_reasons"] == {"listing_not_found": 1}
    assert result["top_rejected_reasons"] == {"listing_not_found": 1}
    assert result["top_caution_reasons"] == {
        "listing_blocked:listing_amazon_interstitial_bn_blocked": 1
    }
    assert result["top_reasons_caution_fail_promotion_to_qualified"] == {
        "listing_blocked:listing_amazon_interstitial_bn_blocked": 1
    }
    assert result["scoutworthy_not_outreach_ready_count"] == 1
    assert result["listing_blocked_caution_rows"][0]["AuthorName"] == "Mary Roe"
    listing_friction = result["listing_friction"]
    assert listing_friction["qualified_listing_blocked_count"] == 1
    assert listing_friction["qualified_listing_blocked_reasons"] == {
        "listing_amazon_interstitial_no_bn_candidate": 1
    }
    assert listing_friction["scoutworthy_listing_blocked_count"] == 1
    assert listing_friction["scoutworthy_listing_blocked_reasons"] == {
        "listing_amazon_interstitial_bn_blocked": 1
    }
    assert listing_friction["top_listing_failure_reasons"] == {
        "listing_amazon_interstitial_no_bn_candidate": 1,
        "listing_amazon_interstitial_bn_blocked": 1,
        "listing_not_found": 1,
    }
    assert listing_friction["scoutworthy_listing_blocked_source_queries"] == [
        {"query": "epic:author-directory", "count": 1}
    ]
    assert listing_friction["top_listing_dead_end_source_queries"] == [
        {"query": "epic:author-directory", "count": 2},
        {"query": "iabx:author-directory", "count": 1},
    ]


def test_build_agent_hunt_stats_reports_listing_friction_email_recovery_stats() -> None:
    candidate_outcome_records = [
        {
            "candidate_domain": "maryroeauthor.com",
            "candidate_url": "https://maryroeauthor.com/about",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "author_name": "Mary Roe",
            "email": "mary@maryroeauthor.com",
            "reject_reason": "listing_amazon_interstitial_no_bn_candidate",
            "listing_recovery_attempted": True,
            "next_action": "try_listing_proof",
            "listing_snippet": "Available on Amazon",
            "agent_hunt_email_recovery_attempted": True,
            "agent_hunt_email_recovery_success": True,
            "agent_hunt_email_recovery_source_url": "https://maryroeauthor.com/contact",
            "agent_hunt_email_recovered_email": "mary@maryroeauthor.com",
            "agent_hunt_email_recovered_email_quality": "same_domain",
            "agent_hunt_email_recovery_proof_snippet": "email mary@maryroeauthor.com",
        },
        {
            "candidate_domain": "janeroeauthor.com",
            "candidate_url": "https://janeroeauthor.com/about",
            "source_type": "iabx_directory",
            "source_query": "iabx:author-directory",
            "source_url": "https://www.iabx.org/author-directory",
            "author_name": "Jane Roe",
            "email": "",
            "reject_reason": "listing_amazon_interstitial_bn_blocked",
            "listing_recovery_attempted": True,
            "next_action": "try_listing_proof",
            "listing_snippet": "Available on Barnes & Noble",
            "agent_hunt_email_recovery_attempted": True,
            "agent_hunt_email_recovery_success": False,
            "agent_hunt_email_recovery_fail_reason": "no_visible_email_found",
        },
    ]

    result = build_agent_hunt_stats(
        validated_rows=[],
        validator_reject_reasons={
            "listing_amazon_interstitial_no_bn_candidate": 1,
            "listing_amazon_interstitial_bn_blocked": 1,
        },
        target=20,
        existing_master_rows=[],
        candidate_outcome_records=candidate_outcome_records,
    )

    assert result["scouted_rows_written"] == 1
    assert result["scouted_rows"][0]["AuthorName"] == "Mary Roe"
    assert result["scouted_rows"][0]["SourceURL"] == "https://www.epicindie.net/authordirectory"
    assert result["scouted_rows"][0]["AuthorEmail"] == "mary@maryroeauthor.com"
    assert result["outreach_tier_counts"]["tier_1_safest"] == 1
    assert result["listing_friction_email_recovery_attempted_count"] == 2
    assert result["listing_friction_email_recovery_success_count"] == 1
    assert result["listing_friction_email_recovery_fail_count"] == 1
    assert result["top_listing_friction_email_recovery_fail_reasons"] == {"no_visible_email_found": 1}
    assert result["listing_friction_email_recovered_source_queries"] == [
        {"query": "epic:author-directory", "count": 1}
    ]
    assert result["listing_friction_email_recovery_failed_source_queries"] == [
        {"query": "iabx:author-directory", "count": 1}
    ]
    assert result["top_reasons_caution_fail_promotion_to_qualified"] == {
        "listing_friction_email_recovery_failed:no_visible_email_found": 1
    }


def test_build_agent_hunt_listing_feedback_reports_listing_friction_concentration() -> None:
    records = [
        {
            "candidate_url": f"https://candidate{i}.example/about",
            "source_url": "https://www.epicindie.net/authordirectory",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "reject_reason": "listing_amazon_interstitial_no_bn_candidate",
        }
        for i in range(3)
    ] + [
        {
            "candidate_url": "https://ok.example/about",
            "source_url": "https://www.epicindie.net/authordirectory",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "reject_reason": "enterprise_or_famous",
        }
    ]

    result = build_agent_hunt_listing_feedback(records)

    assert result["query_penalties"] == {"epic:author-directory": 6}
    assert result["source_type_penalties"] == {"epic_directory": 6}
    assert result["source_domain_penalties"] == {"epicindie.net": 6}
    assert result["penalized_queries"] == [{"query": "epic:author-directory", "penalty": 6}]
    assert result["penalized_source_types"] == [{"source": "epic_directory", "penalty": 6}]
    assert result["penalized_source_domains"] == [{"domain": "epicindie.net", "penalty": 6}]


def test_score_candidate_intake_applies_agent_hunt_listing_feedback_penalty() -> None:
    row = {
        "CandidateURL": "https://janedoeauthor.com/about",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe, indie author",
        "SourceSnippet": "Author website contact jane@janedoeauthor.com",
    }

    baseline = score_candidate_intake(row, validation_profile="agent_hunt")
    penalized = score_candidate_intake(
        row,
        validation_profile="agent_hunt",
        agent_hunt_listing_feedback={
            "query_penalties": {"epic:author-directory": 4},
            "source_type_penalties": {"epic_directory": 2},
            "source_domain_penalties": {"epicindie.net": 2},
        },
    )

    assert penalized["score"] < baseline["score"]
    assert penalized["components"]["prior_listing_friction_penalty"] == -8


def test_build_agent_hunt_source_quality_feedback_reports_epic_directory_junk() -> None:
    records = [
        {
            "candidate_url": f"https://candidate{i}.example/about",
            "source_url": "https://www.epicindie.net/authordirectory",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "reject_reason": "bad_author_name_generic_phrase",
        }
        for i in range(2)
    ] + [
        {
            "candidate_url": "https://candidate3.example/about",
            "source_url": "https://www.epicindie.net/authordirectory",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "reject_reason": "enterprise_or_famous",
        }
    ]

    result = build_agent_hunt_source_quality_feedback(records)

    assert result["query_penalties"] == {"epic:author-directory": 8}
    assert result["source_type_penalties"] == {"epic_directory": 8}
    assert result["source_domain_penalties"] == {"epicindie.net": 8}
    assert result["top_source_quality_failure_reasons"] == {
        "bad_author_name": 2,
        "enterprise_or_famous": 1,
    }


def test_score_candidate_intake_applies_agent_hunt_source_quality_feedback_penalty() -> None:
    row = {
        "CandidateURL": "https://janedoeauthor.com/about",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. jane@janedoeauthor.com",
    }

    baseline = score_candidate_intake(row, validation_profile="agent_hunt")
    penalized = score_candidate_intake(
        row,
        validation_profile="agent_hunt",
        agent_hunt_source_quality_feedback={
            "query_penalties": {"epic:author-directory": 4},
            "source_type_penalties": {"epic_directory": 2},
            "source_domain_penalties": {"epicindie.net": 2},
        },
    )

    assert penalized["score"] < baseline["score"]
    assert penalized["components"]["prior_source_quality_penalty"] == -8


def test_suppress_agent_hunt_source_quality_candidates_rejects_obvious_epic_junk() -> None:
    junk_row = {
        "CandidateURL": "https://creativewriting.example/",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Inspiring Creative Writing",
        "SourceSnippet": "Official website for creative writing books and new releases.",
    }
    kept_row = {
        "CandidateURL": "https://janedoeauthor.com/",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. jane@janedoeauthor.com",
    }

    ordered_rows, scoring = order_candidates_for_strict_validation(
        [junk_row, kept_row],
        validation_profile="agent_hunt",
    )
    kept_rows, stats = suppress_agent_hunt_source_quality_candidates(
        ordered_rows,
        scoring_context=scoring,
    )

    assert kept_rows == [kept_row]
    assert stats["suppressed_by_source_quality_count"] == 1
    assert stats["top_source_quality_suppression_reasons"] == {
        "bad_author_name_generic_source_title": 1
    }
    assert stats["source_quality_suppressed_source_domains"] == [{"domain": "epicindie.net", "count": 1}]


def test_suppress_agent_hunt_source_quality_candidates_preserves_plausible_epic_row() -> None:
    row = {
        "CandidateURL": "https://janedoeauthor.com/",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. Contact jane@janedoeauthor.com",
    }

    ordered_rows, scoring = order_candidates_for_strict_validation(
        [row],
        validation_profile="agent_hunt",
        agent_hunt_source_quality_feedback={
            "query_penalties": {"epic:author-directory": 4},
            "source_type_penalties": {"epic_directory": 2},
            "source_domain_penalties": {"epicindie.net": 2},
        },
    )
    kept_rows, stats = suppress_agent_hunt_source_quality_candidates(
        ordered_rows,
        scoring_context=scoring,
    )

    assert kept_rows == [row]
    assert kept_rows[0]["SourceURL"] == "https://www.epicindie.net/authordirectory"
    assert stats["suppressed_by_source_quality_count"] == 0
    assert stats["source_quality_penalty_before_after_source_types"] == [
        {"source": "epic_directory", "before": 1, "after": 1, "suppressed": 0}
    ]


def test_throttle_agent_hunt_epic_directory_candidates_limits_batch_share() -> None:
    epic_rows = [
        {
            "CandidateURL": f"https://epic{i}.example/",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "SourceTitle": "Jane Doe",
            "SourceSnippet": f"Jane Doe indie fantasy author based in Ohio. jane{i}@example.com",
        }
        for i in range(26)
    ]
    ian_rows = [
        {
            "CandidateURL": f"https://ian{i}.example/",
            "SourceType": "ian_directory",
            "SourceQuery": "independent-author-network:author-directory",
            "SourceURL": "https://independentauthornetwork.com/authors",
            "SourceTitle": "Mary Roe",
            "SourceSnippet": f"Mary Roe author based in Ohio. mary{i}@example.com",
        }
        for i in range(2)
    ]
    ordered_rows, scoring = order_candidates_for_strict_validation(
        epic_rows + ian_rows,
        validation_profile="agent_hunt",
    )

    kept_rows, stats = throttle_agent_hunt_epic_directory_candidates(
        ordered_rows,
        scoring_context=scoring,
    )

    kept_epic = [row for row in kept_rows if row["SourceType"] == "epic_directory"]
    kept_ian = [row for row in kept_rows if row["SourceType"] == "ian_directory"]

    assert len(kept_epic) == 24
    assert len(kept_ian) == 2
    assert stats["epic_directory_throttled_count"] == 2
    assert stats["epic_directory_before_count"] == 26
    assert stats["epic_directory_after_count"] == 24
    assert stats["epic_directory_throttled_source_domains"] == [{"domain": "epicindie.net", "count": 2}]
    assert stats["epic_directory_throttled_source_queries"] == [{"query": "epic:author-directory", "count": 2}]


def test_summarize_candidate_intake_scores_reports_listing_feedback_penalized_candidates() -> None:
    row = {
        "CandidateURL": "https://janedoeauthor.com/about",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe, indie author",
        "SourceSnippet": "Author website contact jane@janedoeauthor.com",
    }
    scored = score_candidate_intake(
        row,
        validation_profile="agent_hunt",
        agent_hunt_listing_feedback={
            "query_penalties": {"epic:author-directory": 6},
            "source_type_penalties": {},
            "source_domain_penalties": {},
        },
    )

    result = summarize_candidate_intake_scores(
        ordered_candidate_rows=[row],
        scoring_context={
            "enabled": True,
            "distribution": {str(scored["band"]): 1},
            "avg_score": float(scored["score"]),
            "max_score": int(scored["score"]),
            "min_score": int(scored["score"]),
            "high_score_threshold": 40,
            "low_score_threshold": 15,
            "top_candidates": [],
            "score_map": {str(scored["identity"]): scored},
            "score_map_by_url": {str(scored["candidate_url"]): scored},
        },
        candidate_outcome_records=[],
        orchestration_stats={"processed_candidates": 1, "slice_summaries": []},
    )

    assert result["listing_feedback_penalized_candidates_total"] == 1
    assert result["listing_feedback_penalized_source_types"] == [{"source": "epic_directory", "count": 1}]
    assert result["listing_feedback_penalized_source_queries"] == [{"query": "epic:author-directory", "count": 1}]
    assert result["listing_feedback_penalized_source_domains"] == [{"domain": "epicindie.net", "count": 1}]


def test_summarize_candidate_intake_scores_reports_source_quality_penalized_candidates() -> None:
    row = {
        "CandidateURL": "https://janedoeauthor.com/about",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. jane@janedoeauthor.com",
    }
    scored = score_candidate_intake(
        row,
        validation_profile="agent_hunt",
        agent_hunt_source_quality_feedback={
            "query_penalties": {"epic:author-directory": 6},
            "source_type_penalties": {},
            "source_domain_penalties": {},
        },
    )

    result = summarize_candidate_intake_scores(
        ordered_candidate_rows=[row],
        scoring_context={
            "enabled": True,
            "distribution": {str(scored["band"]): 1},
            "avg_score": float(scored["score"]),
            "max_score": int(scored["score"]),
            "min_score": int(scored["score"]),
            "high_score_threshold": 40,
            "low_score_threshold": 15,
            "top_candidates": [],
            "score_map": {str(scored["identity"]): scored},
            "score_map_by_url": {str(scored["candidate_url"]): scored},
        },
        candidate_outcome_records=[],
        orchestration_stats={"processed_candidates": 1, "slice_summaries": []},
    )

    assert result["source_quality_feedback_penalized_candidates_total"] == 1
    assert result["source_quality_feedback_penalized_source_types"] == [{"source": "epic_directory", "count": 1}]
    assert result["source_quality_feedback_penalized_source_queries"] == [{"query": "epic:author-directory", "count": 1}]
    assert result["source_quality_feedback_penalized_source_domains"] == [{"domain": "epicindie.net", "count": 1}]


def test_build_rotating_queries_expands_safely_after_stale_runs() -> None:
    fresh_queries = build_rotating_queries(1, stale_runs=0)
    stale_queries = build_rotating_queries(2, stale_runs=2)

    assert len(stale_queries) >= len(fresh_queries)
    assert fresh_queries != stale_queries


def test_build_email_only_source_yield_stats_reports_kept_rows_and_email_hit_rates() -> None:
    result = build_email_only_source_yield_stats(
        [
            {
                "source_url": "https://www.epicindie.net/authordirectory",
                "source_type": "epic_directory",
                "source_query": "epic:author-directory",
                "email": "jane@example.com",
                "kept": True,
            },
            {
                "source_url": "https://www.epicindie.net/authordirectory",
                "source_type": "epic_directory",
                "source_query": "epic:author-directory",
                "email": "",
                "kept": False,
            },
            {
                "source_url": "https://www.google.com/search?q=author",
                "source_type": "google_cse",
                "source_query": "\"indie author\" \"official website\"",
                "email": "author@example.com",
                "kept": True,
            },
        ]
    )

    assert result["processed_candidates"] == 3
    assert result["kept_rows_by_source_domains"] == [
        {"domain": "epicindie.net", "count": 1},
        {"domain": "google.com", "count": 1},
    ]
    assert result["kept_rows_by_source_types"] == [
        {"source": "epic_directory", "count": 1},
        {"source": "google_cse", "count": 1},
    ]
    assert result["kept_rows_by_source_queries"] == [
        {"query": "epic:author-directory", "count": 1},
        {"query": "\"indie author\" \"official website\"", "count": 1},
    ]
    assert result["visible_email_hit_rate_by_source_domains"][0] == {
        "domain": "epicindie.net",
        "processed": 2,
        "visible_email_hits": 1,
        "visible_email_hit_rate": 0.5,
    }
    assert result["visible_email_hit_rate_by_source_types"][1] == {
        "source": "google_cse",
        "processed": 1,
        "visible_email_hits": 1,
        "visible_email_hit_rate": 1.0,
    }


def test_rotate_candidate_groups_rotates_directory_priority() -> None:
    groups = [
        [{"CandidateURL": "https://epic.example"}],
        [{"CandidateURL": "https://ian.example"}],
        [{"CandidateURL": "https://iabx.example"}],
    ]

    rotated = rotate_candidate_groups(groups, 1)

    assert [group[0]["CandidateURL"] for group in rotated] == [
        "https://ian.example",
        "https://iabx.example",
        "https://epic.example",
    ]


def test_astra_outbound_profile_defaults_apply_strict_run_preset() -> None:
    args = Namespace(
        validation_profile="astra_outbound",
        goal_total=100,
        goal_final=0,
        max_runs=20,
        max_stale_runs=5,
        batch_min=10,
        batch_max=20,
        target=40,
        min_candidates=20,
        require_location_proof=False,
        us_only=False,
        listing_strict=False,
        merge_policy="balanced",
    )

    apply_validation_profile_defaults(args)

    assert args.validation_profile == "astra_outbound"
    assert args.goal_final == 20
    assert args.max_runs == 50
    assert args.max_stale_runs == 50
    assert args.batch_min == 20
    assert args.batch_max == 40
    assert args.target == 80
    assert args.min_candidates == 80
    assert args.require_location_proof is True
    assert args.us_only is True
    assert args.listing_strict is True
    assert args.merge_policy == "strict"


def test_strict_interactive_profile_defaults_apply_runtime_preset() -> None:
    args = Namespace(
        validation_profile="strict_interactive",
        goal_total=100,
        goal_final=0,
        max_runs=20,
        max_stale_runs=5,
        batch_min=10,
        batch_max=20,
        target=40,
        min_candidates=20,
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        require_location_proof=False,
        us_only=False,
        listing_strict=False,
        merge_policy="balanced",
    )

    apply_validation_profile_defaults(args)

    assert args.validation_profile == "strict_interactive"
    assert args.max_fetches_per_domain == 10
    assert args.max_seconds_per_domain == 12.0
    assert args.max_total_runtime == 120.0
    assert args.target == 40
    assert args.merge_policy == "balanced"


def test_strict_full_profile_defaults_apply_runtime_preset() -> None:
    args = Namespace(
        validation_profile="strict_full",
        goal_total=100,
        goal_final=0,
        max_runs=20,
        max_stale_runs=5,
        batch_min=10,
        batch_max=20,
        target=40,
        min_candidates=20,
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        require_location_proof=False,
        us_only=False,
        listing_strict=False,
        merge_policy="balanced",
    )

    apply_validation_profile_defaults(args)

    assert args.validation_profile == "strict_full"
    assert args.max_fetches_per_domain == 16
    assert args.max_seconds_per_domain == 25.0
    assert args.max_total_runtime == 900.0
    assert args.target == 40
    assert args.merge_policy == "balanced"


def test_validator_and_single_run_runtime_profile_defaults_match() -> None:
    validator_args = Namespace(
        validation_profile="strict_interactive",
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
    )
    single_run_args = Namespace(
        validation_profile="strict_interactive",
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        target=40,
        min_candidates=80,
        require_location_proof=False,
        us_only=False,
        listing_strict=False,
        min_final=20,
        max_final=40,
    )

    apply_validator_profile_defaults(validator_args)
    apply_single_run_profile_defaults(single_run_args)

    assert validator_args.validation_profile == "strict_interactive"
    assert validator_args.max_fetches_per_domain == 10
    assert validator_args.max_seconds_per_domain == 12.0
    assert validator_args.max_total_runtime == 120.0
    assert single_run_args.max_fetches_per_domain == 10
    assert single_run_args.max_seconds_per_domain == 12.0
    assert single_run_args.max_total_runtime == 120.0


def test_email_only_validator_and_single_run_runtime_profile_defaults_match() -> None:
    validator_args = Namespace(
        validation_profile="email_only",
        max_fetches_per_domain=16,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        location_recovery_mode="same_domain",
        location_recovery_pages=6,
        listing_strict=True,
    )
    single_run_args = Namespace(
        validation_profile="email_only",
        max_fetches_per_domain=16,
        max_seconds_per_domain=25.0,
        max_total_runtime=900.0,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        location_recovery_mode="same_domain",
        location_recovery_pages=6,
        listing_strict=True,
        target=40,
        min_candidates=80,
        require_location_proof=False,
        us_only=False,
        min_final=20,
        max_final=40,
    )

    apply_validator_profile_defaults(validator_args)
    apply_single_run_profile_defaults(single_run_args)

    assert validator_args.validation_profile == "email_only"
    assert validator_args.listing_strict is False
    assert validator_args.max_fetches_per_domain == 6
    assert validator_args.max_seconds_per_domain == 8.0
    assert validator_args.max_total_runtime == 90.0
    assert validator_args.max_pages_for_title == 1
    assert validator_args.max_pages_for_contact == 2
    assert validator_args.max_total_fetches_per_domain_per_run == 6
    assert validator_args.location_recovery_mode == "off"
    assert validator_args.location_recovery_pages == 0

    assert single_run_args.listing_strict is False
    assert single_run_args.max_fetches_per_domain == 6
    assert single_run_args.max_seconds_per_domain == 8.0
    assert single_run_args.max_total_runtime == 90.0
    assert single_run_args.max_pages_for_title == 1
    assert single_run_args.max_pages_for_contact == 2
    assert single_run_args.max_total_fetches_per_domain_per_run == 6
    assert single_run_args.location_recovery_mode == "off"
    assert single_run_args.location_recovery_pages == 0


def test_split_rows_by_merge_policy_keeps_staged_rows_in_queue() -> None:
    rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorWebsite": "https://janedoeauthor.com",
            "ContactPageURL": "https://janedoeauthor.com/contact",
            "AuthorEmail": "jane@janedoeauthor.com",
            "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
            "EmailQuality": "same_domain",
            "RecencyStatus": "verified",
        },
        {
            "AuthorName": "John Doe",
            "AuthorWebsite": "https://johndoeauthor.com",
            "ContactPageURL": "https://johndoeauthor.com/contact",
            "AuthorEmail": "",
            "RecencyStatus": "missing",
        },
    ]

    master_rows, queue_rows = split_rows_by_merge_policy(rows, policy="strict")

    assert [row["AuthorName"] for row in master_rows] == ["Jane Doe"]
    assert [row["AuthorName"] for row in queue_rows] == ["John Doe"]


def test_write_verified_rows_outputs_three_columns_without_header(tmp_path) -> None:
    output_path = tmp_path / "verified.csv"
    rows = [
        {
            "AuthorName": "Jane Doe",
            "AuthorEmail": "Jane@Example.com",
            "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        }
    ]

    count = write_verified_rows(output_path, rows)

    assert count == 1
    assert output_path.read_text(encoding="utf-8").strip() == (
        "Jane Doe,jane@example.com,https://janedoeauthor.com/contact"
    )


def test_orchestrate_candidate_replacements_replaces_failed_rows_until_target() -> None:
    candidate_rows = [{"CandidateURL": f"https://author{i}.example"} for i in range(1, 6)]
    seen: list[list[str]] = []

    def fake_validate_slice(slice_rows, slice_index, remaining_runtime):  # noqa: ANN001
        seen.append([row["CandidateURL"] for row in slice_rows])
        if slice_index == 1:
            return {
                "validated_rows": [{"AuthorName": "A One", "AuthorEmail": "a1@example.com", "ListingURL": "https://amazon.com/dp/1"}],
                "stats": {
                    "total_candidates": 2,
                    "kept_rows": 1,
                    "reject_reasons": {"bad_author_name": 1},
                    "candidate_state_counts": {"verified": 1, "dead_end": 1},
                    "planned_action_counts": {"mark_verified": 1, "stop_dead_end": 1},
                    "stage_timings": {"prefilter_seconds": 1.0, "verify_seconds": 2.0},
                    "batch_runtime_exceeded": False,
                },
            }
        return {
            "validated_rows": [{"AuthorName": "A Two", "AuthorEmail": "a2@example.com", "ListingURL": "https://amazon.com/dp/2"}],
            "stats": {
                "total_candidates": 1,
                "kept_rows": 1,
                "reject_reasons": {},
                "candidate_state_counts": {"verified": 1},
                "planned_action_counts": {"mark_verified": 1},
                "stage_timings": {"prefilter_seconds": 0.5, "verify_seconds": 1.0},
                "batch_runtime_exceeded": False,
            },
        }

    result = orchestrate_candidate_replacements(
        candidate_rows,
        target_verified=2,
        max_candidates_per_slice=2,
        max_total_runtime=120.0,
        validate_slice=fake_validate_slice,
    )

    assert seen == [
        ["https://author1.example", "https://author2.example"],
        ["https://author3.example"],
    ]
    assert result["verified_progress"] == 2
    assert result["verified_target"] == 2
    assert result["candidates_replaced"] == 1
    assert result["dead_end_count"] == 1
    assert result["exhausted_before_target"] is False


def test_orchestrate_candidate_replacements_stops_once_target_is_reached() -> None:
    candidate_rows = [{"CandidateURL": f"https://author{i}.example"} for i in range(1, 5)]
    seen: list[list[str]] = []

    def fake_validate_slice(slice_rows, slice_index, remaining_runtime):  # noqa: ANN001
        seen.append([row["CandidateURL"] for row in slice_rows])
        return {
            "validated_rows": [{"AuthorName": "A One", "AuthorEmail": "a1@example.com", "ListingURL": "https://amazon.com/dp/1"}],
            "stats": {
                "total_candidates": 1,
                "kept_rows": 1,
                "reject_reasons": {},
                "candidate_state_counts": {"verified": 1},
                "planned_action_counts": {"mark_verified": 1},
                "stage_timings": {"prefilter_seconds": 0.1, "verify_seconds": 0.2},
                "batch_runtime_exceeded": False,
            },
        }

    result = orchestrate_candidate_replacements(
        candidate_rows,
        target_verified=1,
        max_candidates_per_slice=3,
        max_total_runtime=120.0,
        validate_slice=fake_validate_slice,
    )

    assert seen == [["https://author1.example"]]
    assert result["processed_candidates"] == 1
    assert result["verified_progress"] == 1


def test_orchestrate_candidate_replacements_stops_when_pool_is_exhausted() -> None:
    candidate_rows = [{"CandidateURL": f"https://author{i}.example"} for i in range(1, 4)]

    def fake_validate_slice(slice_rows, slice_index, remaining_runtime):  # noqa: ANN001
        return {
            "validated_rows": [],
            "stats": {
                "total_candidates": len(slice_rows),
                "kept_rows": 0,
                "reject_reasons": {"no_visible_author_email": len(slice_rows)},
                "candidate_state_counts": {"needs_email_proof": len(slice_rows)},
                "planned_action_counts": {"try_email_proof": len(slice_rows)},
                "stage_timings": {"prefilter_seconds": 0.5, "verify_seconds": 0.5},
                "batch_runtime_exceeded": False,
            },
        }

    result = orchestrate_candidate_replacements(
        candidate_rows,
        target_verified=2,
        max_candidates_per_slice=2,
        max_total_runtime=120.0,
        validate_slice=fake_validate_slice,
    )

    assert result["verified_progress"] == 0
    assert result["processed_candidates"] == 3
    assert result["exhausted_before_target"] is True


def test_orchestrate_candidate_replacements_stops_when_runtime_budget_is_exhausted() -> None:
    candidate_rows = [{"CandidateURL": f"https://author{i}.example"} for i in range(1, 5)]

    def fake_validate_slice(slice_rows, slice_index, remaining_runtime):  # noqa: ANN001
        return {
            "validated_rows": [],
            "stats": {
                "total_candidates": len(slice_rows),
                "kept_rows": 0,
                "reject_reasons": {"batch_runtime_exceeded": 1},
                "candidate_state_counts": {"needs_listing_proof": len(slice_rows)},
                "planned_action_counts": {"try_listing_proof": len(slice_rows)},
                "stage_timings": {"prefilter_seconds": 1.0, "verify_seconds": 1.5},
                "batch_runtime_exceeded": True,
            },
        }

    result = orchestrate_candidate_replacements(
        candidate_rows,
        target_verified=3,
        max_candidates_per_slice=2,
        max_total_runtime=2.0,
        validate_slice=fake_validate_slice,
    )

    assert result["runtime_exhausted"] is True


def test_orchestrate_candidate_replacements_uses_agent_hunt_progress_gate() -> None:
    candidate_rows = [{"CandidateURL": f"https://author{i}.example"} for i in range(1, 5)]
    seen: list[list[str]] = []

    scout_row = {
        "AuthorName": "Scout Row",
        "AuthorWebsite": "https://scout.example",
        "ContactPageURL": "https://scout.example/contact",
        "AuthorEmail": "scout@example.com",
        "AuthorEmailSourceURL": "https://scout.example/contact",
        "EmailQuality": "same_domain",
        "SourceURL": "https://scout.example/contact",
        "BookTitle": "Skyfall",
        "BookTitleStatus": "ok",
        "BookTitleMethod": "source_title",
        "Location": "",
    }
    staged_row = {
        "AuthorName": "Stage Only",
        "AuthorWebsite": "https://stage.example",
        "ContactPageURL": "https://stage.example/contact",
        "AuthorEmail": "",
        "SourceURL": "https://stage.example/contact",
        "BookTitle": "Weak",
        "BookTitleStatus": "ok",
        "Location": "",
    }

    def fake_validate_slice(slice_rows, slice_index, remaining_runtime):  # noqa: ANN001
        seen.append([row["CandidateURL"] for row in slice_rows])
        if slice_index == 1:
            return {
                "validated_rows": [staged_row],
                "stats": {
                    "total_candidates": 1,
                    "kept_rows": 1,
                    "reject_reasons": {},
                    "candidate_state_counts": {"needs_email_proof": 1},
                    "planned_action_counts": {"try_email_proof": 1},
                    "stage_timings": {"prefilter_seconds": 0.1, "verify_seconds": 0.2},
                    "batch_runtime_exceeded": False,
                },
            }
        return {
            "validated_rows": [scout_row],
            "stats": {
                "total_candidates": 1,
                "kept_rows": 1,
                "reject_reasons": {},
                "candidate_state_counts": {"verified": 1},
                "planned_action_counts": {"mark_verified": 1},
                "stage_timings": {"prefilter_seconds": 0.1, "verify_seconds": 0.2},
                "batch_runtime_exceeded": False,
            },
        }

    result = orchestrate_candidate_replacements(
        candidate_rows,
        target_verified=1,
        max_candidates_per_slice=1,
        max_total_runtime=120.0,
        validate_slice=fake_validate_slice,
        qualifies_for_progress=row_is_agent_hunt_qualified,
    )

    assert seen == [["https://author1.example"], ["https://author2.example"]]
    assert result["verified_progress"] == 1
    assert result["processed_candidates"] == 2


def test_score_candidate_intake_prefers_strong_visible_signals() -> None:
    high_row = {
        "CandidateURL": "https://janedoeauthor.com/",
        "SourceType": "epic_directory",
        "SourceQuery": "epic:author-directory",
        "SourceURL": "https://www.epicindie.net/authordirectory",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": (
            "Jane Doe indie fantasy author based in Ohio. "
            "Author of Dragonfall Saga. Contact jane@janedoeauthor.com for media."
        ),
    }
    low_row = {
        "CandidateURL": "https://example.co.uk/store",
        "SourceType": "iabx_directory",
        "SourceQuery": "iabx:author-directory",
        "SourceURL": "https://iabx.example/authors",
        "SourceTitle": "Books & Co",
        "SourceSnippet": "Author Directory | Independent Authors Book Experience author directory listing for Books & Co",
    }

    high = score_candidate_intake(high_row, validation_profile="strict_full")
    low = score_candidate_intake(low_row, validation_profile="strict_full")

    assert int(high["score"]) > int(low["score"])
    assert high["components"]["visible_personal_email_hint"] > 0
    assert high["components"]["author_us_location_hint"] > 0
    assert low["components"]["non_us_hint"] < 0
    assert high["top_positive_features"]
    assert low["top_negative_features"]


def test_order_candidates_for_strict_validation_prioritizes_higher_scores() -> None:
    candidate_rows = [
        {
            "CandidateURL": "https://low.example.co.uk/store",
            "SourceType": "iabx_directory",
            "SourceQuery": "iabx:author-directory",
            "SourceURL": "https://iabx.example/authors",
            "SourceTitle": "Books & Co",
            "SourceSnippet": "Author Directory | Independent Authors Book Experience author directory listing for Books & Co in England",
        },
        {
            "CandidateURL": "https://high.example/",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "SourceTitle": "Jane Doe",
            "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. Author of Dragonfall Saga. jane@high.example",
        },
    ]

    ordered_rows, scoring = order_candidates_for_strict_validation(candidate_rows, validation_profile="strict_full")

    assert scoring["enabled"] is True
    assert ordered_rows[0]["CandidateURL"] == "https://high.example/"
    assert scoring["top_candidates"][0]["candidate_url"] == "https://high.example/"


def test_summarize_candidate_intake_scores_reports_outcomes_and_low_score_delay() -> None:
    candidate_rows = [
        {
            "CandidateURL": "https://high.example/",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "SourceTitle": "Jane Doe",
            "SourceSnippet": "Jane Doe indie fantasy author based in Ohio. Author of Dragonfall Saga. jane@high.example",
        },
        {
            "CandidateURL": "https://mid.example/",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "SourceTitle": "John Roe",
            "SourceSnippet": "John Roe indie author of Stormlight Hollow.",
        },
        {
            "CandidateURL": "https://low.example.co.uk/store",
            "SourceType": "iabx_directory",
            "SourceQuery": "iabx:author-directory",
            "SourceURL": "https://iabx.example/authors",
            "SourceTitle": "Books & Co",
            "SourceSnippet": "Author Directory | Independent Authors Book Experience author directory listing for Books & Co in England",
        },
    ]
    ordered_rows, scoring = order_candidates_for_strict_validation(candidate_rows, validation_profile="strict_full")
    outcome_records = [
        {
            "candidate_url": "https://high.example/",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "current_state": "verified",
            "reject_reason": "kept",
            "kept": True,
        },
        {
            "candidate_url": "https://mid.example/",
            "source_type": "epic_directory",
            "source_query": "epic:author-directory",
            "source_url": "https://www.epicindie.net/authordirectory",
            "current_state": "dead_end",
            "reject_reason": "non_us_location",
            "kept": False,
        },
    ]
    summary = summarize_candidate_intake_scores(
        ordered_candidate_rows=ordered_rows,
        scoring_context=scoring,
        candidate_outcome_records=outcome_records,
        orchestration_stats={
            "processed_candidates": 2,
            "slice_summaries": [{"candidate_count": 1}],
        },
    )

    assert summary["prevalidate_candidate_score_distribution"]
    assert summary["average_candidate_score_by_outcome"]["verified"] > 0
    assert summary["average_candidate_score_by_outcome"]["dead_end"] >= 0
    assert summary["average_feature_contributions_by_outcome"]["dead_end"]
    assert summary["top_false_positive_score_features"]
    assert summary["score_buckets_by_outcome"]
    assert summary["low_score_candidates_total"] >= 1
    assert summary["low_score_candidates_skipped"] >= 1


def test_summarize_candidate_intake_scores_does_not_count_unkept_verified_state() -> None:
    candidate_rows = [
        {
            "CandidateURL": "https://candidate.example/",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "SourceURL": "https://www.epicindie.net/authordirectory",
            "SourceTitle": "Jane Doe",
            "SourceSnippet": "Jane Doe indie fantasy author based in Ohio.",
        }
    ]
    ordered_rows, scoring = order_candidates_for_strict_validation(candidate_rows, validation_profile="strict_full")
    summary = summarize_candidate_intake_scores(
        ordered_candidate_rows=ordered_rows,
        scoring_context=scoring,
        candidate_outcome_records=[
            {
                "candidate_url": "https://candidate.example/",
                "source_type": "epic_directory",
                "source_query": "epic:author-directory",
                "source_url": "https://www.epicindie.net/authordirectory",
                "current_state": "verified",
                "reject_reason": "non_us_location",
                "kept": False,
            }
        ],
        orchestration_stats={"processed_candidates": 1, "slice_summaries": [{"candidate_count": 1}]},
    )

    assert summary["average_candidate_score_by_outcome"]["verified"] == 0.0
    assert summary["average_candidate_score_by_outcome"]["failed"] > 0.0


def test_queue_enrichment_helpers_pick_contactable_url() -> None:
    row = {
        "AuthorName": "Jane Doe",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "AuthorWebsite": "https://janedoeauthor.com",
        "SourceURL": "https://directory.example/jane-doe",
        "SourceTitle": "Jane Doe",
        "SourceSnippet": "Jane Doe, indie fantasy author.",
        "BookTitleStatus": "missing_or_weak",
        "RecencyStatus": "missing",
    }

    candidate_url = queue_row_candidate_url(row)
    candidate_row = queue_row_to_candidate(row)

    assert is_staged_row(row)
    assert candidate_url == "https://janedoeauthor.com/contact"
    assert candidate_row["CandidateURL"] == "https://janedoeauthor.com/contact"
    assert candidate_row["SourceQuery"] == "queue_enrichment"


def test_validate_candidates_keeps_unverified_listing_when_not_strict(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about",
                "SourceQuery": "search",
            }
        )

    pages = {
        "https://janedoeauthor.com/about": """
            <html><head><title>Jane Doe - Skyfall</title></head><body>
              <h1>Jane Doe</h1>
              <h2>Skyfall</h2>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><a href="mailto:hello@janedoeauthor.com">hello@janedoeauthor.com</a></body></html>
        """,
        "https://janedoeauthor.com": """
            <html><body><p>Official site for Jane Doe.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Jane Doe</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.amazon.com/dp/B012345678"
    assert rows[0]["ListingStatus"] == "unverified"
    assert rows[0]["ListingFailReason"] == "listing_wrong_format_or_incomplete"


def test_validate_candidates_still_rejects_unverified_listing_when_strict(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about",
                "SourceQuery": "search",
            }
        )

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <h2>Skyfall</h2>
              <p>Independently published fantasy author in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com": """
            <html><body><p>Jane Doe official site.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_incomplete_product_state"] == 1


def test_classify_listing_failure_splits_major_listing_buckets() -> None:
    mismatch_soup = BeautifulSoup(
        "<html><body><h1>Moonfall</h1><p>Paperback $14.99 Buy now</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=mismatch_soup.get_text(" ", strip=True),
        listing_soup=mismatch_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_title_mismatch"

    no_price_soup = BeautifulSoup(
        "<html><body><h1>Skyfall</h1><p>Paperback Buy now</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=no_price_soup.get_text(" ", strip=True),
        listing_soup=no_price_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_no_price"

    no_buy_soup = BeautifulSoup(
        "<html><body><h1>Skyfall</h1><p>Paperback $14.99</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=no_buy_soup.get_text(" ", strip=True),
        listing_soup=no_buy_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_no_buy_control"

    unavailable_soup = BeautifulSoup(
        "<html><body><h1>Skyfall</h1><p>Paperback $14.99 Currently unavailable</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=unavailable_soup.get_text(" ", strip=True),
        listing_soup=unavailable_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_unavailable"

    reseller_soup = BeautifulSoup(
        "<html><body><h1>Skyfall</h1><p>Paperback Used from $9.99 Other sellers on Amazon</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=reseller_soup.get_text(" ", strip=True),
        listing_soup=reseller_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_reseller_only"

    incomplete_soup = BeautifulSoup(
        "<html><body><h1>Skyfall</h1><p>$14.99 Buy now</p></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=incomplete_soup.get_text(" ", strip=True),
        listing_soup=incomplete_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_wrong_format"

    parse_empty_soup = BeautifulSoup(
        "<html><body><div>Skyfall</div></body></html>",
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=parse_empty_soup.get_text(" ", strip=True),
        listing_soup=parse_empty_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_parse_empty"

    interstitial_soup = BeautifulSoup(
        """
        <html><body>
          <p>Amazon.com</p>
          <p>Click the button below to continue shopping</p>
          <p>Continue shopping</p>
          <p>Conditions of Use Privacy Policy © 1996-2025, Amazon.com, Inc. or its affiliates</p>
        </body></html>
        """,
        "html.parser",
    )
    assert classify_listing_failure(
        listing_url="https://www.amazon.com/dp/B012345678",
        listing_text=interstitial_soup.get_text(" ", strip=True),
        listing_soup=interstitial_soup,
        strict=True,
        expected_titles=["Skyfall"],
        author_name="Jane Doe",
    )["reason"] == "listing_interstitial_or_redirect_state"


def test_classify_listing_fetch_failure_splits_blocked_and_fetch_failed() -> None:
    blocked_reason, blocked_snippet = classify_listing_fetch_failure(
        FetchResult(url="https://www.amazon.com/dp/B012345678", failure_reason="403", status_code=403)
    )
    assert blocked_reason == "listing_redirect_or_blocked"
    assert blocked_snippet == "403"

    fetch_failed_reason, fetch_failed_snippet = classify_listing_fetch_failure(
        FetchResult(url="https://www.amazon.com/dp/B012345678", failure_reason="404", status_code=404)
    )
    assert fetch_failed_reason == "listing_fetch_failed_404"
    assert fetch_failed_snippet == "404"

    timeout_reason, timeout_snippet = classify_listing_fetch_failure(
        FetchResult(url="https://www.amazon.com/dp/B012345678", failure_reason="timeout")
    )
    assert timeout_reason == "listing_fetch_failed_timeout"
    assert timeout_snippet == "timeout"

    non_html_reason, non_html_snippet = classify_listing_fetch_failure(
        FetchResult(url="https://www.amazon.com/dp/B012345678", failure_reason="non_html")
    )
    assert non_html_reason == "listing_fetch_failed_non_html"
    assert non_html_snippet == "non_html"


def test_validate_candidates_records_listing_debug_artifact_and_specific_reason(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    stats_path = tmp_path / "run_001_validate_stats.json"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery", "SourceTitle", "SourceSnippet"])
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about",
                "SourceQuery": "search",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
            }
        )

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <h2>Skyfall</h2>
              <p>Independently published fantasy author in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com": "<html><body><p>Jane Doe official site.</p></body></html>",
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output=str(stats_path),
        validation_profile="default",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_no_buy_control"] == 1

    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["listing_reject_reason_counts"]["listing_no_buy_control"] == 1
    listing_debug_path = tmp_path / "run_001_listing_debug.jsonl"
    assert runtime_stats["listing_debug_path"] == str(listing_debug_path)
    records = [json.loads(line) for line in listing_debug_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(records) == 1
    assert records[0]["reject_reason"] == "listing_no_buy_control"
    assert records[0]["best_listing_candidate_url"] == "https://www.amazon.com/dp/B012345678"
    assert records[0]["best_listing_page_kind"] == "amazon_product"


def test_validate_candidates_recovers_listing_from_canonical_books_page(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about-author",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Email Jane Doe at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://janedoeauthor.com/books": """
            <html><body>
              <section class="books">
                <article class="book-card"><h2>Skyfall</h2><a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a></article>
                <article class="book-card"><h2>Starfall</h2></article>
              </section>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.amazon.com/dp/B012345678"
    assert rows[0]["ListingStatus"] == "verified"
    assert rows[0]["BookTitle"] == "Skyfall"


def test_validate_candidates_recovers_listing_from_titles_page(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about-author",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/titles">Titles</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Email jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://janedoeauthor.com/titles": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.barnesandnoble.com/w/skyfall-jane-doe/114514">Buy at Barnes & Noble</a>
            </body></html>
        """,
        "https://www.barnesandnoble.com/w/skyfall-jane-doe/114514": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Add to Cart</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.barnesandnoble.com/w/skyfall-jane-doe/114514"
    assert rows[0]["ListingStatus"] == "verified"


def test_validate_candidates_skips_repeated_amazon_interstitial_attempts_but_uses_bn_fallback(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    amazon_one = "https://www.amazon.com/dp/B012345678"
    amazon_two = "https://www.amazon.com/dp/B012345679"
    bn_url = "https://www.barnesandnoble.com/w/skyfall-jane-doe/114514"
    pages = {
        "https://janedoeauthor.com/": f"""
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>Publication date: March 1, 2026.</p>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
              <script type=\"application/ld+json\">{{\"@context\":\"https://schema.org\",\"@type\":\"Book\",\"name\":\"Skyfall\"}}</script>
              <a href=\"{amazon_one}\">Amazon edition</a>
              <a href=\"{amazon_two}\">Amazon hardcover</a>
              <a href=\"/titles\">Titles</a>
            </body></html>
        """,
        "https://janedoeauthor.com/titles": f"""
            <html><body>
              <h2>Skyfall</h2>
              <a href=\"{bn_url}\">Barnes & Noble</a>
            </body></html>
        """,
        amazon_one: """
            <html><body>
              <p>Amazon.com</p>
              <p>Click the button below to continue shopping</p>
              <p>Continue shopping</p>
              <p>Conditions of Use Privacy Policy © 1996-2025, Amazon.com, Inc. or its affiliates</p>
            </body></html>
        """,
        bn_url: """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Add to Cart</p></body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
        domain_cache_path="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == bn_url
    assert counts.get(amazon_one, 0) == 1
    assert counts.get(amazon_two, 0) == 0
    assert counts.get("https://janedoeauthor.com/titles", 0) == 1
    assert counts.get(bn_url, 0) == 1


def test_validate_candidates_keeps_interstitial_reject_when_no_valid_bn_fallback_exists(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    amazon_one = "https://www.amazon.com/dp/B012345678"
    amazon_two = "https://www.amazon.com/dp/B012345679"
    bn_url = "https://www.barnesandnoble.com/w/skyfall-jane-doe/114514"
    pages = {
        "https://janedoeauthor.com/": f"""
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>Publication date: March 1, 2026.</p>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
              <script type=\"application/ld+json\">{{\"@context\":\"https://schema.org\",\"@type\":\"Book\",\"name\":\"Skyfall\"}}</script>
              <a href=\"{amazon_one}\">Amazon edition</a>
              <a href=\"{amazon_two}\">Amazon hardcover</a>
              <a href=\"/titles\">Titles</a>
            </body></html>
        """,
        "https://janedoeauthor.com/titles": f"""
            <html><body>
              <h2>Skyfall</h2>
              <a href=\"{bn_url}\">Barnes & Noble</a>
            </body></html>
        """,
        amazon_one: """
            <html><body>
              <p>Amazon.com</p>
              <p>Click the button below to continue shopping</p>
              <p>Continue shopping</p>
              <p>Conditions of Use Privacy Policy © 1996-2025, Amazon.com, Inc. or its affiliates</p>
            </body></html>
        """,
        bn_url: None,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
        domain_cache_path="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_amazon_interstitial_bn_failed_other"] == 1
    assert reject_counts.get("listing_parse_empty", 0) == 0
    assert reject_counts.get("listing_incomplete_product_state", 0) == 0
    assert counts.get(amazon_one, 0) == 1
    assert counts.get(amazon_two, 0) == 0
    assert counts.get("https://janedoeauthor.com/titles", 0) == 1
    assert counts.get(bn_url, 0) == 1
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["listing_reject_reason_counts"]["listing_amazon_interstitial_bn_failed_other"] == 1


def test_validate_candidates_marks_terminal_interstitial_without_bn_candidate(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    amazon_one = "https://www.amazon.com/dp/B012345678"
    amazon_two = "https://www.amazon.com/dp/B012345679"
    pages = {
        "https://janedoeauthor.com/": f"""
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>Publication date: March 1, 2026.</p>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
              <script type=\"application/ld+json\">{{\"@context\":\"https://schema.org\",\"@type\":\"Book\",\"name\":\"Skyfall\"}}</script>
              <a href=\"{amazon_one}\">Amazon edition</a>
              <a href=\"{amazon_two}\">Amazon hardcover</a>
            </body></html>
        """,
        amazon_one: """
            <html><body>
              <p>Amazon.com</p>
              <p>Click the button below to continue shopping</p>
              <p>Continue shopping</p>
              <p>Conditions of Use Privacy Policy © 1996-2025, Amazon.com, Inc. or its affiliates</p>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
        domain_cache_path="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_amazon_interstitial_no_bn_candidate"] == 1
    assert counts.get(amazon_one, 0) == 1
    assert counts.get(amazon_two, 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["listing_reject_reason_counts"]["listing_amazon_interstitial_no_bn_candidate"] == 1


def test_validate_candidates_marks_terminal_interstitial_when_bn_blocked(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    amazon_one = "https://www.amazon.com/dp/B012345678"
    amazon_two = "https://www.amazon.com/dp/B012345679"
    bn_url = "https://www.barnesandnoble.com/w/skyfall-jane-doe/114514"
    pages = {
        "https://janedoeauthor.com/": f"""
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>Publication date: March 1, 2026.</p>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
              <script type=\"application/ld+json\">{{\"@context\":\"https://schema.org\",\"@type\":\"Book\",\"name\":\"Skyfall\"}}</script>
              <a href=\"{amazon_one}\">Amazon edition</a>
              <a href=\"{amazon_two}\">Amazon hardcover</a>
              <a href=\"/titles\">Titles</a>
            </body></html>
        """,
        "https://janedoeauthor.com/titles": f"""
            <html><body>
              <h2>Skyfall</h2>
              <a href=\"{bn_url}\">Barnes & Noble</a>
            </body></html>
        """,
        amazon_one: """
            <html><body>
              <p>Amazon.com</p>
              <p>Click the button below to continue shopping</p>
              <p>Continue shopping</p>
              <p>Conditions of Use Privacy Policy © 1996-2025, Amazon.com, Inc. or its affiliates</p>
            </body></html>
        """,
        bn_url: ("Forbidden", "text/html", bn_url),
    }

    def fake_fetch(session, url, timeout, robots_cache, robots_text_cache, ignore_robots, retry_seconds, expect_html=True):  # noqa: ANN001
        if "barnesandnoble.com/w/skyfall-jane-doe/114514" in url:
            from prospect_validate import FetchResult
            return FetchResult(url=url, final_url=url, failure_reason="403", status_code=403)
        return make_fetch_result(url, pages.get(url), expect_html=expect_html)

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
        domain_cache_path="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_amazon_interstitial_bn_blocked"] == 1


def test_validate_candidates_recovers_listing_from_detail_page_discovered_from_same_domain_navigation(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Shadowfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/library">Books</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Email jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://janedoeauthor.com/library": """
            <html><body>
              <a href="/shadowfall">Shadowfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/shadowfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Shadowfall"}
              </script>
              <h1>Shadowfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Shadowfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.amazon.com/dp/B012345678"
    assert rows[0]["ListingStatus"] == "verified"


def test_validate_candidates_recovers_listing_from_store_product_page_discovered_from_sitemap(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Summerfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Email jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://janedoeauthor.com/sitemap.xml": """
            <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <sitemap><loc>https://janedoeauthor.com/store-products-sitemap.xml</loc></sitemap>
            </sitemapindex>
        """,
        "https://janedoeauthor.com/store-products-sitemap.xml": """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://janedoeauthor.com/product-page/summerfall</loc></url>
            </urlset>
        """,
        "https://janedoeauthor.com/product-page/summerfall": """
            <html><body>
              <h1>Summerfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Summerfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.amazon.com/dp/B012345678"
    assert rows[0]["ListingStatus"] == "verified"


def test_validate_candidates_recovers_listing_from_shortlink_on_same_domain_works_page(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Once More Into The Dark.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/other-works">Other Works</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Email jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://janedoeauthor.com/other-works": """
            <html><body>
              <a href="https://mybook.to/OnceMoreIntoTheDark">Buy Now</a>
            </body></html>
        """,
        "https://mybook.to/OnceMoreIntoTheDark": (
            "redirected",
            "text/html",
            "https://www.amazon.com/dp/B0CJMXXBQR?tag=bk00010a-20&geniuslink=true",
        ),
        "https://www.amazon.com/dp/B0CJMXXBQR?tag=bk00010a-20&geniuslink=true": """
            <html><body><h1>Once More Into The Dark</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
        "https://www.amazon.com/dp/B0CJMXXBQR": """
            <html><body><h1>Once More Into The Dark</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=True,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["ListingURL"] == "https://www.amazon.com/dp/B0CJMXXBQR"
    assert rows[0]["ListingStatus"] == "verified"


def test_normalize_listing_fail_reason_infers_specific_reason_from_blank_value() -> None:
    assert normalize_listing_fail_reason(listing_status="missing", listing_fail_reason="") == "listing_not_found"
    assert normalize_listing_fail_reason(listing_status="fetch_failed", listing_fail_reason="") == "listing_fetch_failed"
    assert normalize_listing_fail_reason(listing_status="unverified", listing_fail_reason="") == "listing_wrong_format_or_incomplete"
    assert (
        normalize_listing_fail_reason(listing_status="fetch_failed", listing_fail_reason="listing_fetch_failed_404")
        == "listing_fetch_failed_404"
    )
    assert normalize_listing_fail_reason(
        listing_status="verified",
        listing_fail_reason="",
        book_title="Skyfall",
        listing_title="Moonfall",
    ) == "listing_title_mismatch"


def test_row_meets_fully_verified_profile_uses_specific_listing_reason_when_blank() -> None:
    ok, reason = row_meets_fully_verified_profile(
        author_name="Jane Doe",
        book_title="Skyfall",
        book_title_status="ok",
        book_title_method="jsonld_book",
        author_email="jane@janedoeauthor.com",
        author_email_quality="same_domain",
        author_email_record={"visible_text": "true"},
        location="Austin, TX",
        indie_proof_strength="onsite",
        listing_status="missing",
        listing_fail_reason="",
        recency_status="verified",
        listing_title="",
        require_us_location=True,
        location_decision="ok",
    )
    assert ok is False
    assert reason == "listing_not_found"


def test_row_meets_fully_verified_profile_does_not_require_book_title() -> None:
    ok, reason = row_meets_fully_verified_profile(
        author_name="Jane Doe",
        book_title="",
        book_title_status="missing_or_weak",
        book_title_method="fallback",
        author_email="jane@janedoeauthor.com",
        author_email_quality="same_domain",
        author_email_record={"visible_text": "true"},
        location="Austin, TX",
        indie_proof_strength="onsite",
        listing_status="verified",
        listing_fail_reason="",
        recency_status="verified",
        listing_title="",
        require_us_location=True,
        location_decision="ok",
    )
    assert ok is True
    assert reason == ""


def test_stats_aggregation_prefers_specific_listing_reason_over_generic() -> None:
    reject_counts = Counter(
        {
            normalize_listing_fail_reason(listing_status="missing", listing_fail_reason=""): 1,
            normalize_listing_fail_reason(
                listing_status="fetch_failed",
                listing_fail_reason="listing_fetch_failed_timeout",
            ): 1,
            normalize_listing_fail_reason(
                listing_status="unverified",
                listing_fail_reason="missing_required_signals",
            ): 1,
        }
    )
    assert "no_valid_listing" not in reject_counts
    assert reject_counts["listing_not_found"] == 1
    assert reject_counts["listing_fetch_failed_timeout"] == 1
    assert reject_counts["listing_wrong_format_or_incomplete"] == 1


def test_should_track_listing_reject_reason_skips_missing_listing_for_dead_strict_email_rows() -> None:
    assert (
        should_track_listing_reject_reason(
            strict_verified_mode=True,
            listing_status="missing",
            listing_fail_reason="listing_not_found",
            listing_recovery_skipped_reason="missing_strict_email",
            listing_candidates=[],
            listing_attempts=[],
        )
        is False
    )
    assert (
        should_track_listing_reject_reason(
            strict_verified_mode=True,
            listing_status="missing",
            listing_fail_reason="listing_not_found",
            listing_recovery_skipped_reason="publisher_location_only_precheck",
            listing_candidates=[],
            listing_attempts=[],
        )
        is True
    )


def test_validate_candidates_fully_verified_accepts_visible_email_and_onsite_proof(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about-author",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Contact Jane Doe about Skyfall at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="astra_outbound",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["AuthorEmail"] == "jane@janedoeauthor.com"
    assert rows[0]["EmailQuality"] == "same_domain"
    assert rows[0]["BookTitle"] == "Skyfall"
    assert rows[0]["BookTitleMethod"] in {"jsonld_book", "listing_title_oracle"}
    assert rows[0]["ListingStatus"] == "verified"
    assert rows[0]["RecencyStatus"] == "verified"
    assert rows[0]["IndieProofStrength"] in {"onsite", "both"}


def test_validate_candidates_email_only_accepts_visible_email_without_strict_proofs(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "search",
                "SourceURL": "https://directory.example/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Fantasy author contact details.",
                "SourceType": "web_search",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe writes fantasy novels.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Reach Jane Doe at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
    }

    email_only_args = make_validator_args(tmp_path, candidates_path, validation_profile="email_only")
    email_only_args.max_pages_for_title = 1
    email_only_args.max_pages_for_contact = 2

    strict_full_args = make_validator_args(tmp_path, candidates_path, validation_profile="strict_full")
    strict_full_args.max_pages_for_title = 1
    strict_full_args.max_pages_for_contact = 2

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        email_only_rows, email_only_reject_counts, email_only_total = validate_candidates(email_only_args)

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        strict_rows, strict_reject_counts, strict_total = validate_candidates(strict_full_args)

    assert email_only_total == 1
    assert email_only_reject_counts == {}
    assert len(email_only_rows) == 1
    assert strict_total == 1
    assert strict_rows == []
    assert sum(strict_reject_counts.values()) == 1

    row = email_only_rows[0]
    assert row["AuthorName"] == "Jane Doe"
    assert row["AuthorEmail"] == "jane@janedoeauthor.com"
    assert row["SourceURL"] == "https://directory.example/jane-doe"
    assert row["IndieProofStrength"] == ""
    assert row["ListingStatus"] == "missing"
    assert row["RecencyStatus"] == "missing"


def test_validate_candidates_email_only_rejects_business_brand_row(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceTitle", "SourceSnippet", "SourceURL"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://novelnotions.net/",
                "SourceQuery": "search",
                "SourceTitle": "Novel Notions",
                "SourceSnippet": "Novel Notions book reviews and interviews.",
                "SourceURL": "https://search.example/novel-notions",
            }
        )

    pages = {
        "https://novelnotions.net/": """
            <html><body>
              <h1>Novel Notions</h1>
              <p>Novel Notions is a book review blog covering independently published fantasy books.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://novelnotions.net/contact": """
            <html><body>
              <p>Email Novel Notions at hello [at] novelnotions [dot] net.</p>
            </body></html>
        """,
    }

    args = make_validator_args(tmp_path, candidates_path, validation_profile="email_only")
    args.max_pages_for_title = 1
    args.max_pages_for_contact = 2

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["bad_author_name"] == 1


def test_validate_candidates_email_only_rejects_bad_author_name(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceTitle", "SourceSnippet", "SourceURL"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://bookshub.example/",
                "SourceQuery": "search",
                "SourceTitle": "Books",
                "SourceSnippet": "Books contact page.",
                "SourceURL": "https://search.example/books",
            }
        )

    pages = {
        "https://bookshub.example/": """
            <html><body>
              <h1>Books</h1>
              <p>Email books [at] bookshub [dot] example for more information.</p>
            </body></html>
        """,
    }

    args = make_validator_args(tmp_path, candidates_path, validation_profile="email_only")
    args.max_pages_for_title = 1
    args.max_pages_for_contact = 1

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["bad_author_name"] == 1


def test_validate_candidates_email_only_rejects_mailto_only_email(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "search",
                "SourceURL": "https://directory.example/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Fantasy author contact details.",
                "SourceType": "web_search",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe writes fantasy novels.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <a href="mailto:jane@janedoeauthor.com">Email Jane</a>
            </body></html>
        """,
    }

    args = make_validator_args(tmp_path, candidates_path, validation_profile="email_only")
    args.max_pages_for_title = 1
    args.max_pages_for_contact = 2

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_visible_author_email"] == 1


def test_validate_candidates_verified_no_us_accepts_missing_location_with_other_proof(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Contact Jane Doe about Skyfall at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="verified_no_us",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["AuthorEmail"] == "jane@janedoeauthor.com"
    assert rows[0]["ListingStatus"] == "verified"
    assert rows[0]["RecencyStatus"] == "verified"
    assert rows[0]["Location"] == "Unknown"


def test_classify_location_evidence_splits_location_buckets() -> None:
    assert classify_location_evidence(
        text="Official site for Jane Doe.",
        source_url="https://janedoeauthor.com/",
    )["decision"] == "no_location_signal"

    assert classify_location_evidence(
        text="Jane Doe is based in London, England.",
        source_url="https://janedoeauthor.com/about",
    )["decision"] == "non_us_location"

    assert classify_location_evidence(
        text="Publisher headquartered in New York, NY.",
        source_url="https://publisher.example/about",
        source_kind="publisher",
    )["decision"] == "publisher_location_only"

    assert classify_location_evidence(
        text="Jane Doe is based in the Pacific Northwest.",
        source_url="https://janedoeauthor.com/about",
    )["decision"] == "location_ambiguous"

    assert classify_location_evidence(
        text="Jane Doe writes fiction in Texas.",
        source_url="https://janedoeauthor.com/about",
    )["decision"] == "weak_us_signal"


def test_classify_location_evidence_accepts_author_tied_us_phrases() -> None:
    assert classify_location_evidence(
        text="Jane Doe is based in California and writes epic fantasy.",
        source_url="https://janedoeauthor.com/about",
        source_kind="about_page",
        author_name="Jane Doe",
    )["decision"] == "ok"

    assert classify_location_evidence(
        text="Jane Doe is an indie author from Tennessee.",
        source_url="https://janedoeauthor.com/about-author",
        source_kind="author_page",
        author_name="Jane Doe",
    )["decision"] == "ok"

    result = classify_location_evidence(
        text="Jane Doe is an award-winning fantasy author in Portland, Oregon.",
        source_url="https://janedoeauthor.com/bio",
        source_kind="bio_page",
        author_name="Jane Doe",
    )
    assert result["decision"] == "ok"
    assert result["location"] == "Portland, Oregon"

    nav_noisy = classify_location_evidence(
        text=(
            "Author Zack Argyle SIGN UP FOR THE NEWSLETTER Store Blog SPFBO Art About Contact "
            "Zack Argyle lives just outside of Seattle, WA, USA, with his wife and two children."
        ),
        source_url="https://www.zackargyle.com/about",
        source_kind="about_page",
        author_name="Zack Argyle",
    )
    assert nav_noisy["decision"] == "ok"
    assert nav_noisy["location"] == "Zack Argyle lives just outside of Seattle, WA, USA"

    first_person = classify_location_evidence(
        text=(
            "I love to work in watercolor pencils, acrylic and oil paints. "
            "I live in Medford, NJ with my husband and three kids."
        ),
        source_url="https://www.tamikareedwrites.com/",
        source_kind="page",
        author_name="Tamika Reed",
    )
    assert first_person["decision"] == "ok"
    assert first_person["location"] == "I live in Medford, NJ"


def test_classify_location_evidence_rejects_generic_usa_and_footer_false_positives() -> None:
    assert classify_location_evidence(
        text="Available throughout the USA for readers everywhere.",
        source_url="https://janedoeauthor.com/",
        source_kind="page",
        author_name="Jane Doe",
    )["decision"] == "weak_us_signal"

    assert classify_location_evidence(
        text="123 Main Street, Austin, TX. Open daily for in-store pickup.",
        source_url="https://janedoeauthor.com/contact",
        source_kind="contact_page",
        author_name="Jane Doe",
    )["decision"] == "publisher_location_only"

    assert classify_location_evidence(
        text="E.G. Radcliff 154 West Park Avenue #141 | Elmhurst, IL 60126 | info@egradcliff.com",
        source_url="https://www.egradcliff.com/about",
        source_kind="about_page",
        author_name="E. G. Radcliff",
    )["decision"] == "publisher_location_only"

    assert classify_location_evidence(
        text="Another Books and Beers event in beautiful Howell, NJ. Authors from across New Jersey attend.",
        source_url="https://tomkranzbooks.com/",
        source_kind="page",
        author_name="Tom Kranz",
    )["decision"] == "publisher_location_only"


def test_classify_location_evidence_prefers_author_location_over_publisher_location() -> None:
    result = classify_location_evidence(
        text=(
            "Jane Doe is a fantasy novelist based in Columbus, Ohio. "
            "Published by North Star Press, headquartered in London, England."
        ),
        source_url="https://janedoeauthor.com/about",
        source_kind="about_page",
        author_name="Jane Doe",
    )

    assert result["decision"] == "ok"
    assert result["location"] == "Columbus, Ohio"


def test_validate_candidates_records_specific_location_reject_reason_and_debug_artifact(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    stats_path = tmp_path / "run_001_validate_stats.json"
    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output=str(stats_path),
        validation_profile="fully_verified",
        domain_cache_path="",
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_location_signal"] == 1
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["location_decision_counts"]["no_location_signal"] == 1
    debug_path = tmp_path / "run_001_location_debug.jsonl"
    assert runtime_stats["location_debug_path"] == str(debug_path)
    debug_lines = [json.loads(line) for line in debug_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(debug_lines) == 1
    assert debug_lines[0]["final_decision"] == "no_location_signal"
    assert debug_lines[0]["candidate_url"] == "https://janedoeauthor.com/"
    near_miss_path = tmp_path / "near_miss_location.csv"
    assert runtime_stats["near_miss_location_path"] == str(near_miss_path)
    near_miss_rows = list(csv.DictReader(near_miss_path.open("r", encoding="utf-8", newline="")))
    assert len(near_miss_rows) == 1
    assert near_miss_rows[0]["AuthorName"] == "Jane Doe"
    assert near_miss_rows[0]["BookTitle"] == "Skyfall"
    assert near_miss_rows[0]["AuthorEmail"] == "jane@janedoeauthor.com"
    assert near_miss_rows[0]["RejectReason"] == "no_location_signal"


def test_validate_candidates_recovers_location_from_same_domain_author_page(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>New release March 1, 2026.</p>
              <a href="/about-author">About the Author</a>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/about-author": """
            <html><body>
              <h2>About Jane Doe</h2>
              <p>Jane Doe is a fantasy author from Columbus, Ohio.</p>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="fully_verified",
        domain_cache_path="",
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["Location"] == "Columbus, Ohio"
    assert rows[0]["LocationProofURL"] == "https://janedoeauthor.com/about-author"
    assert "fantasy author from Columbus, Ohio" in rows[0]["LocationProofSnippet"]


def test_validate_candidates_location_recovery_finds_unlinked_author_page(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/about-author": """
            <html><body>
              <h2>About Jane Doe</h2>
              <p>Jane Doe is a fantasy author from Columbus, Ohio.</p>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="fully_verified",
        domain_cache_path="",
        max_fetches_per_domain=0,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["Location"] == "Columbus, Ohio"
    assert rows[0]["LocationProofURL"] == "https://janedoeauthor.com/about-author"


def test_validate_candidates_strict_skips_listing_recovery_when_email_missing(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_visible_author_email"] == 1
    assert counts.get("https://janedoeauthor.com/extras", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["listing_reject_reason_counts"].get("listing_not_found", 0) == 0
    assert runtime_stats["candidate_state_counts"]["needs_email_proof"] == 1
    assert runtime_stats["planned_action_counts"]["try_email_proof"] == 1
    assert runtime_stats["proof_ledger_samples"][0]["reject_reason_if_any"] == "no_visible_author_email"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_skipped_reason"] == "missing_strict_email"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is False


def test_validate_candidates_strict_skips_location_recovery_after_non_location_failure(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p></body></html>
        """,
        "https://janedoeauthor.com/about-author": """
            <html><body><p>Jane Doe is a fantasy author from Columbus, Ohio.</p></body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["listing_not_found"] == 1
    assert counts.get("https://janedoeauthor.com/about-author", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["location_recovery_skipped_reason"] == "strict_non_location_failure"
    assert runtime_stats["top_candidate_budget_burn"][0]["location_recovery_attempted"] is False


def test_validate_candidates_strict_skips_listing_recovery_for_definitive_non_us_location(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in London, England.</p>
              <p>New release March 1, 2026.</p>
              <p>Email Jane Doe at jane [at] janedoeauthor [dot] com.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["non_us_location"] == 1
    assert counts.get("https://janedoeauthor.com/extras", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_skipped_reason"] == "definitive_non_us_location"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is False


def test_validate_candidates_strict_skips_listing_recovery_for_weak_email_quality(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <p>Email Jane Doe at info [at] janedoeauthor [dot] com.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_visible_author_email"] == 1
    assert counts.get("https://janedoeauthor.com/extras", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_skipped_reason"] == "weak_strict_email_quality"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is False


def test_validate_candidates_strict_skips_listing_recovery_for_publisher_location_dead_end(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about-author",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/about-author": """
            <html><body>
              <p>Independently published fantasy novels.</p>
              <p>Published by Skyfall Press from Austin, TX. Contact us for support.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/extras">Buy Skyfall</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Email jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
            </body></html>
        """,
        "https://janedoeauthor.com/extras": """
            <html><body>
              <h2>Skyfall</h2>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=2,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="fully_verified",
        location_recovery_mode="same_domain",
        location_recovery_pages=4,
        near_miss_location_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["publisher_location_only"] == 1
    assert counts.get("https://janedoeauthor.com/extras", 0) == 0
    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_skipped_reason"] == "publisher_location_only_precheck"
    assert runtime_stats["top_candidate_budget_burn"][0]["listing_recovery_attempted"] is False


def test_validate_candidates_fully_verified_rejects_mailto_only_email(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Skyfall.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <a href="mailto:jane@janedoeauthor.com">Email Jane</a>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_visible_author_email"] == 1


def test_validate_candidates_fully_verified_rejects_directory_only_indie_proof(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet", "SourceType"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory/jane-doe",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, indie fantasy author in Austin, TX.",
                "SourceType": "epic_directory",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is a fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"Skyfall"}
              </script>
              <h1>Skyfall</h1>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <p>Contact Jane Doe at jane [at] janedoeauthor [dot] com.</p>
            </body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=3,
        max_pages_for_contact=3,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
        validation_profile="fully_verified",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["no_onsite_indie_proof"] == 1


def test_validate_candidates_prefilter_counts_only_stage_b_ready_candidates(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow({"CandidateURL": "https://missing.example.com/", "SourceQuery": "search"})
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/", "SourceQuery": "search"})

    pages = {
        "https://missing.example.com/": None,
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": "<html><body><h1>Skyfall</h1></body></html>",
        "https://janedoeauthor.com/contact": "<html><body><p>Contact Jane Doe.</p></body></html>",
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="default",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 2
    assert reject_counts["page_fetch_failed_404"] == 1
    assert getattr(args, "_validation_stats")["verify_candidates"] == 1
    assert len(rows) == 1


def test_validate_candidates_enforces_fetch_budget(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/about", "SourceQuery": "search"})

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": "<html><body><h1>Skyfall</h1></body></html>",
        "https://janedoeauthor.com/contact": "<html><body><p>Contact Jane Doe.</p></body></html>",
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=1,
        max_fetches_per_domain=1,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="default",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["fetch_budget_exceeded"] == 1


def test_validate_candidates_default_profile_rejects_review_blog_brand_identity(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://novelnotions.net/",
                "SourceQuery": "search",
                "SourceTitle": "Novel Notions",
                "SourceSnippet": "Novel Notions book reviews and indie author interviews.",
            }
        )

    pages = {
        "https://novelnotions.net/": """
            <html><body>
              <h1>Novel Notions</h1>
              <h2>Skyfall</h2>
              <p>Novel Notions is a book review blog covering independently published fantasy books and author interviews.</p>
              <p>Updated March 1, 2026.</p>
              <a href="/contact">Contact</a>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://novelnotions.net/contact": "<html><body><p>Contact Novel Notions.</p></body></html>",
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Add to cart In stock</p></body></html>
        """,
    }

    args = make_validator_args(tmp_path, candidates_path, validation_profile="default")
    args.max_pages_for_title = 1
    args.max_pages_for_contact = 1

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert rows == []
    assert reject_counts["bad_author_name"] == 1


def test_validate_candidates_records_budget_burn_summaries(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/about", "SourceQuery": "search"})

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": "<html><body><p>Contact Jane Doe.</p></body></html>",
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="default",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        validate_candidates(args)

    runtime_stats = getattr(args, "_validation_stats")
    assert runtime_stats["candidate_budget_burn_count"] == 1
    assert runtime_stats["top_candidate_budget_burn"]
    assert runtime_stats["top_candidate_budget_burn"][0]["candidate_domain"] == "janedoeauthor.com"
    assert runtime_stats["top_candidate_budget_burn"][0]["fetch_count"] >= 1
    assert runtime_stats["top_candidate_budget_burn"][0]["dominant_phase"] in {"prefilter", "verify", "balanced"}
    assert runtime_stats["top_domain_budget_burn"]
    assert runtime_stats["top_domain_budget_burn"][0]["domain"] == "janedoeauthor.com"
    assert runtime_stats["top_domain_budget_burn"][0]["dominant_phase"] in {"prefilter", "verify", "balanced"}


def test_validate_candidates_reuses_sitemap_cache_within_run(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/", "SourceQuery": "search"})
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/about", "SourceQuery": "search"})

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/sitemap.xml": (
            "<?xml version='1.0' encoding='UTF-8'?><urlset>"
            "<url><loc>https://janedoeauthor.com/books/skyfall</loc></url>"
            "<url><loc>https://janedoeauthor.com/contact</loc></url>"
            "</urlset>",
            "application/xml",
        ),
        "https://janedoeauthor.com/books/skyfall": "<html><body><h1>Skyfall</h1></body></html>",
        "https://janedoeauthor.com/contact": "<html><body><p>Contact Jane Doe.</p></body></html>",
    }
    counts: dict[str, int] = {}

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path="",
        validation_profile="default",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        validate_candidates(args)

    assert counts["https://janedoeauthor.com/sitemap.xml"] == 1
    assert getattr(args, "_validation_stats")["cache_hits"]["sitemap"] >= 1


def test_validate_candidates_resume_uses_domain_cache(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow({"CandidateURL": "https://janedoeauthor.com/", "SourceQuery": "search"})

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/sitemap.xml": (
            "<?xml version='1.0' encoding='UTF-8'?><urlset>"
            "<url><loc>https://janedoeauthor.com/contact</loc></url>"
            "</urlset>",
            "application/xml",
        ),
        "https://janedoeauthor.com/contact": "<html><body><p>Contact Jane Doe.</p></body></html>",
    }
    cache_path = tmp_path / "domain_cache.jsonl"
    first_counts: dict[str, int] = {}
    second_counts: dict[str, int] = {}

    args_first = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated_first.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=1,
        max_total_fetches_per_domain_per_run=14,
        max_fetches_per_domain=14,
        max_seconds_per_domain=25.0,
        max_timeouts_per_domain=2,
        max_total_runtime=900.0,
        max_concurrency=4,
        listing_strict=False,
        stats_output="",
        domain_cache_path=str(cache_path),
        validation_profile="default",
    )

    args_second = Namespace(**{**args_first.__dict__, "output": str(tmp_path / "validated_second.csv")})

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, first_counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        validate_candidates(args_first)

    assert cache_path.exists()

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=counting_fetch_with_meta_factory(pages, second_counts)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        validate_candidates(args_second)

    assert first_counts["https://janedoeauthor.com/sitemap.xml"] == 1
    assert second_counts.get("https://janedoeauthor.com/sitemap.xml", 0) == 0


def test_validate_candidates_prefers_verified_listing_title_over_fallback(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, author of Vigil: Inferno Season.",
            }
        )

    pages = {
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane for media inquiries.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=1,
        max_pages_for_contact=2,
        max_total_fetches_per_domain_per_run=6,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["BookTitle"] == "Skyfall"
    assert rows[0]["BookTitleMethod"] == "listing_title_oracle"
    assert rows[0]["BookTitleConfidence"] == "strong"
    assert int(rows[0]["BookTitleScore"]) > 0
    assert rows[0]["ListingStatus"] == "verified"


def test_validate_candidates_keeps_weak_title_as_blank_instead_of_rejecting(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://www.leeconleyauthor.com/about",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory",
                "SourceTitle": "Lee C. Conley",
                "SourceSnippet": 'Lee C. Conley, author of "Speculative Fiction - Lee C".',
            }
        )

    pages = {
        "https://www.leeconleyauthor.com/about": """
            <html><body>
              <h1>Lee C. Conley</h1>
              <p>Lee C. Conley is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://www.leeconleyauthor.com/contact": """
            <html><body><p>Contact Lee for media inquiries.</p></body></html>
        """,
        "https://www.leeconleyauthor.com": """
            <html><body><p>Lee C. Conley official site.</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=2,
        max_pages_for_contact=4,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["BookTitle"] == ""
    assert rows[0]["BookTitleConfidence"] == "weak"
    assert rows[0]["BookTitleStatus"] == "missing_or_weak"
    assert rows[0]["BookTitleRejectReason"] in {"matches_author_name", "looks_like_person_name", "weak_method"}
    assert rows[0]["BookTitleTopCandidates"] != ""


def test_validate_candidates_stages_missing_recency_instead_of_rejecting(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "Jane Doe, indie fantasy author.",
            }
        )

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <h2>Skyfall</h2>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane for media inquiries.</p></body></html>
        """,
        "https://janedoeauthor.com": """
            <html><body><p>Official site for Jane Doe.</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=2,
        max_pages_for_contact=4,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["BookTitle"] == ""
    assert rows[0]["BookTitleStatus"] == "missing_or_weak"
    assert rows[0]["BookTitleConfidence"] == "weak"
    assert rows[0]["RecencyStatus"] == "missing"
    assert rows[0]["RecencyFailReason"] == "no_on_page_date"


def test_validate_candidates_salvages_root_when_candidate_deep_link_404(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["CandidateURL", "SourceQuery"])
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about-old",
                "SourceQuery": "search",
            }
        )

    pages = {
        "https://janedoeauthor.com/about-old": None,
        "https://janedoeauthor.com/": """
            <html><body>
              <h1>Jane Doe</h1>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/skyfall">Skyfall</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://janedoeauthor.com/books/skyfall": """
            <html><body><h1>Skyfall</h1></body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body><p>Contact Jane for media inquiries.</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=2,
        max_pages_for_contact=4,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["AuthorWebsite"] == "https://janedoeauthor.com"
    assert rows[0]["BookTitle"] == ""
    assert rows[0]["BookTitleStatus"] == "missing_or_weak"
    assert rows[0]["BookTitleConfidence"] == "weak"


def test_validate_candidates_promotes_explicit_book_page_title_over_weak_source_label(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://www.leeconleyauthor.com/",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory",
                "SourceTitle": "Lee C. Conley",
                "SourceSnippet": "Speculative Fiction - Lee C",
            }
        )

    pages = {
        "https://www.leeconleyauthor.com/": """
            <html><body>
              <h1>Lee C. Conley</h1>
              <p>Lee C. Conley is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/books/a-ritual-of-bone">A Ritual of Bone</a>
              <a href="/contact">Contact</a>
            </body></html>
        """,
        "https://www.leeconleyauthor.com/books/a-ritual-of-bone": """
            <html><body>
              <script type="application/ld+json">
                {"@context":"https://schema.org","@type":"Book","name":"A Ritual of Bone"}
              </script>
              <h1>A Ritual of Bone</h1>
            </body></html>
        """,
        "https://www.leeconleyauthor.com/contact": """
            <html><body><p>Contact Lee for media inquiries.</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=0,
        max_pages_for_title=2,
        max_pages_for_contact=4,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["BookTitle"] == "A Ritual of Bone"
    assert rows[0]["BookTitleMethod"] in {"jsonld_book", "books_index_card"}
    assert rows[0]["BookTitleConfidence"] == "strong"
    assert rows[0]["BookTitleStatus"] == "ok"


def test_resolve_primary_book_title_prefers_books_page_primary_over_secondary_season_entry() -> None:
    homepage = BeautifulSoup(
        """
        <html><body>
          <h2>Books</h2>
          <a href="/books/the-eye-of-everfell-shadow-battles-saga-book-1">
            <img alt="The Eye of Everfell (Shadow Battles Saga Book 1)" />
          </a>
          <h3>The Eye of Everfell (Shadow Battles Saga Book 1)</h3>
          <a href="/books/the-eye-of-everfell-shadow-battles-saga-book-1">The Eye of Everfell (Shadow Battles Saga Book 1)</a>
          <a href="/books/vigil-inferno-season-cyber-knight-saga-book-2">
            <img alt="Vigil: Inferno Season (Cyber Knight Saga Book 2)" />
          </a>
          <h3>Vigil: Inferno Season (Cyber Knight Saga Book 2)</h3>
          <a href="/books/vigil-inferno-season-cyber-knight-saga-book-2">Vigil: Inferno Season (Cyber Knight Saga Book 2)</a>
          <h2>Series</h2>
          <h3>Shadow Battles</h3>
          <a href="/books/the-eye-of-everfell-shadow-battles-saga-book-1">The Eye of Everfell (Shadow Battles Saga Book 1)</a>
          <a href="/books/the-darkest-champion-shadow-battles-saga-book-2">The Darkest Champion (Shadow Battles Saga Book 2)</a>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://www.knightvisionbooks.com/",
        title_pages=[("https://www.knightvisionbooks.com/", homepage.get_text(" ", strip=True), homepage)],
        source_title="Lewis Knight official website",
        source_snippet="The website of indie fantasy and sci-fi author Lewis Knight. Visit",
        source_url="https://www.epicindie.net/authordirectory",
        author_name="Lewis Knight",
    )

    assert result["title"] == "The Eye of Everfell"
    assert result["method"] == "books_index_card"
    assert result["confidence"] == "strong"
    assert result["status"] == "ok"
    assert int(result["score"]) > 0


def test_resolve_primary_book_title_directory_snippet_match_beats_homepage_h1() -> None:
    homepage = BeautifulSoup(
        """
        <html><body>
          <h1>Jane Doe</h1>
          <h2>Welcome to my author site</h2>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://janedoeauthor.com/",
        title_pages=[("https://janedoeauthor.com/", homepage.get_text(" ", strip=True), homepage)],
        source_title="Jane Doe",
        source_snippet="Jane Doe, author of Skyfall.",
        source_url="https://www.epicindie.net/authordirectory/jane-doe",
        author_name="Jane Doe",
    )

    assert result["title"] == "Skyfall"
    assert result["method"] == "source_snippet"
    assert result["status"] == "missing_or_weak"


def test_resolve_primary_book_title_listing_match_beats_other_titles() -> None:
    homepage = BeautifulSoup(
        """
        <html><body>
          <h2>Books</h2>
          <h3>Vigil: Inferno Season (Cyber Knight Saga Book 2)</h3>
          <a href="/books/vigil-inferno-season-cyber-knight-saga-book-2">Vigil: Inferno Season (Cyber Knight Saga Book 2)</a>
          <h3>The Eye of Everfell (Shadow Battles Saga Book 1)</h3>
          <a href="/books/the-eye-of-everfell-shadow-battles-saga-book-1">The Eye of Everfell (Shadow Battles Saga Book 1)</a>
        </body></html>
        """,
        "html.parser",
    )
    listing_page = BeautifulSoup(
        """
        <html><body>
          <h1>The Eye of Everfell</h1>
          <p>Paperback $14.99 Buy now</p>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://www.knightvisionbooks.com/",
        title_pages=[("https://www.knightvisionbooks.com/", homepage.get_text(" ", strip=True), homepage)],
        source_title="Lewis Knight official website",
        source_snippet="The website of indie fantasy and sci-fi author Lewis Knight. Visit",
        source_url="https://www.epicindie.net/authordirectory",
        listing_url="https://www.amazon.com/dp/B00OUGLXLU",
        listing_status="unverified",
        listing_page=("https://www.amazon.com/dp/B00OUGLXLU", listing_page.get_text(" ", strip=True), listing_page),
        author_name="Lewis Knight",
    )

    assert result["title"] == "The Eye of Everfell"
    assert result["method"] == "listing_title_oracle"
    assert result["confidence"] == "strong"


def test_extract_book_title_from_book_page_accepts_creativework_jsonld() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"CreativeWork","name":"A Ritual of Bone"}
          </script>
          <h1>Lee C. Conley</h1>
        </body></html>
        """,
        "html.parser",
    )

    title, method = extract_book_title_from_book_page(
        "https://www.leeconleyauthor.com/books/a-ritual-of-bone",
        soup,
        soup.get_text(" ", strip=True),
        author_name="Lee C. Conley",
    )

    assert title == "A Ritual of Bone"
    assert method == "jsonld_book"


def test_resolve_primary_book_title_uses_itemlist_and_image_context_book_cards() -> None:
    homepage = BeautifulSoup(
        """
        <html><body>
          <h2>Books</h2>
          <script type="application/ld+json">
            {
              "@context":"https://schema.org",
              "@type":"ItemList",
              "itemListElement":[
                {"@type":"ListItem","position":1,"item":{"@type":"Book","name":"The Glass Crown"}},
                {"@type":"ListItem","position":2,"item":{"@type":"Book","name":"Storm Key"}}
              ]
            }
          </script>
          <article>
            <a href="/books/the-glass-crown"><img src="/covers/glass.jpg" aria-label="The Glass Crown" /></a>
            <figure><figcaption>The Glass Crown</figcaption></figure>
          </article>
          <article>
            <a href="/books/storm-key"><img src="/covers/storm.jpg" title="Storm Key" /></a>
            <figure><figcaption>Storm Key</figcaption></figure>
          </article>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://example.com/",
        title_pages=[("https://example.com/", homepage.get_text(" ", strip=True), homepage)],
        source_title="Jane Doe",
        source_snippet="Jane Doe, indie fantasy author.",
        source_url="https://www.epicindie.net/authordirectory",
        author_name="Jane Doe",
    )

    assert result["title"] in {"The Glass Crown", "Storm Key"}
    assert result["method"] == "books_index_card"
    assert result["confidence"] == "strong"


def test_resolve_primary_book_title_does_not_choose_series_label_when_books_exist() -> None:
    series_page = BeautifulSoup(
        """
        <html><body>
          <h2>Series</h2>
          <h3>Shadow Battles</h3>
          <a href="/series/shadow-battles">Shadow Battles</a>
          <a href="/books/the-eye-of-everfell-shadow-battles-saga-book-1">The Eye of Everfell (Shadow Battles Saga Book 1)</a>
          <a href="/books/the-darkest-champion-shadow-battles-saga-book-2">The Darkest Champion (Shadow Battles Saga Book 2)</a>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://www.knightvisionbooks.com/series/shadow-battles",
        title_pages=[("https://www.knightvisionbooks.com/series/shadow-battles", series_page.get_text(" ", strip=True), series_page)],
        source_title="Lewis Knight official website",
        source_snippet="The website of indie fantasy and sci-fi author Lewis Knight. Visit",
        source_url="https://www.epicindie.net/authordirectory",
        author_name="Lewis Knight",
    )

    assert result["title"] != "Shadow Battles"
    assert result["title"] == "The Eye of Everfell"


def test_resolve_primary_book_title_downgrades_authorish_titles() -> None:
    homepage = BeautifulSoup(
        """
        <html><body>
          <h1>Daan Katz</h1>
          <h2>Daan Katz Author</h2>
        </body></html>
        """,
        "html.parser",
    )

    result = resolve_primary_book_title(
        candidate_url="https://daankatz.com/",
        title_pages=[("https://daankatz.com/", homepage.get_text(" ", strip=True), homepage)],
        source_title="Daan Katz Author",
        source_snippet="Official site",
        source_url="https://www.epicindie.net/authordirectory",
        author_name="Daan Katz",
    )

    assert result["status"] == "missing_or_weak"
    assert result["reject_reason"] in {"matches_author_name", "author_suffix_label", "weak_method", "no_title_candidate"}


def test_validate_candidates_only_records_visible_text_email_on_author_site(tmp_path) -> None:
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["CandidateURL", "SourceQuery", "SourceURL", "SourceTitle", "SourceSnippet"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "CandidateURL": "https://janedoeauthor.com/about",
                "SourceQuery": "epic:author-directory",
                "SourceURL": "https://www.epicindie.net/authordirectory",
                "SourceTitle": "Jane Doe",
                "SourceSnippet": "The website of indie fantasy author Jane Doe.",
            }
        )

    pages = {
        "https://janedoeauthor.com/about": """
            <html><body>
              <h1>Jane Doe</h1>
              <h2>Skyfall</h2>
              <p>Jane Doe is an independently published fantasy author based in Austin, TX.</p>
              <p>New release March 1, 2026.</p>
              <a href="/contact">Contact</a>
              <a href="https://www.amazon.com/dp/B012345678">Buy on Amazon</a>
            </body></html>
        """,
        "https://janedoeauthor.com/contact": """
            <html><body>
              <a href="mailto:hidden@janedoeauthor.com">Email Jane</a>
              <p>Use the contact form for media requests.</p>
            </body></html>
        """,
        "https://janedoeauthor.com": """
            <html><body><p>Official site for Jane Doe.</p></body></html>
        """,
        "https://www.amazon.com/dp/B012345678": """
            <html><body><h1>Skyfall</h1><p>Paperback $14.99 Buy now</p></body></html>
        """,
        "https://www.epicindie.net/authordirectory": """
            <html><body><p>Contact directory@example.com for site support.</p></body></html>
        """,
    }

    args = Namespace(
        input=str(candidates_path),
        output=str(tmp_path / "validated.csv"),
        delay=0.0,
        timeout=5.0,
        min_year=2022,
        max_year=2026,
        max_candidates=0,
        require_email=False,
        require_contact_path=False,
        ignore_robots=False,
        robots_retry_seconds=60.0,
        contact_path_strict=False,
        require_location_proof=False,
        us_only=False,
        max_support_pages=1,
        max_pages_for_title=4,
        max_pages_for_contact=6,
        max_total_fetches_per_domain_per_run=14,
        listing_strict=False,
        stats_output="",
    )

    with (
        patch("prospect_validate.build_session", return_value=object()),
        patch("prospect_validate.fetch_with_meta", side_effect=fake_fetch_with_meta_factory(pages)),
        patch("prospect_validate.time.sleep", return_value=None),
    ):
        rows, reject_counts, total = validate_candidates(args)

    assert total == 1
    assert reject_counts == {}
    assert len(rows) == 1
    assert rows[0]["AuthorEmail"] == "hidden@janedoeauthor.com"
    assert rows[0]["AuthorEmailSourceURL"] == "https://janedoeauthor.com/contact"
    assert rows[0]["EmailQuality"] == "same_domain"
    assert rows[0]["ContactURL"] == "https://janedoeauthor.com/contact"
    assert rows[0]["ContactURLMethod"] == "email_source"
    assert rows[0]["SourceURL"] == "https://www.epicindie.net/authordirectory"
    assert rows[0]["IndieProofURL"] in {
        "https://janedoeauthor.com/about",
        "https://www.epicindie.net/authordirectory",
    }


def test_parse_sitemap_xml_handles_urlset_and_index() -> None:
    urlset_xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://janedoeauthor.com/books/skyfall</loc></url>
          <url><loc>https://janedoeauthor.com/contact</loc></url>
        </urlset>
    """
    index_xml = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://janedoeauthor.com/sitemap-pages.xml</loc></sitemap>
          <sitemap><loc>https://janedoeauthor.com/sitemap-books.xml</loc></sitemap>
        </sitemapindex>
    """

    urls, child_sitemaps = parse_sitemap_xml(urlset_xml)
    index_urls, index_children = parse_sitemap_xml(index_xml)

    assert urls == [
        "https://janedoeauthor.com/books/skyfall",
        "https://janedoeauthor.com/contact",
    ]
    assert child_sitemaps == []
    assert index_urls == []
    assert index_children == [
        "https://janedoeauthor.com/sitemap-pages.xml",
        "https://janedoeauthor.com/sitemap-books.xml",
    ]


def test_collect_sitemap_urls_follows_sitemap_index() -> None:
    responses = {
        "https://janedoeauthor.com/sitemap.xml": (
            """
            <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <sitemap><loc>https://janedoeauthor.com/sitemap-pages.xml</loc></sitemap>
              <sitemap><loc>https://janedoeauthor.com/sitemap-books.xml</loc></sitemap>
            </sitemapindex>
            """,
            "application/xml",
        ),
        "https://janedoeauthor.com/sitemap-pages.xml": (
            """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://janedoeauthor.com/about</loc></url>
              <url><loc>https://janedoeauthor.com/contact</loc></url>
            </urlset>
            """,
            "application/xml",
        ),
        "https://janedoeauthor.com/sitemap-books.xml": (
            """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://janedoeauthor.com/books/skyfall</loc></url>
            </urlset>
            """,
            "application/xml",
        ),
    }

    def fetcher(url: str, expect_html: bool = False) -> FetchResult:
        return make_fetch_result(url, responses.get(url), expect_html=expect_html)

    discovered = collect_sitemap_urls("https://janedoeauthor.com", fetcher, sitemap_cache={})

    assert discovered == [
        "https://janedoeauthor.com/about",
        "https://janedoeauthor.com/contact",
        "https://janedoeauthor.com/books/skyfall",
    ]


def test_homepage_book_page_discovery_prefers_bookish_paths() -> None:
    urls = [
        "https://janedoeauthor.com/about",
        "https://janedoeauthor.com/2017/05/22/book-writing-update-5222017/",
        "https://janedoeauthor.com/works/the-midnight-archive",
        "https://janedoeauthor.com/contact",
        "https://janedoeauthor.com/books/skyfall",
        "https://janedoeauthor.com/series/the-dawn-saga",
    ]

    discovered = discover_book_page_urls("https://janedoeauthor.com", urls, limit=2)

    assert discovered == [
        "https://janedoeauthor.com/books/skyfall",
        "https://janedoeauthor.com/works/the-midnight-archive",
    ]


def test_discover_same_domain_listing_support_urls_uses_anchor_text_and_is_bounded() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <a href="/storefront">Titles</a>
          <a href="/my-works">Works</a>
          <a href="/collection">Books and Buy Links</a>
          <a href="/about">About</a>
        </body></html>
        """,
        "html.parser",
    )

    discovered = discover_same_domain_listing_support_urls(
        "https://janedoeauthor.com/",
        soup,
        limit=2,
        author_name="Jane Doe",
    )

    assert len(discovered) == 2
    assert "https://janedoeauthor.com/collection" in discovered
    assert "https://janedoeauthor.com/about" not in discovered


def test_normalize_book_title_strips_series_parenthetical_suffixes() -> None:
    assert normalize_book_title("The Darkest Champion (Shadow Battles Saga Book 0.5)") == "The Darkest Champion"


def test_extract_book_title_from_book_page_prefers_jsonld_book() -> None:
    soup = BeautifulSoup(
        """
        <html>
          <head>
            <meta property="og:title" content="The Midnight Archive | Jane Doe" />
            <script type="application/ld+json">
              {"@context": "https://schema.org", "@type": "Book", "name": "The Midnight Archive"}
            </script>
          </head>
          <body><h1>Books</h1><h2>The Midnight Archive</h2></body>
        </html>
        """,
        "html.parser",
    )

    title, method = extract_book_title_from_book_page(
        "https://janedoeauthor.com/books/the-midnight-archive",
        soup,
        soup.get_text(" ", strip=True),
        author_name="Jane Doe",
    )

    assert title == "The Midnight Archive"
    assert method == "jsonld_book"


def test_directory_indie_proof_accepts_source_type_fallback() -> None:
    proof_url, proof_snippet, strength = directory_indie_proof(
        source_query="directory-page-2",
        source_type="epic_directory",
        source_url="https://www.epicindie.net/authordirectory/jane-doe",
        source_title="Jane Doe",
        source_snippet="Indie fantasy author Jane Doe",
    )

    assert proof_url == "https://www.epicindie.net/authordirectory/jane-doe"
    assert proof_snippet == "Indie fantasy author Jane Doe"
    assert strength == "directory"


def test_extract_page_email_records_handles_visible_obfuscation_with_proof() -> None:
    html = """
        <html><body>
          <p>For media inquiries email jane (at) janedoeauthor (dot) com.</p>
        </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")

    records = extract_page_email_records(
        source_url="https://janedoeauthor.com/contact",
        text=soup.get_text(" ", strip=True),
        soup=soup,
        author_website="https://janedoeauthor.com",
    )

    assert len(records) == 1
    assert records[0]["email"] == "jane@janedoeauthor.com"
    assert records[0]["quality"] == "same_domain"
    assert "jane" in records[0]["proof_snippet"].lower()
    assert "janedoeauthor" in records[0]["proof_snippet"].lower()


def test_extract_page_email_records_ignores_plain_word_false_positive_without_contact_context() -> None:
    html = """
        <html><body>
          <p>The threat grew terrible at them dot guardians of the gate.</p>
        </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")

    records = extract_page_email_records(
        source_url="https://janedoeauthor.com/about",
        text=soup.get_text(" ", strip=True),
        soup=soup,
        author_website="https://janedoeauthor.com",
    )

    assert records == []


def test_extract_page_email_records_ignores_blog_footer_false_positive() -> None:
    html = """
        <html><body>
          <p>Blog at WordPress.com. Subscribe</p>
        </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")

    records = extract_page_email_records(
        source_url="https://janedoeauthor.com",
        text=soup.get_text(" ", strip=True),
        soup=soup,
        author_website="https://janedoeauthor.com",
    )

    assert records == []


def test_extract_listing_candidates_from_author_page_keeps_allowed_retailers_only() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <a href="https://www.amazon.com/b?node=16571048011">Amazon Category</a>
          <a href="https://www.amazon.com/dp/B012345678">Amazon Listing</a>
          <a href="https://www.target.com/s/book-title">Target Search</a>
          <a href="https://www.barnesandnoble.com/w/example-title/114514">B&N Listing</a>
        </body></html>
        """,
        "html.parser",
    )

    listings = extract_listing_candidates_from_page("https://janedoeauthor.com/books/skyfall", soup)

    assert listings == [
        ("https://www.amazon.com/dp/B012345678", "https://janedoeauthor.com/books/skyfall", "book_page_outbound"),
        (
            "https://www.barnesandnoble.com/w/example-title/114514",
            "https://janedoeauthor.com/books/skyfall",
            "book_page_outbound",
        ),
    ]


def test_extract_listing_candidates_from_page_expands_allowed_shortlinks() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <a href="https://mybook.to/example">Buy Now</a>
          <a href="https://getbook.at/example2">Buy on Amazon</a>
          <a href="https://bookshop.org/books/example">Purchase</a>
        </body></html>
        """,
        "html.parser",
    )

    def expander(url: str) -> str:
        if "mybook.to" in url:
            return "https://www.amazon.com/dp/B012345678?tag=test-20"
        if "getbook.at" in url:
            return "https://www.barnesandnoble.com/w/example-title/114514?ean=114514"
        return ""

    listings = extract_listing_candidates_from_page(
        "https://janedoeauthor.com/other-works",
        soup,
        shortlink_expander=expander,
    )

    assert listings == [
        ("https://www.amazon.com/dp/B012345678", "https://janedoeauthor.com/other-works", "book_page_shortlink"),
        (
            "https://www.barnesandnoble.com/w/example-title/114514",
            "https://janedoeauthor.com/other-works",
            "book_page_shortlink",
        ),
    ]
