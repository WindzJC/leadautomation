from __future__ import annotations

from unittest.mock import patch

import requests
from bs4 import BeautifulSoup

from lead_utils import is_allowed_retailer_url
from prospect_harvest import (
    classify_google_cse_status,
    extract_goodreads_outbound_links,
    extract_ian_directory_candidates,
    extract_iabx_directory_candidates,
    expand_shortlink,
    google_cse_healthcheck,
    harvest,
    harvest_openlibrary_candidates,
    is_candidate_result,
    is_candidate_url,
    is_promotable_author_outbound,
    is_promotable_author_outbound_link,
    is_preferred_candidate_url,
    prioritize_directory_candidates,
    resolve_discovered_url,
    rotate_rows,
    search_brave_web,
    search_bing_rss,
    search_google_cse,
)
from prospect_validate import (
    has_enterprise_signal,
    is_us_location,
    is_listing_domain,
    is_listing_url,
    listing_has_required_signals,
    normalize_listing_url,
)


class FakeResponse:
    def __init__(
        self,
        text: str = "",
        json_data: object | None = None,
        json_error: Exception | None = None,
        url: str = "",
        status_code: int = 200,
    ) -> None:
        self.text = text
        self._json_data = json_data
        self._json_error = json_error
        self.url = url
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.HTTPError(f"{self.status_code} error")
            error.response = self
            raise error
        return None

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._json_data


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, params=None, timeout: float = 0.0, headers=None, allow_redirects: bool = False):  # noqa: ANN001
        self.calls.append((url, timeout))
        if not self._responses:
            raise AssertionError("No more fake responses configured")
        event = self._responses.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event

    def head(self, url: str, timeout: float = 0.0, allow_redirects: bool = False):  # noqa: ANN001
        self.calls.append((url, timeout))
        if not self._responses:
            raise AssertionError("No more fake responses configured")
        event = self._responses.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event


def test_harvest_blocks_noisy_and_irrelevant_hosts() -> None:
    assert not is_candidate_url("https://www.worldbank.org/news/topic/poverty", source_query="indie author")
    assert not is_candidate_url("https://archive-it.org/collections/123", source_query="indie author")
    assert not is_candidate_url("https://app.thestorygraph.com/books/123", source_query="indie author")
    assert not is_candidate_url("https://youtu.be/abc123", source_query="indie author")
    assert not is_candidate_url("https://people.com/some-celebrity-story", source_query="indie author")
    assert not is_candidate_url("https://example.com/home", source_query="")
    assert not is_candidate_url("https://example.com/home", source_query='\"indie author\" \"contact\"')
    assert is_candidate_url("https://example.com/author/jane-doe", source_query="indie author contact")


def test_harvest_search_results_require_author_context_for_root_domains() -> None:
    assert is_candidate_result(
        "https://janedoe.com/",
        title="Jane Doe",
        snippet="Fantasy author and indie novelist",
        source_query='"indie author" "contact"',
    )
    assert not is_candidate_result(
        "https://www.anydesk.com/",
        title="AnyDesk: Remote Desktop Software",
        snippet="Access and control from anywhere",
        source_query='"indie author" "contact"',
    )


def test_bing_rss_malformed_xml_returns_empty_results() -> None:
    session = FakeSession([FakeResponse(text="<rss><channel><item></channel>")])

    results = search_bing_rss("indie author contact", per_query=5, session=session, timeout=5.0)

    assert results == []


def test_google_cse_malformed_json_returns_empty_results() -> None:
    session = FakeSession([FakeResponse(json_error=ValueError("bad json"))])

    results = search_google_cse(
        "indie author contact",
        per_query=5,
        session=session,
        api_key="key",
        cx="cx",
        timeout=5.0,
    )

    assert results == []


def test_brave_search_returns_filtered_candidate_urls() -> None:
    session = FakeSession(
        [
            FakeResponse(
                json_data={
                    "web": {
                        "results": [
                            {
                                "url": "https://janedoeauthor.com/contact",
                                "title": "Jane Doe Author",
                                "description": "Fantasy author website and newsletter",
                            },
                            {
                                "url": "https://www.anydesk.com/",
                                "title": "AnyDesk",
                                "description": "Remote desktop software",
                            },
                        ]
                    }
                }
            )
        ]
    )

    results = search_brave_web(
        "fantasy indie author contact",
        per_query=5,
        session=session,
        api_key="key",
        timeout=5.0,
    )

    assert results == ["https://janedoeauthor.com/contact"]


def test_brave_search_malformed_json_returns_empty_results() -> None:
    session = FakeSession([FakeResponse(json_error=ValueError("bad json"))])

    results = search_brave_web(
        "fantasy indie author contact",
        per_query=5,
        session=session,
        api_key="key",
        timeout=5.0,
    )

    assert results == []


def test_shortlink_expansion_keeps_only_amazon_com_listing_paths() -> None:
    keep_session = FakeSession([FakeResponse(url="https://www.amazon.com/dp/B012345678?ref_=abc")])
    drop_session = FakeSession([FakeResponse(url="https://www.amazon.com/books/example")])

    expanded = expand_shortlink("https://amzn.to/example", session=keep_session, timeout=5.0)
    kept = resolve_discovered_url("https://amzn.to/example", session=FakeSession([FakeResponse(url=expanded)]), timeout=5.0)
    dropped = resolve_discovered_url("https://amzn.to/example", session=drop_session, timeout=5.0)

    assert expanded == "https://www.amazon.com/dp/B012345678"
    assert kept == "https://www.amazon.com/dp/B012345678"
    assert dropped == ""


def test_retailer_rules_reject_category_and_search_urls() -> None:
    assert not is_allowed_retailer_url("https://www.amazon.com/b?node=16571048011")
    assert not is_allowed_retailer_url("https://www.target.com/s/kindles+kindles")
    assert is_allowed_retailer_url("https://www.amazon.com/dp/B012345678")
    assert not is_candidate_url("https://www.amazon.com/b?node=16571048011", source_query="indie author")
    assert not is_candidate_url("https://www.target.com/s/kindles+kindles", source_query="indie author")
    assert is_candidate_url("https://www.amazon.com/dp/B012345678", source_query="indie author")


def test_google_cse_healthcheck_classifies_common_failures() -> None:
    forbidden_session = FakeSession(
        [
            FakeResponse(
                json_data={
                    "error": {
                        "message": "API has not been used",
                        "errors": [{"reason": "accessNotConfigured", "message": "API has not been used"}],
                    }
                },
                status_code=403,
            )
        ]
    )
    invalid_session = FakeSession(
        [
            FakeResponse(
                json_data={
                    "error": {
                        "message": "API key not valid. Please pass a valid API key.",
                        "errors": [{"reason": "keyInvalid", "message": "API key not valid."}],
                    }
                },
                status_code=400,
            )
        ]
    )

    forbidden = google_cse_healthcheck(forbidden_session, api_key="key", cx="cx", timeout=5.0)
    invalid = google_cse_healthcheck(invalid_session, api_key="key", cx="cx", timeout=5.0)

    assert forbidden["status"] == "accessNotConfigured"
    assert forbidden["http_status"] == 403
    assert invalid["status"] == "keyInvalid"
    assert classify_google_cse_status(403, "", "forbidden") == "forbidden"


def test_directory_candidates_are_prioritized_by_bookish_source_context() -> None:
    rows = [
        {
            "CandidateURL": "https://plainauthor.example",
            "SourceTitle": "Plain Author",
            "SourceSnippet": "The website of indie fantasy author Plain Author.",
        },
        {
            "CandidateURL": "https://bookish.example",
            "SourceTitle": "Bookish Author",
            "SourceSnippet": "Bookish Author is the author of The Midnight Archive, Book 1 in a new fantasy series.",
        },
    ]

    ranked = prioritize_directory_candidates(rows)

    assert ranked[0]["CandidateURL"] == "https://bookish.example"


def test_iabx_directory_extracts_external_author_sites_and_skips_retail() -> None:
    session = FakeSession(
        [
            FakeResponse(
                text="""
                <html><head><title>IABX Author Directory</title></head><body>
                  <a href="https://www.iabx.org/author-directory">Authors</a>
                  <a href="https://janedoeauthor.com/">Jane Doe</a>
                  <a href="https://www.amazon.com/dp/B012345678">Retail Row</a>
                  <a href="mailto:jane@example.com">Jane Email</a>
                </body></html>
                """,
                url="https://www.iabx.org/author-directory",
            )
        ]
    )

    rows = extract_iabx_directory_candidates(session=session, timeout=5.0)

    assert len(rows) == 1
    assert rows[0]["CandidateURL"] == "https://janedoeauthor.com/"
    assert rows[0]["SourceType"] == "iabx_directory"


def test_goodreads_outbound_only_promotes_author_contact_like_urls() -> None:
    soup = BeautifulSoup(
        """
        <html><body>
          <a href="https://janedoeauthor.com/">Home</a>
          <a href="https://janedoeauthor.com/contact">Contact</a>
          <a href="https://susanee.com/">Author Website</a>
          <a href="https://janedoeauthor.com/books">Books</a>
          <a href="https://novelnotions.net/">Novel Notions</a>
          <a href="https://www.youtube.com/watch?v=abc123">Video</a>
          <a href="https://betterworldbooks.com/product/123">Retail</a>
        </body></html>
        """,
        "html.parser",
    )

    links = extract_goodreads_outbound_links(
        base_url="https://www.goodreads.com/book/show/123",
        soup=soup,
        limit=10,
        session=object(),  # type: ignore[arg-type]
        timeout=5.0,
    )

    assert links == [
        "https://janedoeauthor.com/",
        "https://janedoeauthor.com/contact",
        "https://susanee.com/",
    ]
    assert is_promotable_author_outbound("https://janedoeauthor.com/")
    assert not is_promotable_author_outbound("https://janedoeauthor.com/books")
    assert not is_promotable_author_outbound("https://novelnotions.net/")
    assert is_promotable_author_outbound_link(
        "https://susanee.com/",
        anchor_text="Author Website",
        context_text="Author Website",
    )
    assert not is_promotable_author_outbound_link(
        "https://novelnotions.net/",
        anchor_text="Novel Notions",
        context_text="Novel Notions",
    )


def test_extract_ian_directory_candidates_fast_fails_after_timeout_streak() -> None:
    session = FakeSession(
        [
            FakeResponse(
                text="""
                <html><body>
                  <a href="/jane-doe.html">Jane Doe</a>
                  <a href="/slow-one.html">Slow One</a>
                  <a href="/slow-two.html">Slow Two</a>
                  <a href="/too-late.html">Too Late</a>
                </body></html>
                """,
                url="https://www.independentauthornetwork.com/author-directory.html",
            ),
                FakeResponse(
                    text="""
                    <html><body>
                      <h1>Jane Doe</h1>
                      <div class="paragraph">Jane Doe is an indie fantasy author of Skyfall and other novels.</div>
                      <a href="https://janedoeauthor.com/">Official Website</a>
                    </body></html>
                    """,
                    url="https://www.independentauthornetwork.com/jane-doe.html",
                ),
            requests.Timeout("slow profile"),
            requests.Timeout("slow profile"),
            FakeResponse(
                text="""
                <html><body>
                  <h1>Should Not Fetch</h1>
                  <div class="paragraph">Indie fantasy author.</div>
                  <a href="https://toolateauthor.com/">Official Website</a>
                </body></html>
                """,
                url="https://www.independentauthornetwork.com/too-late.html",
            ),
        ]
    )

    rows = extract_ian_directory_candidates(
        session=session,
        timeout=12.0,
        rotation_offset=0,
        page_budget=1,
        outbound_per_profile=1,
        max_profiles_per_page=6,
        max_total_profiles=6,
        max_consecutive_profile_failures=2,
        max_profile_timeouts=2,
    )

    assert [row["CandidateURL"] for row in rows] == ["https://janedoeauthor.com/"]
    assert [url for url, _ in session.calls] == [
        "https://www.independentauthornetwork.com/author-directory.html",
        "https://www.independentauthornetwork.com/jane-doe.html",
        "https://www.independentauthornetwork.com/slow-one.html",
        "https://www.independentauthornetwork.com/slow-two.html",
    ]


def test_preferred_candidate_url_requires_book_listing_paths() -> None:
    assert is_preferred_candidate_url("https://www.goodreads.com/book/show/123-example")
    assert is_preferred_candidate_url("https://www.amazon.com/dp/B012345678")
    assert is_preferred_candidate_url("https://www.barnesandnoble.com/w/example/123")
    assert not is_preferred_candidate_url("https://www.amazon.com/")
    assert not is_preferred_candidate_url("https://openlibrary.org/works/OL1W")


def test_rotate_rows_changes_goodreads_starting_point() -> None:
    rows = [
        {"CandidateURL": "https://example.com/one"},
        {"CandidateURL": "https://example.com/two"},
        {"CandidateURL": "https://example.com/three"},
    ]

    rotated = rotate_rows(rows, 1)

    assert [row["CandidateURL"] for row in rotated] == [
        "https://example.com/two",
        "https://example.com/three",
        "https://example.com/one",
    ]


def test_openlibrary_candidates_rotate_subjects_and_emit_external_book_links() -> None:
    with (
        patch(
            "prospect_harvest.search_openlibrary_subject",
            side_effect=[
                [{"work_key": "/works/OL1W"}],
                [{"work_key": "/works/OL2W"}],
                [{"work_key": "/works/OL3W"}],
                [{"work_key": "/works/OL4W"}],
            ],
        ),
        patch(
            "prospect_harvest.extract_openlibrary_candidate_links",
            side_effect=[
                ["https://www.goodreads.com/book/show/1"],
                ["https://www.goodreads.com/book/show/2"],
                ["https://www.amazon.com/dp/B000000003"],
                ["https://www.barnesandnoble.com/w/example/4"],
            ],
        ),
    ):
        rows = harvest_openlibrary_candidates(
            session=object(),  # type: ignore[arg-type]
            per_subject=1,
            timeout=5.0,
            rotation_offset=1,
            max_links_per_work=1,
        )

    assert rows[0]["SourceQuery"] == "openlibrary:subject:science-fiction"
    assert [row["CandidateURL"] for row in rows] == [
        "https://www.goodreads.com/book/show/1",
        "https://www.goodreads.com/book/show/2",
        "https://www.amazon.com/dp/B000000003",
        "https://www.barnesandnoble.com/w/example/4",
    ]


def test_harvest_skips_duckduckgo_after_403_once_directories_fill_gate() -> None:
    ddg_error = requests.HTTPError("403 blocked")
    ddg_error.response = type("Resp", (), {"status_code": 403})()
    epic_rows = [
        {
            "CandidateURL": "https://author-one.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        },
        {
            "CandidateURL": "https://author-two.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        },
        {
            "CandidateURL": "https://author-three.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        },
        {
            "CandidateURL": "https://author-four.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        },
    ]

    with (
        patch("prospect_harvest.extract_epic_directory_candidates", return_value=epic_rows),
        patch("prospect_harvest.extract_iabx_directory_candidates", return_value=[]),
        patch("prospect_harvest.extract_ian_directory_candidates", return_value=[]),
        patch("prospect_harvest.harvest_openlibrary_candidates", return_value=[]),
        patch("prospect_harvest.harvest_goodreads_candidates", return_value=[]),
        patch("prospect_harvest.search_duckduckgo", side_effect=[ddg_error, AssertionError("DDG retried after 403")]),
        patch(
            "prospect_harvest.search_bing_html",
            side_effect=[
                ["https://author.example/one"],
                ["https://author.example/two"],
            ],
        ),
        patch("prospect_harvest.search_bing_rss", return_value=[]),
        patch("prospect_harvest.time.sleep", return_value=None),
    ):
        rows, stats = harvest(
            queries=["q1", "q2"],
            per_query=5,
            target=5,
            min_candidates=5,
            pause_seconds=0.0,
            max_per_domain=0,
            goodreads_pages=1,
            max_goodreads_candidates=1,
            goodreads_outbound_per_url=0,
            include_goodreads_outbound=False,
            goodreads_rotation_offset=1,
            max_openlibrary_candidates=0,
            openlibrary_per_subject=1,
            include_openlibrary=False,
            brave_api_key="",
            google_api_key="",
            google_cx="",
            search_timeout=5.0,
            goodreads_timeout=5.0,
            http_retries=0,
            harvest_time_budget=30.0,
            disable_web_fallback=False,
        )

    assert [row["CandidateURL"] for row in rows] == [
        "https://author-one.example/contact",
        "https://author-two.example/contact",
        "https://author-three.example/contact",
        "https://author-four.example/contact",
        "https://author.example/one",
    ]
    assert stats["per_source"]["epic_directory"] == 4
    assert stats["per_source"]["web_search"] == 1


def test_harvest_uses_directories_before_web_fallbacks() -> None:
    epic_rows = [
        {
            "CandidateURL": f"https://epic{idx}.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        }
        for idx in range(1, 11)
    ]
    ian_rows = [
        {
            "CandidateURL": f"https://ian{idx}.example/contact",
            "SourceType": "ian_directory",
            "SourceQuery": "independent-author-network:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        }
        for idx in range(1, 7)
    ]

    with (
        patch("prospect_harvest.extract_epic_directory_candidates", return_value=epic_rows),
        patch("prospect_harvest.extract_iabx_directory_candidates", return_value=[]),
        patch("prospect_harvest.extract_ian_directory_candidates", return_value=ian_rows),
        patch("prospect_harvest.harvest_openlibrary_candidates", return_value=[]),
        patch("prospect_harvest.harvest_goodreads_candidates", return_value=[]),
        patch(
            "prospect_harvest.search_duckduckgo",
            side_effect=[
                [f"https://search{idx}.example/contact" for idx in range(1, 5)],
            ],
        ),
        patch("prospect_harvest.search_bing_html", return_value=[]),
        patch("prospect_harvest.search_bing_rss", return_value=[]),
        patch("prospect_harvest.time.sleep", return_value=None),
    ):
        rows, stats = harvest(
            queries=["q1"],
            per_query=5,
            target=20,
            min_candidates=20,
            pause_seconds=0.0,
            max_per_domain=0,
            goodreads_pages=1,
            max_goodreads_candidates=0,
            goodreads_outbound_per_url=0,
            include_goodreads_outbound=False,
            goodreads_rotation_offset=0,
            max_openlibrary_candidates=20,
            openlibrary_per_subject=1,
            include_openlibrary=True,
            brave_api_key="",
            google_api_key="",
            google_cx="",
            search_timeout=5.0,
            goodreads_timeout=5.0,
            http_retries=0,
            harvest_time_budget=30.0,
            disable_web_fallback=False,
        )

    assert len(rows) == 20
    assert stats["per_source"]["epic_directory"] == 10
    assert stats["per_source"]["ian_directory"] == 6
    assert stats["per_source"]["web_search"] == 4
    assert all(not row["CandidateURL"].startswith("https://www.amazon.com/b") for row in rows)


def test_harvest_skips_expanded_ian_retry_when_initial_pass_is_empty() -> None:
    epic_rows = [
        {
            "CandidateURL": f"https://epic{idx}.example/contact",
            "SourceType": "epic_directory",
            "SourceQuery": "epic:author-directory",
            "DiscoveredAtUTC": "2026-03-06T00:00:00Z",
        }
        for idx in range(1, 5)
    ]

    with (
        patch("prospect_harvest.extract_epic_directory_candidates", return_value=epic_rows),
        patch("prospect_harvest.extract_iabx_directory_candidates", return_value=[]),
        patch(
            "prospect_harvest.extract_ian_directory_candidates",
            side_effect=[[], AssertionError("expanded IAN retry should be skipped")],
        ),
        patch("prospect_harvest.harvest_openlibrary_candidates", return_value=[]),
        patch("prospect_harvest.harvest_goodreads_candidates", return_value=[]),
        patch("prospect_harvest.search_duckduckgo", return_value=["https://search.example/contact"]),
        patch("prospect_harvest.search_bing_html", return_value=[]),
        patch("prospect_harvest.search_bing_rss", return_value=[]),
        patch("prospect_harvest.time.sleep", return_value=None),
    ):
        rows, stats = harvest(
            queries=["q1"],
            per_query=5,
            target=5,
            min_candidates=5,
            pause_seconds=0.0,
            max_per_domain=0,
            goodreads_pages=1,
            max_goodreads_candidates=0,
            goodreads_outbound_per_url=2,
            include_goodreads_outbound=False,
            goodreads_rotation_offset=0,
            max_openlibrary_candidates=0,
            openlibrary_per_subject=1,
            include_openlibrary=False,
            brave_api_key="",
            google_api_key="",
            google_cx="",
            search_timeout=5.0,
            goodreads_timeout=12.0,
            http_retries=0,
            harvest_time_budget=30.0,
            disable_web_fallback=False,
        )

    assert len(rows) == 5
    assert rows[-1]["CandidateURL"] == "https://search.example/contact"
    assert stats["per_source"]["epic_directory"] == 4
    assert stats["per_source"]["web_search"] == 1


def test_listing_domain_rule_is_amazon_com_or_bn_only() -> None:
    assert is_listing_domain("https://www.amazon.com/dp/B012345678")
    assert is_listing_domain("https://www.barnesandnoble.com/w/book/123")
    assert not is_listing_domain("https://www.amazon.co.uk/dp/B012345678")


def test_regional_amazon_listing_is_not_normalized_into_supported_listing() -> None:
    normalized = normalize_listing_url("https://www.amazon.co.uk/Book-Name/dp/B083Y748QX/ref=sr_1_1")
    assert normalized == "https://www.amazon.co.uk/Book-Name/dp/B083Y748QX/ref=sr_1_1"
    assert not is_listing_url(normalized)


def test_listing_required_signals_strict() -> None:
    ok = "Paperback $14.99 In stock Add to cart"
    missing_price = "Paperback In stock Add to cart"
    missing_buy = "Paperback $14.99"
    assert listing_has_required_signals(ok, strict=True)
    assert not listing_has_required_signals(missing_price, strict=True)
    assert not listing_has_required_signals(missing_buy, strict=True)
    assert listing_has_required_signals(missing_price, strict=False)
    assert listing_has_required_signals(missing_buy, strict=False)


def test_enterprise_followers_threshold_50k() -> None:
    assert has_enterprise_signal("Followed by 60k followers on social media")
    assert not has_enterprise_signal("Followed by 12k followers on social media")


def test_us_location_detection() -> None:
    assert is_us_location("Austin, TX")
    assert is_us_location("Seattle, Washington, United States")
    assert not is_us_location("Toronto, Ontario, Canada")
