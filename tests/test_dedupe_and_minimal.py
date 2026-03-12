from __future__ import annotations

import csv
from pathlib import Path

from prospect_dedupe import dedupe
from run_lead_finder_loop import build_rotating_queries, write_contact_queue_rows, write_minimal_rows


def test_dedupe_by_email_author_or_listing() -> None:
    rows = [
        {"AuthorName": "A One", "BookTitle": "Book 1", "AuthorEmail": "A@EXAMPLE.COM", "ListingURL": "https://amazon.com/dp/1"},
        {"AuthorName": "A Two", "BookTitle": "Book 2", "AuthorEmail": "a@example.com", "ListingURL": "https://amazon.com/dp/2"},
        {"AuthorName": "A One", "BookTitle": "Book 3", "AuthorEmail": "other@example.com", "ListingURL": "https://amazon.com/dp/3"},
        {"AuthorName": "B One", "BookTitle": "Book 4", "AuthorEmail": "b@example.com", "ListingURL": "https://amazon.com/dp/1"},
        {"AuthorName": "C One", "BookTitle": "Book 5", "AuthorEmail": "c@example.com", "ListingURL": "https://amazon.com/dp/5"},
    ]

    out = dedupe(rows)
    assert len(out) == 2
    assert out[0]["AuthorEmail"] == "a@example.com"
    assert out[1]["AuthorEmail"] == "c@example.com"


def test_minimal_output_no_header_default(tmp_path: Path) -> None:
    rows = [
        {"AuthorName": "Alpha", "BookTitle": "Book A", "AuthorEmail": "ALPHA@EXAMPLE.COM"},
        {"AuthorName": "Alpha", "BookTitle": "Book A", "AuthorEmail": "alpha@example.com"},
        {"AuthorName": "Beta", "BookTitle": "Book B", "AuthorEmail": "beta@example.com"},
    ]
    out_file = tmp_path / "minimal.csv"

    count = write_minimal_rows(out_file, rows, with_header=False)
    assert count == 2

    with out_file.open("r", encoding="utf-8", newline="") as fh:
        data = list(csv.reader(fh))
    assert data == [
        ["alpha@example.com", "Alpha", "Book A", "", "", ""],
        ["beta@example.com", "Beta", "Book B", "", "", ""],
    ]


def test_minimal_output_includes_source_urls(tmp_path: Path) -> None:
    rows = [
        {
            "AuthorName": "Gamma",
            "BookTitle": "Book C",
            "AuthorEmail": "gamma@example.com",
            "AuthorEmailSourceURL": "https://author.example.com/contact",
            "AuthorNameSourceURL": "https://author.example.com/about",
            "BookTitleSourceURL": "https://www.amazon.com/dp/ABC1234567",
        }
    ]
    out_file = tmp_path / "minimal_sources.csv"

    count = write_minimal_rows(out_file, rows, with_header=True)
    assert count == 1

    with out_file.open("r", encoding="utf-8", newline="") as fh:
        data = list(csv.reader(fh))
    assert data == [
        ["Email", "AuthorName", "BookTitle", "EmailSourceURL", "AuthorNameSourceURL", "BookTitleSourceURL"],
        [
            "gamma@example.com",
            "Gamma",
            "Book C",
            "https://author.example.com/contact",
            "https://author.example.com/about",
            "https://www.amazon.com/dp/ABC1234567",
        ],
    ]


def test_contact_queue_includes_only_no_email_rows(tmp_path: Path) -> None:
    rows = [
        {
            "AuthorName": "No Email Author",
            "BookTitle": "Book X",
            "AuthorEmail": "",
            "AuthorWebsite": "https://author.example",
            "ContactPageURL": "https://author.example/contact",
            "SubscribeURL": "",
            "Location": "Austin, TX",
            "ListingURL": "https://www.amazon.com/dp/ABC1234567",
            "AuthorNameSourceURL": "https://author.example/about",
            "BookTitleSourceURL": "https://www.amazon.com/dp/ABC1234567",
        },
        {
            "AuthorName": "With Email Author",
            "BookTitle": "Book Y",
            "AuthorEmail": "yes@example.com",
            "AuthorWebsite": "https://with-email.example",
            "ContactPageURL": "https://with-email.example/contact",
        },
    ]
    out_file = tmp_path / "contact_queue.csv"

    count = write_contact_queue_rows(out_file, rows)
    assert count == 1

    with out_file.open("r", encoding="utf-8", newline="") as fh:
        data = list(csv.reader(fh))
    assert len(data) == 2
    assert data[1][0] == "No Email Author"


def test_rotating_queries_are_author_site_focused() -> None:
    queries = build_rotating_queries(1)

    assert any('"indie author" "official website" "contact"' == query for query in queries)
    assert any('"self-published author" "official website"' == query for query in queries)
    assert any('"fantasy" "indie author" "official website"' == query for query in queries)
