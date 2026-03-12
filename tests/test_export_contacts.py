from __future__ import annotations

from export_contacts import (
    canonicalize_export_email,
    canonicalize_export_url,
    resolve_contact_url,
    row_export_score,
    select_contact_rows,
    select_email_rows,
)


def test_resolve_contact_url_priority_prefers_email_source() -> None:
    row = {
        "AuthorEmail": "jane@example.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact?utm_source=test",
        "ContactPageURL": "https://janedoeauthor.com/contact",
        "SubscribeURL": "https://janedoeauthor.com/newsletter",
        "AuthorWebsite": "https://janedoeauthor.com/",
    }

    contact_url, method = resolve_contact_url(row)

    assert contact_url == "https://janedoeauthor.com/contact"
    assert method == "email_source"


def test_resolve_contact_url_uses_press_media_before_website_fallback() -> None:
    row = {
        "PressKitURL": "https://janedoeauthor.com/press-kit/",
        "MediaURL": "https://janedoeauthor.com/media",
        "AuthorWebsite": "https://janedoeauthor.com/",
    }

    contact_url, method = resolve_contact_url(row)

    assert contact_url == "https://janedoeauthor.com/press-kit/"
    assert method == "press_media"


def test_row_export_score_prefers_email_over_contact_page() -> None:
    email_row = {
        "AuthorName": "Jane Doe",
        "AuthorEmail": "jane@example.com",
        "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
        "AuthorWebsite": "https://janedoeauthor.com",
        "Location": "Austin, TX",
        "BookTitleConfidence": "strong",
        "RecencyStatus": "verified",
    }
    contact_row = {
        "AuthorName": "John Doe",
        "ContactPageURL": "https://johndoeauthor.com/contact",
        "AuthorWebsite": "https://johndoeauthor.com",
        "Location": "Austin, TX",
        "BookTitleConfidence": "strong",
        "RecencyStatus": "verified",
    }

    assert row_export_score(email_row) > row_export_score(contact_row)


def test_select_contact_rows_dedupes_author_email_contact_and_listing() -> None:
    rows = [
        {
            "AuthorName": "Jane Doe",
            "BookTitle": "Skyfall",
            "AuthorEmail": "Jane@Example.com",
            "AuthorEmailSourceURL": "https://janedoeauthor.com/contact?utm_source=test",
            "ContactURL": "https://janedoeauthor.com/contact?utm_source=test",
            "ContactURLMethod": "email_source",
            "SourceURL": "https://directory.example/jane-doe",
            "ListingURL": "https://www.amazon.com/dp/B012345678/ref=abc",
            "Location": "Austin, TX",
            "BookTitleConfidence": "strong",
            "RecencyStatus": "verified",
        },
        {
            "AuthorName": "Jane Doe",
            "BookTitle": "Skyfall",
            "AuthorEmail": "jane@example.com",
            "AuthorEmailSourceURL": "https://janedoeauthor.com/contact",
            "ContactURL": "https://janedoeauthor.com/contact/",
            "ContactURLMethod": "email_source",
            "SourceURL": "https://directory.example/jane-doe-duplicate",
            "ListingURL": "https://www.amazon.com/dp/B012345678",
            "Location": "Austin, TX",
            "BookTitleConfidence": "strong",
            "RecencyStatus": "verified",
        },
        {
            "AuthorName": "John Doe",
            "BookTitle": "Nightfall",
            "ContactPageURL": "https://johndoeauthor.com/contact",
            "AuthorWebsite": "https://johndoeauthor.com",
            "SourceURL": "https://directory.example/john-doe",
            "ListingURL": "https://www.amazon.com/dp/B022222222",
        },
    ]

    contact_rows = select_contact_rows(rows, limit=100)
    email_rows = select_email_rows(rows)

    assert contact_rows == [
        ["Jane Doe", "Skyfall", "https://janedoeauthor.com/contact", "https://directory.example/jane-doe"],
        ["John Doe", "Nightfall", "https://johndoeauthor.com/contact", "https://directory.example/john-doe"],
    ]
    assert email_rows == [["Jane Doe", "Skyfall", "jane@example.com", "https://janedoeauthor.com/contact"]]


def test_canonicalization_helpers_strip_tracking_and_scheme() -> None:
    assert canonicalize_export_email(" Jane@Example.COM ") == "jane@example.com"
    assert canonicalize_export_url("https://janedoeauthor.com/contact/?utm_source=test#section") == "janedoeauthor.com/contact"
