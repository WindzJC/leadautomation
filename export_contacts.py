#!/usr/bin/env python3
"""
Export ranked daily outreach files from the contact queue.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from lead_utils import canonical_listing_key, normalize_person_name, strip_tracking_query_params
from pipeline_paths import csv_output, ensure_parent

CONTACT_EXPORT_COLUMNS = ["AuthorName", "BookTitle", "ContactURL", "SourceURL"]
EMAIL_EXPORT_COLUMNS = ["AuthorName", "BookTitle", "AuthorEmail", "EmailSourceURL"]
FETCH_FAILURE_SNIPPETS = ("robots_disallow", "fetch_failed", "page_fetch_failed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export ranked outreach lists from contact_queue.csv.")
    parser.add_argument("--input", default=csv_output("contact_queue.csv"), help="Input queue CSV.")
    parser.add_argument("--limit", type=int, default=100, help="Max rows in contact_100.csv.")
    parser.add_argument("--out", default=csv_output("contact_100.csv"), help="No-header contact export path.")
    parser.add_argument("--out-email", default=csv_output("email_only.csv"), help="No-header email-only export path.")
    return parser.parse_args()


def read_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def clean_output_url(url: str) -> str:
    value = strip_tracking_query_params((url or "").strip())
    if not value:
        return ""
    if not re.match(r"^https?://", value, flags=re.IGNORECASE):
        value = "https://" + value
    parsed = urlparse(value)
    if not parsed.netloc:
        return ""
    return urlunparse(parsed._replace(netloc=parsed.netloc.lower(), fragment=""))


def canonicalize_export_url(url: str) -> str:
    cleaned = clean_output_url(url)
    if not cleaned:
        return ""
    parsed = urlparse(cleaned)
    query = urlencode(parse_qsl(parsed.query, keep_blank_values=True), doseq=True)
    path = parsed.path.rstrip("/")
    if path == "/":
        path = ""
    canonical = f"{parsed.netloc.lower()}{path}"
    if query:
        canonical = f"{canonical}?{query}"
    return canonical


def canonicalize_export_email(email: str) -> str:
    return (email or "").strip().lower()


def resolve_contact_url(row: Dict[str, str]) -> Tuple[str, str]:
    author_email = canonicalize_export_email(row.get("AuthorEmail", ""))
    email_source = clean_output_url(row.get("AuthorEmailSourceURL", ""))
    contact_page = clean_output_url(row.get("ContactPageURL", ""))
    subscribe_url = clean_output_url(row.get("SubscribeURL", ""))
    press_url = clean_output_url(row.get("PressKitURL", ""))
    media_url = clean_output_url(row.get("MediaURL", ""))
    website = clean_output_url(row.get("AuthorWebsite", ""))
    explicit_contact_url = clean_output_url(row.get("ContactURL", ""))
    explicit_method = (row.get("ContactURLMethod", "") or "").strip()

    if author_email and email_source:
        return email_source, "email_source"
    if contact_page and contact_page != website:
        return contact_page, "contact_page"
    if subscribe_url:
        return subscribe_url, "subscribe"
    if press_url:
        return press_url, "press_media"
    if media_url:
        return media_url, "press_media"
    if explicit_contact_url:
        return explicit_contact_url, explicit_method or "website_fallback"
    if website:
        return website, "website_fallback"
    return "", ""


def best_source_url(row: Dict[str, str]) -> str:
    for field in ("SourceURL", "AuthorEmailSourceURL", "ContactPageURL", "AuthorWebsite", "ContactURL"):
        value = clean_output_url(row.get(field, ""))
        if value:
            return value
    return ""


def export_fetch_penalty(row: Dict[str, str]) -> int:
    fetch_status = " ".join(
        [
            (row.get("FetchStatus", "") or "").strip().lower(),
            (row.get("ListingFailReason", "") or "").strip().lower(),
            (row.get("RecencyFailReason", "") or "").strip().lower(),
        ]
    )
    if any(snippet in fetch_status for snippet in FETCH_FAILURE_SNIPPETS):
        return -50
    return 0


def row_export_score(row: Dict[str, str]) -> int:
    contact_url, contact_method = resolve_contact_url(row)
    if not contact_url:
        return -10_000

    score = 0
    if canonicalize_export_email(row.get("AuthorEmail", "")):
        score += 100

    if contact_method == "contact_page":
        score += 60
    elif contact_method in {"subscribe", "press_media"}:
        score += 40

    location = (row.get("Location", "") or "").strip()
    if location and location.lower() != "unknown":
        score += 20
    if (row.get("BookTitleConfidence", "") or "").strip().lower() == "strong":
        score += 20
    if (row.get("RecencyStatus", "") or "").strip().lower() == "verified":
        score += 10

    score += export_fetch_penalty(row)
    return score


def ranked_rows(rows: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    return [
        row
        for _, row in sorted(
            enumerate(rows),
            key=lambda item: (row_export_score(item[1]), -item[0]),
            reverse=True,
        )
    ]


def select_contact_rows(rows: Sequence[Dict[str, str]], limit: int) -> List[List[str]]:
    out_rows: List[List[str]] = []
    seen_authors: set[str] = set()
    seen_emails: set[str] = set()
    seen_contacts: set[str] = set()
    seen_listing_keys: set[str] = set()

    for row in ranked_rows(rows):
        author = (row.get("AuthorName", "") or "").strip()
        if not author:
            continue
        contact_url, _ = resolve_contact_url(row)
        source_url = best_source_url(row)
        if not (contact_url and source_url):
            continue

        author_key = normalize_person_name(author)
        email_key = canonicalize_export_email(row.get("AuthorEmail", ""))
        contact_key = canonicalize_export_url(contact_url)
        listing_key = canonical_listing_key((row.get("ListingURL", "") or "").strip())

        if author_key and author_key in seen_authors:
            continue
        if email_key and email_key in seen_emails:
            continue
        if contact_key and contact_key in seen_contacts:
            continue
        if listing_key and listing_key in seen_listing_keys:
            continue

        out_rows.append(
            [
                author,
                (row.get("BookTitle", "") or "").strip(),
                contact_url,
                source_url,
            ]
        )
        if author_key:
            seen_authors.add(author_key)
        if email_key:
            seen_emails.add(email_key)
        if contact_key:
            seen_contacts.add(contact_key)
        if listing_key:
            seen_listing_keys.add(listing_key)

        if limit > 0 and len(out_rows) >= limit:
            break

    return out_rows


def select_email_rows(rows: Sequence[Dict[str, str]]) -> List[List[str]]:
    out_rows: List[List[str]] = []
    seen_authors: set[str] = set()
    seen_emails: set[str] = set()
    seen_urls: set[str] = set()
    seen_listing_keys: set[str] = set()

    for row in ranked_rows(rows):
        author = (row.get("AuthorName", "") or "").strip()
        email = canonicalize_export_email(row.get("AuthorEmail", ""))
        email_source_url = clean_output_url(row.get("AuthorEmailSourceURL", ""))
        if not (author and email and email_source_url):
            continue

        author_key = normalize_person_name(author)
        url_key = canonicalize_export_url(email_source_url)
        listing_key = canonical_listing_key((row.get("ListingURL", "") or "").strip())

        if author_key and author_key in seen_authors:
            continue
        if email in seen_emails:
            continue
        if url_key and url_key in seen_urls:
            continue
        if listing_key and listing_key in seen_listing_keys:
            continue

        out_rows.append(
            [
                author,
                (row.get("BookTitle", "") or "").strip(),
                email,
                email_source_url,
            ]
        )
        if author_key:
            seen_authors.add(author_key)
        seen_emails.add(email)
        if url_key:
            seen_urls.add(url_key)
        if listing_key:
            seen_listing_keys.add(listing_key)

    return out_rows


def write_rows(path: str, rows: Iterable[Sequence[str]]) -> int:
    count = 0
    ensure_parent(Path(path))
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def main() -> int:
    args = parse_args()
    rows = read_rows(args.input)
    contact_rows = select_contact_rows(rows, max(0, args.limit))
    email_rows = select_email_rows(rows)
    contact_count = write_rows(args.out, contact_rows)
    email_count = write_rows(args.out_email, email_rows)
    print(f"[OK] wrote {contact_count} contact rows -> {args.out}")
    print(f"[OK] wrote {email_count} email rows -> {args.out_email}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
