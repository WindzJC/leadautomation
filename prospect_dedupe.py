#!/usr/bin/env python3
"""
Stage 3: Deduplicate validated prospects and keep final 20-40.
"""

from __future__ import annotations

import argparse
import csv
import re
from typing import Dict, List

OUTPUT_COLUMNS = [
    "AuthorName",
    "BookTitle",
    "BookTitleMethod",
    "BookTitleScore",
    "BookTitleConfidence",
    "BookTitleStatus",
    "BookTitleRejectReason",
    "BookTitleTopCandidates",
    "AuthorEmail",
    "EmailQuality",
    "AuthorNameSourceURL",
    "BookTitleSourceURL",
    "AuthorEmailSourceURL",
    "AuthorEmailProofSnippet",
    "AuthorWebsite",
    "ContactPageURL",
    "SubscribeURL",
    "PressKitURL",
    "MediaURL",
    "ContactURL",
    "ContactURLMethod",
    "Location",
    "LocationProofURL",
    "LocationProofSnippet",
    "LocationMethod",
    "SourceURL",
    "SourceTitle",
    "SourceSnippet",
    "IndieProofURL",
    "IndieProofSnippet",
    "IndieProofStrength",
    "ListingURL",
    "ListingStatus",
    "ListingFailReason",
    "ListingEnrichedFromURL",
    "ListingEnrichmentMethod",
    "RecencyProofURL",
    "RecencyProofSnippet",
    "RecencyStatus",
    "RecencyFailReason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate validated author prospects.")
    parser.add_argument("--input", default="validated.csv", help="Validated input CSV.")
    parser.add_argument("--output", default="final_prospects.csv", help="Final output CSV.")
    parser.add_argument("--min-final", type=int, default=20, help="Expected minimum final rows.")
    parser.add_argument("--max-final", type=int, default=40, help="Maximum final rows.")
    return parser.parse_args()


def normalize_text(value: str) -> str:
    return re.sub(r"\W+", "", (value or "").lower())


def row_quality_score(row: Dict[str, str]) -> int:
    score = 0
    if (row.get("AuthorEmail", "") or "").strip():
        score += 30
    if (row.get("BookTitleStatus", "ok") or "ok").strip().lower() == "ok" and (row.get("BookTitle", "") or "").strip():
        score += 18
    if (row.get("RecencyStatus", "missing") or "missing").strip().lower() == "verified":
        score += 8

    listing_status = (row.get("ListingStatus", "") or "").strip().lower()
    if listing_status == "verified":
        score += 8
    elif listing_status == "unverified":
        score += 4
    elif listing_status == "missing":
        score += 2

    if (row.get("ContactPageURL", "") or "").strip():
        score += 4
    if (row.get("SubscribeURL", "") or "").strip():
        score += 2
    if (row.get("Location", "") or "").strip() and (row.get("Location", "") or "").strip().lower() != "unknown":
        score += 2

    try:
        score += max(0, int(row.get("BookTitleScore", 0) or 0)) // 10
    except (TypeError, ValueError):
        pass
    return score


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen_email = set()
    seen_author = set()
    seen_listing = set()

    ranked_rows = sorted(
        enumerate(rows),
        key=lambda item: (row_quality_score(item[1]), -item[0]),
        reverse=True,
    )

    for _, row in ranked_rows:
        author = normalize_text(row.get("AuthorName", ""))
        email = (row.get("AuthorEmail", "") or "").strip().lower()
        listing = row.get("ListingURL", "").strip().lower()

        if email and email in seen_email:
            continue
        if author and author in seen_author:
            continue
        if listing and listing in seen_listing:
            continue

        out_row = {col: row.get(col, "") for col in OUTPUT_COLUMNS}
        out_row["AuthorEmail"] = email
        out.append(out_row)

        if email:
            seen_email.add(email)
        if author:
            seen_author.add(author)
        if listing:
            seen_listing.add(listing)

    return out


def read_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def write_rows(path: str, rows: List[Dict[str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    rows = read_rows(args.input)
    deduped = dedupe(rows)

    # Keep earliest rows as-is, capped by max-final.
    if args.max_final > 0:
        deduped = deduped[: args.max_final]

    write_rows(args.output, deduped)
    print(f"[OK] wrote {len(deduped)} rows to {args.output}")
    if len(deduped) < args.min_final:
        print(
            f"[INFO] final count below requested minimum ({len(deduped)}/{args.min_final}). "
            "Run harvest with broader queries and re-validate."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
