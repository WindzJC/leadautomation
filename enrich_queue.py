#!/usr/bin/env python3
"""
Re-validate staged queue rows with a higher-budget crawl and emit promotable rows.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

from prospect_dedupe import OUTPUT_COLUMNS
from run_lead_finder_loop import row_qualifies_for_master

ASTRA_OUTBOUND_PROFILE = "astra_outbound"
VERIFIED_NO_US_PROFILE = "verified_no_us"
STRICT_INTERACTIVE_PROFILE = "strict_interactive"
STRICT_FULL_PROFILE = "strict_full"

CANDIDATE_COLUMNS = [
    "CandidateURL",
    "SourceType",
    "SourceQuery",
    "SourceURL",
    "SourceTitle",
    "SourceSnippet",
    "DiscoveredAtUTC",
]


def parse_args() -> argparse.Namespace:
    now_year = dt.datetime.now(dt.UTC).year
    parser = argparse.ArgumentParser(description="Re-enrich staged contact-queue rows and emit promotable rows.")
    parser.add_argument("--input", default="contact_queue.csv", help="Input queue CSV.")
    parser.add_argument("--candidates-output", default="queue_enrichment_candidates.csv", help="Temporary candidate CSV.")
    parser.add_argument("--validated-output", default="queue_enrichment_validated.csv", help="Validated queue output CSV.")
    parser.add_argument("--output", default="promoted.csv", help="Strict-promotable output CSV.")
    parser.add_argument("--max-rows", type=int, default=0, help="Optional cap on staged rows to re-enrich.")
    parser.add_argument("--delay", type=float, default=0.05, help="Validation pause between requests.")
    parser.add_argument("--timeout", type=float, default=8.0, help="HTTP timeout seconds.")
    parser.add_argument("--min-year", type=int, default=now_year - 4, help="Minimum recency year.")
    parser.add_argument("--max-year", type=int, default=now_year, help="Maximum recency year.")
    parser.add_argument("--max-support-pages", type=int, default=1, help="Max supporting pages to fetch per candidate.")
    parser.add_argument("--max-pages-for-title", type=int, default=3, help="Max book pages to fetch per domain.")
    parser.add_argument("--max-pages-for-contact", type=int, default=3, help="Max contact/about pages to fetch per domain.")
    parser.add_argument(
        "--max-total-fetches-per-domain-per-run",
        type=int,
        default=6,
        help="Hard cap on total fetches per domain during validation.",
    )
    parser.add_argument(
        "--max-fetches-per-domain",
        type=int,
        default=0,
        help="Preferred per-domain fetch cap. When unset, falls back to --max-total-fetches-per-domain-per-run.",
    )
    parser.add_argument("--max-seconds-per-domain", type=float, default=25.0, help="Hard cap on network seconds per domain.")
    parser.add_argument("--max-timeouts-per-domain", type=int, default=2, help="Stop fetching a domain after this many timeouts.")
    parser.add_argument("--max-total-runtime", type=float, default=900.0, help="Hard cap on validator wall-clock runtime.")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Stage B concurrency setting forwarded to the validator.")
    parser.add_argument("--ignore-robots", action="store_true", help="Ignore robots.txt checks (not recommended).")
    parser.add_argument(
        "--robots-retry-seconds",
        type=float,
        default=300.0,
        help="Retry window for unreachable/5xx robots.txt states.",
    )
    parser.add_argument("--contact-path-strict", action="store_true", help="Require stronger contact path hints.")
    parser.add_argument("--require-location-proof", action="store_true", help="Keep only leads with explicit location text.")
    parser.add_argument("--us-only", action="store_true", help="Keep only US-based leads.")
    parser.add_argument("--listing-strict", action="store_true", help="Require strict listing verification.")
    parser.add_argument(
        "--validation-profile",
        choices=(
            "default",
            "fully_verified",
            ASTRA_OUTBOUND_PROFILE,
            VERIFIED_NO_US_PROFILE,
            STRICT_INTERACTIVE_PROFILE,
            STRICT_FULL_PROFILE,
        ),
        default="default",
        help="Validation profile forwarded to prospect_validate.py.",
    )
    parser.add_argument("--stats-output", default="enrich_queue_stats.json", help="Optional JSON stats output.")
    args = parser.parse_args()
    profile = str(args.validation_profile or "default").strip().lower()
    args.validation_profile = profile
    if profile == STRICT_INTERACTIVE_PROFILE:
        if int(args.max_fetches_per_domain or 0) == 0:
            args.max_fetches_per_domain = 10
        if float(args.max_seconds_per_domain or 0.0) == 25.0:
            args.max_seconds_per_domain = 12.0
        if float(args.max_total_runtime or 0.0) == 900.0:
            args.max_total_runtime = 120.0
    elif profile == STRICT_FULL_PROFILE:
        if int(args.max_fetches_per_domain or 0) == 0:
            args.max_fetches_per_domain = 16
    return args


def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_candidate_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANDIDATE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows([{col: row.get(col, "") for col in OUTPUT_COLUMNS} for row in rows])


def is_staged_row(row: Dict[str, str]) -> bool:
    book_status = (row.get("BookTitleStatus", "ok") or "ok").strip().lower()
    recency_status = (row.get("RecencyStatus", "verified") or "verified").strip().lower()
    return book_status != "ok" or recency_status != "verified"


def queue_row_candidate_url(row: Dict[str, str]) -> str:
    for field in ("ContactPageURL", "AuthorWebsite", "SourceURL"):
        value = (row.get(field, "") or "").strip()
        if value:
            return value
    return ""


def queue_row_to_candidate(row: Dict[str, str]) -> Dict[str, str]:
    return {
        "CandidateURL": queue_row_candidate_url(row),
        "SourceType": "queue_enrichment",
        "SourceQuery": "queue_enrichment",
        "SourceURL": (row.get("SourceURL", "") or "").strip(),
        "SourceTitle": (row.get("SourceTitle", "") or row.get("AuthorName", "") or "").strip(),
        "SourceSnippet": (row.get("SourceSnippet", "") or "").strip(),
        "DiscoveredAtUTC": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def row_has_books_hint(row: Dict[str, str]) -> bool:
    for field in ("BookTitleSourceURL", "ListingEnrichedFromURL", "SourceURL"):
        value = (row.get(field, "") or "").strip().lower()
        if any(token in value for token in ("/book", "/books", "/bibliography", "/works", "/novels", "/series", "/titles")):
            return True
    return False


def queue_row_priority(row: Dict[str, str]) -> int:
    score = 0
    if (row.get("ListingURL", "") or "").strip():
        score += 50
    if row_has_books_hint(row):
        score += 30
    if (row.get("ContactPageURL", "") or "").strip():
        score += 20
    if any("robots_disallow" in (row.get(field, "") or "").strip().lower() for field in row):
        score -= 50
    return score


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    candidates_path = Path(args.candidates_output)
    validated_path = Path(args.validated_output)
    output_path = Path(args.output)

    queue_rows = read_rows(input_path)
    staged_rows = sorted(
        [row for row in queue_rows if is_staged_row(row)],
        key=queue_row_priority,
        reverse=True,
    )
    if args.max_rows > 0:
        staged_rows = staged_rows[: args.max_rows]

    candidates = [queue_row_to_candidate(row) for row in staged_rows if queue_row_candidate_url(row)]
    write_candidate_rows(candidates_path, candidates)
    if not candidates:
        write_rows(validated_path, [])
        write_rows(output_path, [])
        print(f"[INFO] no staged queue rows to enrich in {input_path}")
        return 0

    started_at = time.monotonic()
    cmd = [
        sys.executable,
        "prospect_validate.py",
        "--input",
        str(candidates_path),
        "--output",
        str(validated_path),
        "--delay",
        str(max(0.0, args.delay)),
        "--timeout",
        str(max(1.0, args.timeout)),
        "--min-year",
        str(args.min_year),
        "--max-year",
        str(args.max_year),
        "--max-support-pages",
        str(max(0, args.max_support_pages)),
        "--max-pages-for-title",
        str(max(0, args.max_pages_for_title)),
        "--max-pages-for-contact",
        str(max(0, args.max_pages_for_contact)),
        "--max-total-fetches-per-domain-per-run",
        str(max(1, args.max_total_fetches_per_domain_per_run)),
        "--max-fetches-per-domain",
        str(max(0, args.max_fetches_per_domain)),
        "--max-seconds-per-domain",
        str(max(0.0, args.max_seconds_per_domain)),
        "--max-timeouts-per-domain",
        str(max(0, args.max_timeouts_per_domain)),
        "--max-total-runtime",
        str(max(0.0, args.max_total_runtime)),
        "--max-concurrency",
        str(max(1, args.max_concurrency)),
        "--validation-profile",
        args.validation_profile,
    ]
    if args.ignore_robots:
        cmd.append("--ignore-robots")
    if args.robots_retry_seconds != 300.0:
        cmd.extend(["--robots-retry-seconds", str(max(10.0, args.robots_retry_seconds))])
    if args.contact_path_strict:
        cmd.append("--contact-path-strict")
    if args.require_location_proof:
        cmd.append("--require-location-proof")
    if args.us_only:
        cmd.append("--us-only")
    if args.listing_strict:
        cmd.append("--listing-strict")

    print(f"[RUN] {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        return result.returncode

    validated_rows = read_rows(validated_path)
    promoted_rows = [
        row for row in validated_rows if row_qualifies_for_master(row, policy="strict", validation_profile=args.validation_profile)
    ]
    write_rows(output_path, promoted_rows)
    elapsed = max(0.0, time.monotonic() - started_at)
    if args.stats_output:
        stats_payload = {
            "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "validation_profile": args.validation_profile,
            "input_rows": len(queue_rows),
            "staged_rows": len(staged_rows),
            "candidate_rows": len(candidates),
            "validated_rows": len(validated_rows),
            "promoted_rows": len(promoted_rows),
            "elapsed_seconds": round(elapsed, 4),
            "selection": [
                {
                    "author": (row.get("AuthorName", "") or "").strip(),
                    "candidate_url": queue_row_candidate_url(row),
                    "priority": queue_row_priority(row),
                }
                for row in staged_rows
            ],
        }
        Path(args.stats_output).write_text(json.dumps(stats_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(
        f"[OK] revalidated {len(candidates)} staged queue rows, "
        f"promoted {len(promoted_rows)} -> {output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
