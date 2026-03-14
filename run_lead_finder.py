#!/usr/bin/env python3
"""
One-command runner for the full lead pipeline:
harvest -> validate -> dedupe.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

SECRET_FLAGS = {"--google-api-key", "--brave-api-key"}
ASTRA_OUTBOUND_PROFILE = "astra_outbound"
VERIFIED_NO_US_PROFILE = "verified_no_us"
STRICT_INTERACTIVE_PROFILE = "strict_interactive"
STRICT_FULL_PROFILE = "strict_full"
AGENT_HUNT_PROFILE = "agent_hunt"


def normalize_validation_profile(value: str) -> str:
    return str(value or "default").strip().lower()


def apply_validation_profile_defaults(args: argparse.Namespace) -> None:
    profile = normalize_validation_profile(getattr(args, "validation_profile", "default"))
    args.validation_profile = profile
    if profile == STRICT_INTERACTIVE_PROFILE:
        if int(getattr(args, "max_fetches_per_domain", 0) or 0) == 0:
            args.max_fetches_per_domain = 10
        if float(getattr(args, "max_seconds_per_domain", 25.0) or 0.0) == 25.0:
            args.max_seconds_per_domain = 12.0
        if float(getattr(args, "max_total_runtime", 900.0) or 0.0) == 900.0:
            args.max_total_runtime = 120.0
        return
    if profile == STRICT_FULL_PROFILE:
        if int(getattr(args, "max_fetches_per_domain", 0) or 0) == 0:
            args.max_fetches_per_domain = 16
        if float(getattr(args, "max_seconds_per_domain", 25.0) or 0.0) == 25.0:
            args.max_seconds_per_domain = 25.0
        if float(getattr(args, "max_total_runtime", 900.0) or 0.0) == 900.0:
            args.max_total_runtime = 900.0
        return
    if profile == AGENT_HUNT_PROFILE:
        return
    if profile != ASTRA_OUTBOUND_PROFILE:
        return

    if int(getattr(args, "target", 40) or 40) == 40:
        args.target = 80
    args.min_candidates = max(80, int(getattr(args, "min_candidates", 80) or 80))
    args.require_location_proof = True
    args.us_only = True
    args.listing_strict = True
    if int(getattr(args, "min_final", 20) or 20) == 20:
        args.min_final = 20
    if int(getattr(args, "max_final", 40) or 40) == 40:
        args.max_final = 40


def parse_args() -> argparse.Namespace:
    max_year_default = dt.datetime.now(dt.UTC).year
    min_year_default = max_year_default - 4
    parser = argparse.ArgumentParser(description="Run the full author lead finder pipeline.")
    parser.add_argument("--target", type=int, default=40, help="Harvest target candidate count.")
    parser.add_argument(
        "--min-candidates",
        type=int,
        default=80,
        help="Minimum harvested candidates to attempt before accepting a shortfall.",
    )
    parser.add_argument("--per-query", type=int, default=30, help="Max results kept per query.")
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=0,
        help="Max candidates per domain to keep source diversity.",
    )
    parser.add_argument("--pause-seconds", type=float, default=1.2, help="Harvest pause between queries.")
    parser.add_argument("--goodreads-pages", type=int, default=3, help="Pages to crawl per Goodreads seed.")
    parser.add_argument(
        "--max-goodreads-candidates",
        type=int,
        default=12,
        help="Maximum candidates taken from Goodreads seeds before web queries.",
    )
    parser.add_argument(
        "--goodreads-outbound-per-url",
        type=int,
        default=2,
        help="Max outbound author/contact URLs to add per Goodreads page.",
    )
    parser.add_argument(
        "--disable-goodreads-outbound",
        action="store_true",
        help="Disable extracting outbound author/contact links from Goodreads pages.",
    )
    parser.add_argument(
        "--goodreads-rotation-offset",
        type=int,
        default=0,
        help="Rotate Goodreads seed candidates by this offset before applying the cap.",
    )
    parser.add_argument(
        "--search-timeout",
        type=float,
        default=10.0,
        help="Search request timeout seconds for harvest stage.",
    )
    parser.add_argument(
        "--goodreads-timeout",
        type=float,
        default=12.0,
        help="Goodreads seed request timeout seconds for harvest stage.",
    )
    parser.add_argument(
        "--harvest-http-retries",
        type=int,
        default=1,
        help="Transient retry count for harvest HTTP requests.",
    )
    parser.add_argument(
        "--harvest-time-budget",
        type=float,
        default=90.0,
        help="Approximate seconds to spend escalating harvest before accepting a shortfall.",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Validation pause between HTTP requests.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds.")
    parser.add_argument("--min-year", type=int, default=min_year_default, help="Minimum recency year.")
    parser.add_argument("--max-year", type=int, default=max_year_default, help="Maximum recency year.")
    parser.add_argument("--max-candidates", type=int, default=0, help="Optional validator candidate cap.")
    parser.add_argument(
        "--max-support-pages",
        type=int,
        default=2,
        help="Max supporting pages to fetch per candidate during validation.",
    )
    parser.add_argument(
        "--max-pages-for-title",
        type=int,
        default=4,
        help="Max sitemap/nav-discovered book pages to fetch per domain during validation.",
    )
    parser.add_argument(
        "--max-pages-for-contact",
        type=int,
        default=6,
        help="Max sitemap/nav-discovered contact/about/newsletter pages to fetch per domain during validation.",
    )
    parser.add_argument(
        "--max-total-fetches-per-domain-per-run",
        type=int,
        default=14,
        help="Hard cap on total fetches per domain during validation.",
    )
    parser.add_argument(
        "--max-fetches-per-domain",
        type=int,
        default=0,
        help="Preferred per-domain fetch cap. When unset, falls back to --max-total-fetches-per-domain-per-run.",
    )
    parser.add_argument(
        "--max-seconds-per-domain",
        type=float,
        default=25.0,
        help="Hard cap on total validator network seconds per domain.",
    )
    parser.add_argument(
        "--max-timeouts-per-domain",
        type=int,
        default=2,
        help="Stop fetching a domain after this many timeout failures.",
    )
    parser.add_argument(
        "--max-total-runtime",
        type=float,
        default=900.0,
        help="Hard cap on validator wall-clock runtime in seconds.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=4,
        help="Stage B concurrency setting forwarded to the validator.",
    )
    parser.add_argument(
        "--location-recovery-mode",
        choices=("off", "same_domain"),
        default="same_domain",
        help="Targeted same-domain recovery mode for strict location near-misses.",
    )
    parser.add_argument(
        "--location-recovery-pages",
        type=int,
        default=6,
        help="Additional same-domain location-support pages to try before recording a strict location near-miss.",
    )
    parser.add_argument("--require-email", action="store_true", help="Keep only leads where AuthorEmail is found.")
    parser.add_argument(
        "--require-location-proof",
        action="store_true",
        help="Keep only leads with explicit location text found.",
    )
    parser.add_argument(
        "--us-only",
        action="store_true",
        help="Keep only US-based leads (from detected location text).",
    )
    parser.add_argument(
        "--require-contact-path",
        action="store_true",
        help="Keep only leads with non-homepage contact/subscribe paths.",
    )
    parser.add_argument(
        "--ignore-robots",
        action="store_true",
        help="Ignore robots.txt checks during validation (not recommended).",
    )
    parser.add_argument(
        "--robots-retry-seconds",
        type=float,
        default=300.0,
        help="Retry window (seconds) for unreachable/5xx robots.txt states.",
    )
    parser.add_argument(
        "--contact-path-strict",
        action="store_true",
        help="Require stronger contact path hints (contact/press/media/support/help).",
    )
    parser.add_argument(
        "--listing-strict",
        action="store_true",
        help="Require format + price + purchase control on listing pages.",
    )
    parser.add_argument(
        "--validation-profile",
        choices=(
            "default",
            "fully_verified",
            AGENT_HUNT_PROFILE,
            ASTRA_OUTBOUND_PROFILE,
            VERIFIED_NO_US_PROFILE,
            STRICT_INTERACTIVE_PROFILE,
            STRICT_FULL_PROFILE,
        ),
        default="default",
        help="Validation profile: default keeps staged rows; fully_verified hard-gates outbound-ready rows; agent_hunt keeps staged validation but is intended for scout-mode ranking/export in the batch loop; astra_outbound applies the Astra strict outbound preset; verified_no_us keeps the strict proof gates but does not require U.S. location; strict_interactive and strict_full keep fully_verified acceptance rules with smaller or larger runtime budgets.",
    )
    parser.add_argument("--min-final", type=int, default=20, help="Expected minimum final rows.")
    parser.add_argument("--max-final", type=int, default=40, help="Maximum final rows.")
    parser.add_argument("--candidates", default="candidates.csv", help="Stage 1 output CSV.")
    parser.add_argument("--validated", default="validated.csv", help="Stage 2 output CSV.")
    parser.add_argument("--final", default="final_prospects.csv", help="Stage 3 output CSV.")
    parser.add_argument(
        "--validate-stats-output",
        default="",
        help="Optional JSON output for validator reject/count stats.",
    )
    parser.add_argument(
        "--near-miss-location-output",
        default="",
        help="Optional CSV for strict rows that fail only on U.S. author location.",
    )
    parser.add_argument("--queries-file", default="", help="Optional custom queries file.")
    parser.add_argument(
        "--google-api-key",
        default=os.getenv("GOOGLE_API_KEY", ""),
        help="Optional Google CSE API key (or env GOOGLE_API_KEY).",
    )
    parser.add_argument(
        "--google-cx",
        default=os.getenv("GOOGLE_CSE_CX", ""),
        help="Optional Google CSE engine ID (or env GOOGLE_CSE_CX).",
    )
    parser.add_argument(
        "--brave-api-key",
        default=os.getenv("BRAVE_SEARCH_API_KEY", ""),
        help="Optional Brave Search API key (or env BRAVE_SEARCH_API_KEY).",
    )
    args = parser.parse_args()
    apply_validation_profile_defaults(args)
    return args


def format_logged_command(cmd: list[str]) -> str:
    redacted: list[str] = []
    skip_next = False
    for idx, token in enumerate(cmd):
        if skip_next:
            skip_next = False
            continue
        if token in SECRET_FLAGS and idx + 1 < len(cmd):
            redacted.extend([token, "<redacted>"])
            skip_next = True
            continue
        redacted.append(token)
    return " ".join(redacted)


def run_command(cmd: list[str]) -> None:
    print(f"[RUN] {format_logged_command(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> int:
    args = parse_args()
    py = sys.executable

    harvest_cmd = [
        py,
        "prospect_harvest.py",
        "--target",
        str(max(1, args.target)),
        "--min-candidates",
        str(max(1, args.min_candidates)),
        "--per-query",
        str(max(1, args.per_query)),
        "--max-per-domain",
        str(max(0, args.max_per_domain)),
        "--pause-seconds",
        str(max(0.0, args.pause_seconds)),
        "--goodreads-pages",
        str(max(1, args.goodreads_pages)),
        "--max-goodreads-candidates",
        str(max(0, args.max_goodreads_candidates)),
        "--goodreads-outbound-per-url",
        str(max(0, args.goodreads_outbound_per_url)),
        "--goodreads-rotation-offset",
        str(max(0, args.goodreads_rotation_offset)),
        "--search-timeout",
        str(max(1.0, args.search_timeout)),
        "--goodreads-timeout",
        str(max(1.0, args.goodreads_timeout)),
        "--http-retries",
        str(max(0, args.harvest_http_retries)),
        "--harvest-time-budget",
        str(max(10.0, args.harvest_time_budget)),
        "--output",
        args.candidates,
    ]
    if args.queries_file:
        harvest_cmd.extend(["--queries-file", args.queries_file])
    if args.disable_goodreads_outbound:
        harvest_cmd.append("--disable-goodreads-outbound")
    if args.brave_api_key:
        harvest_cmd.extend(["--brave-api-key", args.brave_api_key])
    if args.google_api_key:
        harvest_cmd.extend(["--google-api-key", args.google_api_key])
    if args.google_cx:
        harvest_cmd.extend(["--google-cx", args.google_cx])

    validate_cmd = [
        py,
        "prospect_validate.py",
        "--input",
        args.candidates,
        "--output",
        args.validated,
        "--delay",
        str(max(0.0, args.delay)),
        "--timeout",
        str(max(1.0, args.timeout)),
        "--min-year",
        str(args.min_year),
        "--max-year",
        str(args.max_year),
        "--max-candidates",
        str(max(0, args.max_candidates)),
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
        "--location-recovery-mode",
        args.location_recovery_mode,
        "--location-recovery-pages",
        str(max(0, args.location_recovery_pages)),
    ]
    if args.require_email:
        validate_cmd.append("--require-email")
    if args.require_location_proof:
        validate_cmd.append("--require-location-proof")
    if args.us_only:
        validate_cmd.append("--us-only")
    if args.require_contact_path:
        validate_cmd.append("--require-contact-path")
    if args.ignore_robots:
        validate_cmd.append("--ignore-robots")
    if args.robots_retry_seconds != 300.0:
        validate_cmd.extend(["--robots-retry-seconds", str(max(10.0, args.robots_retry_seconds))])
    if args.contact_path_strict:
        validate_cmd.append("--contact-path-strict")
    if args.listing_strict:
        validate_cmd.append("--listing-strict")
    if args.validation_profile:
        validate_cmd.extend(["--validation-profile", args.validation_profile])
    if args.validate_stats_output:
        validate_cmd.extend(["--stats-output", args.validate_stats_output])
    if args.near_miss_location_output:
        validate_cmd.extend(["--near-miss-location-output", args.near_miss_location_output])

    dedupe_cmd = [
        py,
        "prospect_dedupe.py",
        "--input",
        args.validated,
        "--output",
        args.final,
        "--min-final",
        str(max(0, args.min_final)),
        "--max-final",
        str(max(1, args.max_final)),
    ]

    run_command(harvest_cmd)
    run_command(validate_cmd)
    run_command(dedupe_cmd)

    final_path = Path(args.final)
    if final_path.exists():
        print(f"[OK] pipeline complete -> {final_path}")
    else:
        print(f"[WARN] pipeline finished but missing expected output: {final_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
