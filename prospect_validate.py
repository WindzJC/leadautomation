#!/usr/bin/env python3
"""
Stage 2: Crawl candidate pages and enforce qualification rules.
Keeps only records with proof-backed indie/self-pub signals.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
import urllib.robotparser
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lead_utils import (
    canonical_listing_key,
    domain_matches_blocklist,
    is_allowed_retailer_url,
    is_retailer_url,
    normalize_person_name,
    registrable_domain,
    strip_tracking_query_params,
    url_matches_blocklist,
)
from pipeline_paths import csv_output, ensure_parent

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
NEAR_MISS_LOCATION_COLUMNS = [
    "AuthorName",
    "BookTitle",
    "AuthorEmail",
    "SourceURL",
    "RejectReason",
    "BestLocationSnippet",
    "CandidateRecoveryURLs",
]
ASTRA_OUTBOUND_PROFILE = "astra_outbound"
VERIFIED_NO_US_PROFILE = "verified_no_us"
STRICT_INTERACTIVE_PROFILE = "strict_interactive"
STRICT_FULL_PROFILE = "strict_full"
AGENT_HUNT_PROFILE = "agent_hunt"
EMAIL_ONLY_PROFILE = "email_only"
EMAIL_ONLY_LIGHTWEIGHT_RUNTIME = {
    "listing_strict": False,
    "max_fetches_per_domain": 6,
    "max_seconds_per_domain": 8.0,
    "max_total_runtime": 90.0,
    "max_pages_for_title": 1,
    "max_pages_for_contact": 2,
    "max_total_fetches_per_domain_per_run": 6,
    "location_recovery_mode": "off",
    "location_recovery_pages": 0,
}
ASTRA_RULE_VISIBLE_PUBLIC_EMAIL_ONLY = "visible_public_email_only"
ASTRA_RULE_US_LOCATION_PROOF_REQUIRED = "u_s_location_proof_required"
ASTRA_RULE_INDIE_SELF_PUB_PROOF_REQUIRED = "indie_self_pub_proof_required"
ASTRA_RULE_LISTING_PROOF_REQUIRED = "amazon_or_barnes_noble_listing_proof_required_with_visible_purchase_evidence"
ASTRA_RULE_NON_FAMOUS_FILTER_REQUIRED = "non_famous_filter_required"
ASTRA_RULE_RECENCY_PROOF_REQUIRED = "recency_proof_required"
ASTRA_RULE_STRICT_DEDUPE_REQUIRED = "strict_dedupe_by_author_email_and_normalized_listing_url"
ASTRA_RULE_BEST_SOURCE_URL_REQUIRED = "best_source_url_required"
ASTRA_RULE_KEYS = (
    ASTRA_RULE_VISIBLE_PUBLIC_EMAIL_ONLY,
    ASTRA_RULE_US_LOCATION_PROOF_REQUIRED,
    ASTRA_RULE_INDIE_SELF_PUB_PROOF_REQUIRED,
    ASTRA_RULE_LISTING_PROOF_REQUIRED,
    ASTRA_RULE_NON_FAMOUS_FILTER_REQUIRED,
    ASTRA_RULE_RECENCY_PROOF_REQUIRED,
    ASTRA_RULE_STRICT_DEDUPE_REQUIRED,
    ASTRA_RULE_BEST_SOURCE_URL_REQUIRED,
)
STRICT_VERIFIED_PROFILES = {
    "fully_verified",
    ASTRA_OUTBOUND_PROFILE,
    VERIFIED_NO_US_PROFILE,
    STRICT_INTERACTIVE_PROFILE,
    STRICT_FULL_PROFILE,
}
US_STRICT_VERIFIED_PROFILES = {"fully_verified", ASTRA_OUTBOUND_PROFILE, STRICT_INTERACTIVE_PROFILE, STRICT_FULL_PROFILE}
LOCATION_REJECT_REASONS = {
    "no_location_signal",
    "location_ambiguous",
    "publisher_location_only",
    "non_us_location",
    "weak_us_signal",
}


def normalize_validation_profile(value: str) -> str:
    return str(value or "default").strip().lower()


def is_astra_outbound_profile(value: str) -> bool:
    return normalize_validation_profile(value) == ASTRA_OUTBOUND_PROFILE


def cli_flag_present(args: argparse.Namespace, *flags: str) -> bool:
    cli_flags = set(getattr(args, "_cli_flags", ()) or ())
    return any(flag in cli_flags for flag in flags)


def apply_profile_setting(args: argparse.Namespace, attribute: str, value: object, *flags: str) -> None:
    if cli_flag_present(args, *flags):
        return
    setattr(args, attribute, value)


def apply_validation_profile_defaults(args: argparse.Namespace) -> None:
    profile = normalize_validation_profile(getattr(args, "validation_profile", "default"))
    args.validation_profile = profile
    if profile == EMAIL_ONLY_PROFILE:
        apply_profile_setting(args, "listing_strict", EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["listing_strict"], "--listing-strict")
        apply_profile_setting(
            args,
            "max_fetches_per_domain",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_fetches_per_domain"],
            "--max-fetches-per-domain",
        )
        apply_profile_setting(
            args,
            "max_seconds_per_domain",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_seconds_per_domain"],
            "--max-seconds-per-domain",
        )
        apply_profile_setting(
            args,
            "max_total_runtime",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_total_runtime"],
            "--max-total-runtime",
        )
        apply_profile_setting(
            args,
            "max_pages_for_title",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_pages_for_title"],
            "--max-pages-for-title",
        )
        apply_profile_setting(
            args,
            "max_pages_for_contact",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_pages_for_contact"],
            "--max-pages-for-contact",
        )
        apply_profile_setting(
            args,
            "max_total_fetches_per_domain_per_run",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["max_total_fetches_per_domain_per_run"],
            "--max-total-fetches-per-domain-per-run",
        )
        apply_profile_setting(
            args,
            "location_recovery_mode",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["location_recovery_mode"],
            "--location-recovery-mode",
        )
        apply_profile_setting(
            args,
            "location_recovery_pages",
            EMAIL_ONLY_LIGHTWEIGHT_RUNTIME["location_recovery_pages"],
            "--location-recovery-pages",
        )
        return
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
    if profile != ASTRA_OUTBOUND_PROFILE:
        return
    apply_profile_setting(args, "require_location_proof", True, "--require-location-proof")
    apply_profile_setting(args, "us_only", True, "--us-only")
    apply_profile_setting(args, "listing_strict", True, "--listing-strict")


@dataclass
class FetchResult:
    url: str
    final_url: str = ""
    text: str = ""
    soup: Optional[BeautifulSoup] = None
    content_type: str = ""
    failure_reason: str = ""
    status_code: int = 0

    @property
    def ok(self) -> bool:
        return bool(self.text) and self.failure_reason == ""


@dataclass
class TitleCandidate:
    title: str
    source_url: str
    method: str
    page_kind: str = ""
    position: int = 0
    raw_title: str = ""


@dataclass
class DomainMetrics:
    fetch_count: int = 0
    total_seconds: float = 0.0
    timeout_count: int = 0
    prefilter_seconds: float = 0.0
    verify_seconds: float = 0.0
    failures: List[Dict[str, object]] = field(default_factory=list)


@dataclass
class CandidateBudgetMetrics:
    candidate_url: str
    candidate_domain: str
    source_type: str = ""
    source_query: str = ""
    source_url: str = ""
    source_title: str = ""
    source_snippet: str = ""
    query_original: str = ""
    query_effective: str = ""
    widen_level: int = 0
    widen_reason: str = ""
    stale_run_counter: int = 0
    source_family: str = ""
    source_score: float = 0.0
    cross_run_score: float = 0.0
    author_name: str = ""
    book_title: str = ""
    fetch_count: int = 0
    total_seconds: float = 0.0
    prefilter_seconds: float = 0.0
    verify_seconds: float = 0.0
    reject_reason: str = ""
    kept: bool = False
    location_recovery_attempted: bool = False
    listing_recovery_attempted: bool = False
    location_recovery_skipped_reason: str = ""
    listing_recovery_skipped_reason: str = ""
    current_state: str = "harvested"
    validation_state: str = "harvested"
    next_action: str = ""
    next_action_reason: str = ""
    last_attempted_action: str = ""
    email: str = ""
    email_source_url: str = ""
    email_snippet: str = ""
    location_source_url: str = ""
    location_value: str = ""
    us_snippet: str = ""
    indie_proof_url: str = ""
    indie_proof_value: str = ""
    indie_snippet: str = ""
    retailer_listing_url: str = ""
    retailer_listing_status: str = ""
    listing_snippet: str = ""
    recency_source_url: str = ""
    recency_value: str = ""
    book_url: str = ""
    best_source_url: str = ""
    normalized_listing_url: str = ""
    astra_rule_checks: Dict[str, object] = field(default_factory=dict)


@dataclass
class CandidateProofLedger:
    candidate_url: str = ""
    candidate_domain: str = ""
    current_state: str = "harvested"
    last_attempted_action: str = ""
    best_email_snippet: str = ""
    best_email_source_url: str = ""
    best_location_snippet: str = ""
    best_location_source_url: str = ""
    best_listing_snippet: str = ""
    best_listing_source_url: str = ""
    best_recency_snippet: str = ""
    best_recency_source_url: str = ""
    best_indie_snippet: str = ""
    best_indie_source_url: str = ""
    reject_reason_if_any: str = ""
    action_history: List[str] = field(default_factory=list)
    astra_rule_checks: Dict[str, object] = field(default_factory=dict)


CANDIDATE_WORKING_STATES = {
    "harvested",
    "triage_ready",
    "needs_email_proof",
    "needs_location_proof",
    "needs_listing_proof",
    "needs_recency_proof",
    "dead_end",
    "verified",
}
PLANNER_ACTIONS = {
    "try_email_proof",
    "try_location_proof",
    "try_listing_proof",
    "try_recency_proof",
    "stop_dead_end",
    "mark_verified",
}
DEAD_END_REASONS = {
    "definitive_non_us_location",
    "non_us_location",
    "bad_author_name",
}
EMAIL_REJECT_REASONS = {
    "no_visible_author_email",
    "no_author_email",
    "weak_strict_email_quality",
}
LOCATION_REJECT_PLANNER_REASONS = {
    "publisher_location_only",
    "location_ambiguous",
    "weak_us_signal",
    "no_location_signal",
}
LISTING_REJECT_PREFIXES = ("listing_",)
RECENCY_REJECT_REASONS = {"no_recency_proof"}

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

INDIE_KEYWORDS = [
    "indie",
    "independently published",
    "self-published",
    "self published",
    "kdp",
    "kindle direct publishing",
    "ingramspark",
    "draft2digital",
    "barnes & noble press",
    "b&n press",
    "bookbaby",
    "smashwords",
]

ENTERPRISE_SIGNALS = [
    "penguin random house",
    "harpercollins",
    "simon & schuster",
    "hachette",
    "macmillan",
    "new york times bestseller",
    "nyt bestseller",
    "usa today bestseller",
    "wall street journal bestseller",
    "wsj bestseller",
    "big five publisher",
    "imprint of",
]

LOCATION_PATTERNS = [
    r"\bbased in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\blocated in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\blives in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\bresides in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\bliving in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\bborn in\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\bauthor from\s+([A-Z][A-Za-z .,'-]{2,60})",
    r"\b(?:novelist|writer)\s+in\s+([A-Z][A-Za-z .,'-]{2,60})",
]

US_STATE_CODES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

US_STATE_NAMES = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "district of columbia",
}

SOCIAL_HOST_SNIPPETS = (
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",
    "linkedin.com",
    "pinterest.com",
)

NON_AUTHOR_SITE_SNIPPETS = (
    "wikipedia.org",
    "bookdepository.",
    "blackwells.",
    "kickstarter.com",
    "bookshop.org",
    "itunes.apple.com",
    "apps.apple.com",
    "play.google.com",
    "goodreads.com",
    "imdb.com",
    "amazonads.",
    "advertising.amazon.",
)

PRICE_RE = re.compile(r"([$£€]\s?\d{1,4}(?:[.,]\d{2})?)")
FORMAT_HINTS = (
    "paperback",
    "hardcover",
    "kindle",
    "ebook",
    "audiobook",
    "mass market",
)
BUY_HINTS = (
    "buy now",
    "add to cart",
    "in stock",
    "pre-order",
    "preorder",
    "ships from",
    "available now",
    "purchase",
    "buy on amazon",
    "buy on barnes",
    "get a copy",
)
LISTING_UNAVAILABLE_HINTS = (
    "currently unavailable",
    "temporarily out of stock",
    "out of stock",
    "sold out",
    "unavailable",
)
LISTING_RESELLER_HINTS = (
    "other sellers",
    "other sellers on amazon",
    "all buying options",
    "used from",
    "new from",
    "shop used",
    "shop new",
    "sold by ",
)
LISTING_BLOCKED_HINTS = (
    "robot check",
    "captcha",
    "access denied",
    "verify you are a human",
    "automated access",
)
LISTING_INTERSTITIAL_HINTS = (
    "continue shopping",
    "conditions of use",
    "privacy policy",
    "amazon.com, inc. or its affiliates",
)
LISTING_REJECT_REASONS = {
    "listing_not_found",
    "listing_title_mismatch",
    "listing_no_price",
    "listing_no_buy_control",
    "listing_unavailable",
    "listing_reseller_only",
    "listing_interstitial_or_redirect_state",
    "listing_amazon_interstitial_no_bn_candidate",
    "listing_amazon_interstitial_bn_blocked",
    "listing_amazon_interstitial_bn_failed_other",
    "listing_parse_empty",
    "listing_incomplete_product_state",
    "listing_wrong_format",
    "listing_wrong_format_or_incomplete",
    "listing_fetch_failed_timeout",
    "listing_fetch_failed_404",
    "listing_fetch_failed_non_html",
    "listing_fetch_failed_other",
    "listing_fetch_failed",
    "listing_redirect_or_blocked",
}
LISTING_SHORTLINK_HOSTS = ("mybook.to", "getbook.at")
LISTING_FAILURE_PRIORITY = {
    "listing_title_mismatch": 90,
    "listing_no_price": 80,
    "listing_no_buy_control": 75,
    "listing_unavailable": 70,
    "listing_reseller_only": 68,
    "listing_amazon_interstitial_bn_blocked": 68,
    "listing_amazon_interstitial_bn_failed_other": 68,
    "listing_amazon_interstitial_no_bn_candidate": 68,
    "listing_interstitial_or_redirect_state": 67,
    "listing_parse_empty": 66,
    "listing_incomplete_product_state": 65,
    "listing_wrong_format": 64,
    "listing_wrong_format_or_incomplete": 60,
    "listing_redirect_or_blocked": 50,
    "listing_fetch_failed_timeout": 45,
    "listing_fetch_failed_404": 44,
    "listing_fetch_failed_non_html": 43,
    "listing_fetch_failed_other": 42,
    "listing_fetch_failed": 40,
    "listing_not_found": 30,
    "listing_already_seen": 20,
    "listing_not_fetched": 10,
}

RECENCY_DATE_PATTERNS = [
    re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b"),
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b"),
    re.compile(
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{1,2}),\s*(20\d{2})\b",
        re.IGNORECASE,
    ),
]

MONTH_MAP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

SUPPORT_PAGE_BLOCK_HINTS = (
    "/tag/",
    "/category/",
    "/search",
    "/wp-json",
    "/feed",
    "/comments",
    "/about/us",
    "/about/terms",
    "/about/privacy",
    "/about/careers",
)
BOOK_PAGE_PATH_HINTS = (
    "/book",
    "/books",
    "/series",
    "/bibliography",
    "/works",
    "/novels",
    "/titles",
)
STORE_LISTING_PAGE_PATH_HINTS = (
    "/shop",
    "/store",
    "/product-page",
    "/products",
)
LISTING_RECOVERY_TEXT_HINTS = (
    "books",
    "book",
    "novels",
    "titles",
    "works",
    "bibliography",
    "series",
    "buy",
    "amazon",
    "barnes",
    "noble",
)
CONTACT_PAGE_PATH_HINTS = (
    "/contact",
    "/about",
    "/about-author",
    "/about-the-author",
    "/author",
    "/bio",
    "/press",
    "/media",
    "/newsletter",
    "/subscribe",
)
LOCATION_PAGE_PATH_HINTS = (
    "/about",
    "/about-author",
    "/about-the-author",
    "/author",
    "/bio",
    "/contact",
    "/media",
    "/press",
)
PRESS_PAGE_HINTS = ("/press", "/press-kit", "/presskit")
MEDIA_PAGE_HINTS = ("/media", "/media-kit", "/mediakit")
DIRECTORY_SOURCE_HINTS = (
    "epic:author-directory",
    "independent-author-network:author-directory",
    "iabx:author-directory",
    "indieview:author-directory",
)
LOCATION_DECISION_PRIORITY = {
    "ok": 100,
    "non_us_location": 90,
    "publisher_location_only": 80,
    "location_ambiguous": 70,
    "weak_us_signal": 60,
    "no_location_signal": 10,
}
STRONG_LOCATION_METHODS = {"text_phrase", "footer", "jsonld", "directory"}
PUBLISHER_LOCATION_CONTEXT_HINTS = (
    "publisher",
    "publishing",
    "published by",
    "imprint",
    "press",
    "customer service",
    "support",
    "headquartered",
    "our office",
    "contact us",
)
AUTHOR_LOCATION_CONTEXT_HINTS = (
    "author",
    "writer",
    "novelist",
    "poet",
    "storyteller",
    "based in",
    "located in",
    "lives in",
    "resides in",
    "living in",
    "author from",
    "born in",
)
AMBIGUOUS_LOCATION_TERMS = (
    "pacific northwest",
    "midwest",
    "southwest",
    "southeast",
    "northeast",
    "west coast",
    "east coast",
    "worldwide",
    "global",
    "online",
    "virtual",
)
NON_US_LOCATION_TERMS = (
    "canada",
    "united kingdom",
    "england",
    "scotland",
    "wales",
    "ireland",
    "australia",
    "new zealand",
    "philippines",
    "singapore",
    "india",
    "germany",
    "france",
    "spain",
    "italy",
    "netherlands",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "japan",
    "china",
    "hong kong",
    "taiwan",
    "mexico",
    "brazil",
    "argentina",
    "chile",
    "colombia",
    "south africa",
    "nigeria",
    "kenya",
)
BIZ_LOCATION_CONTEXT_HINTS = (
    "suite",
    "street",
    "avenue",
    "road",
    "boulevard",
    "floor",
    "hours",
    "open daily",
    "bookstore",
    "shop",
    "store",
    "tickets",
    "festival",
    "conference",
    "event",
    "venue",
)
AUTHOR_TIED_CITY_STATE_RE = re.compile(
    r"\b([A-Z][A-Za-z .'-]{1,40},\s*(?:%s|%s)(?:,\s*(?:USA|United States))?)\b"
    % (
        "|".join(sorted(US_STATE_CODES)),
        "|".join(re.escape(name.title()) for name in sorted(US_STATE_NAMES, key=len, reverse=True)),
    )
)
STRONG_AUTHOR_US_LOCATION_PATTERNS = [
    re.compile(r"\bi\s+live\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\bi\s+am\s+based\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\bi'?m\s+based\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\bi\s+reside\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\bi\s+am\s+from\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\bi'?m\s+from\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\b[a-z][a-z .'-]{1,40}\s+lives(?:\s+just\s+outside\s+of|\s+outside\s+of|\s+in)\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\b[a-z][a-z .'-]{1,40}\s+is\s+based\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\b[a-z][a-z .'-]{1,40}\s+resides\s+in\s+([^.;|]{2,100})", re.IGNORECASE),
    re.compile(r"\b[a-z][a-z .'-]{1,40}\s+is\s+from\s+([^.;|]{2,100})", re.IGNORECASE),
]
BROAD_LOCATION_PATTERNS = [
    re.compile(r"\bbased in\s+([^.;|]{2,80})", re.IGNORECASE),
    re.compile(r"\blocated in\s+([^.;|]{2,80})", re.IGNORECASE),
    re.compile(r"\blives in\s+([^.;|]{2,80})", re.IGNORECASE),
    re.compile(r"\bborn in\s+([^.;|]{2,80})", re.IGNORECASE),
    re.compile(r"\bfrom\s+([^.;|]{2,80})", re.IGNORECASE),
]

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,24}\b")
OBFUSCATED_EMAIL_RE = re.compile(
    r"\b([a-zA-Z0-9._%+\-]{1,64})\s*(?:@|\(at\)|\[at\]|\bat\b)\s*"
    r"([a-zA-Z0-9.\-]{1,190})\s*(?:\.|\(dot\)|\[dot\]|\bdot\b)\s*([A-Za-z]{2,24})\b",
    flags=re.IGNORECASE,
)

BLOCKED_EMAIL_LOCALPART_SNIPPETS = (
    "abuse",
    "postmaster",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "example",
    "yourname",
    "username",
)

BLOCKED_EMAIL_DOMAIN_SNIPPETS = (
    "goodreads.com",
    "amazon.com",
    "barnesandnoble.com",
    "kdp.amazon.com",
    "kdpcommunity.com",
)

BLOCKED_EMAIL_DOMAIN_PREFIXES = ("www.",)
BLOCKED_EMAIL_TLDS = {
    "and",
    "but",
    "the",
    "for",
    "with",
    "from",
    "this",
    "that",
    "your",
    "their",
    "there",
}

PERSONAL_EMAIL_DOMAINS = {
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "protonmail.com",
    "aol.com",
    "gmx.com",
    "mail.com",
}
EMAIL_CONTEXT_BUSINESS_HINTS = (
    "business",
    "contact",
    "email",
    "for inquiries",
    "media",
    "press",
    "publicity",
)
EMAIL_OBFUSCATION_CONTEXT_HINTS = EMAIL_CONTEXT_BUSINESS_HINTS + (
    "reach",
    "write",
    "mail",
)
ROLE_RISKY_LOCALPARTS = ("contact", "hello", "info")

CONTACT_PATH_HINTS = (
    "contact",
    "about",
    "press",
    "media",
    "author",
    "bio",
)

CONTACT_PATH_HINTS_STRICT = (
    "contact",
    "press",
    "media",
    "support",
    "help",
)

SUBSCRIBE_PATH_HINTS = (
    "subscribe",
    "newsletter",
    "mailing-list",
    "mailing_list",
    "join",
    "updates",
)

SUBSCRIBE_BLOCK_HINTS = (
    "rss",
    "feed",
    "/wp-json",
    "/tag/",
    "/category/",
)

BAD_AUTHOR_NAME_SNIPPETS = (
    "advanced search",
    "search results",
    "privacy policy",
    "terms of use",
    "terms of service",
    "customer reviews",
    "official site",
    "imdb",
    "advertising",
    "get in touch",
    "graphic design",
    "web design",
    "short story anthologies",
)

BAD_AUTHOR_NAME_TOKENS = {
    "advanced",
    "search",
    "results",
    "imdb",
    "amazon",
    "advertising",
    "official",
    "website",
    "author",
    "book",
    "books",
    "press",
    "media",
    "support",
    "help",
    "contact",
    "privacy",
    "terms",
    "home",
    "news",
    "websites",
    "graphic",
    "design",
    "anthology",
    "anthologies",
    "email",
    "inquiries",
}

AUTHOR_BRAND_NAME_TOKENS = {
    "author",
    "authors",
    "blog",
    "blogs",
    "book",
    "books",
    "bookish",
    "chapter",
    "chapters",
    "fiction",
    "library",
    "lit",
    "literary",
    "literature",
    "notes",
    "notions",
    "novel",
    "novels",
    "page",
    "pages",
    "reader",
    "readers",
    "reading",
    "review",
    "reviews",
    "shelf",
    "shelves",
    "story",
    "stories",
    "writer",
    "writers",
    "writing",
}

NON_AUTHOR_EDITORIAL_TEXT_HINTS = (
    "author interviews",
    "book blog",
    "book review",
    "book reviews",
    "book review blog",
    "book reviewer",
    "book reviewers",
    "bookish blog",
    "cover reveal",
    "cover reveals",
    "editorial team",
    "for readers",
    "guest review",
    "reading list",
    "review blog",
    "review site",
    "reviews and interviews",
)

BAD_BOOK_TITLE_SNIPPETS = (
    "advanced search",
    "amazon.com",
    "book review",
    "book-review",
    "book sale",
    "book-sale",
    "conditions of use",
    "search results",
    "privacy policy",
    "review",
    "sale",
    "sign in",
    "terms of use",
    "terms of service",
    "update",
    "official site",
    "imdb",
    "skip to content",
    "skip to main content",
    "navigation menu",
    "author |",
    "writer |",
    "fantasy author |",
    "website of indie",
    "website of independent",
    "website of self-published",
)

GENERIC_BOOK_TITLE_WORDS = {
    "book",
    "books",
    "home",
    "menu",
    "search",
    "results",
    "author",
    "official",
    "site",
    "news",
    "blog",
}

WEAK_BOOK_TITLE_LABELS = (
    "speculative fiction",
    "science fiction",
    "historical fiction",
    "fantasy author",
    "science-fiction author",
    "fantasy fiction",
    "historical fiction",
    "author website",
    "official website",
    "official site",
    "websites",
    "graphic design",
)
TITLE_NAV_LABELS = {
    "books",
    "author",
    "writer",
    "menu",
    "navigation menu",
    "skip to",
    "skip to content",
    "skip to main content",
    "main content",
    "follow the author",
    "follow author",
    "official site",
    "official website",
    "home",
    "welcome",
    "about",
    "contact",
    "bibliography",
    "works",
}
STRICT_MASTER_TITLE_METHODS = {"jsonld_book", "listing_match", "listing_title_oracle", "books_index_card"}
PERSON_NAME_STYLE_RE = re.compile(r"^(?:[A-Z]\.){1,3}\s+[A-Z][A-Za-z'’-]+$")

BOOK_LINK_BLOCK_SNIPPETS = (
    "about",
    "bio",
    "blog",
    "book trailer",
    "bonus content",
    "contact",
    "creative writing",
    "editing services",
    "ebook",
    "events",
    "guide",
    "home",
    "images",
    "lessons",
    "maps",
    "media",
    "merch",
    "music",
    "newsletter",
    "news",
    "order a signed copy",
    "privacy",
    "read more",
    "reviews",
    "search",
    "services",
    "shop",
    "store",
    "support",
    "view all",
    "view more",
    "view series",
    "view story",
)
BOOK_LINK_CONTEXT_HINTS = (
    "available now",
    "book",
    "books",
    "buy now",
    "debut",
    "ebook",
    "fantasy",
    "fiction",
    "hardcover",
    "novel",
    "paperback",
    "release",
    "series",
)

TITLE_METHOD_BASE_SCORES = {
    "source_quote": 26,
    "source_snippet": 24,
    "source_title": 20,
    "books_page_jsonld": 24,
    "books_page_jsonld_itemlist": 20,
    "books_page_heading": 18,
    "books_page_link": 18,
    "books_page_img_alt": 17,
    "books_page_card_figcaption": 19,
    "books_page_card_title_attr": 18,
    "books_page_card_aria": 18,
    "books_page_card_text": 17,
    "books_page_list": 16,
    "book_detail_jsonld": 30,
    "book_detail_og": 26,
    "book_detail_h1": 24,
    "book_detail_h2": 22,
    "book_detail_link": 18,
    "listing_jsonld": 30,
    "listing_og": 26,
    "listing_h1": 24,
    "listing_h2": 22,
    "listing_link": 18,
    "listing_title_oracle": 34,
    "recency_snippet": 28,
    "candidate_page": 12,
    "fallback_link": 12,
}

TITLE_PAGE_KIND_SCORES = {
    "homepage_books": 6,
    "books_index": 8,
    "series_index": 5,
    "book_detail": 8,
    "listing": 8,
    "directory": 4,
    "recency": 6,
}

ROBOTS_USER_AGENT = USER_AGENT
ROBOTS_CACHE_TTL_SECONDS = 86400.0


def parse_args() -> argparse.Namespace:
    max_year_default = dt.datetime.now(dt.timezone.utc).year
    min_year_default = max_year_default - 4
    parser = argparse.ArgumentParser(description="Validate candidate author/book pages.")
    parser.add_argument("--input", default=csv_output("candidates.csv"), help="Input candidate CSV.")
    parser.add_argument("--output", default=csv_output("validated.csv"), help="Output validated CSV.")
    parser.add_argument("--delay", type=float, default=0.4, help="Pause between network requests.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds.")
    parser.add_argument("--min-year", type=int, default=min_year_default, help="Minimum publication year.")
    parser.add_argument("--max-year", type=int, default=max_year_default, help="Maximum publication year.")
    parser.add_argument("--max-candidates", type=int, default=0, help="Optional cap for faster runs.")
    parser.add_argument("--require-email", action="store_true", help="Keep only rows where author email is found.")
    parser.add_argument(
        "--require-contact-path",
        action="store_true",
        help="Keep only rows with a non-homepage contact path or subscribe/newsletter path.",
    )
    parser.add_argument(
        "--ignore-robots",
        action="store_true",
        help="Ignore robots.txt checks (not recommended).",
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
        help="Require stronger contact path hints (contact/press/media/support/help) when using --require-contact-path.",
    )
    parser.add_argument(
        "--require-location-proof",
        action="store_true",
        help="Keep only rows where location text is found.",
    )
    parser.add_argument(
        "--us-only",
        action="store_true",
        help="Keep only leads where location appears to be in the United States.",
    )
    parser.add_argument(
        "--max-support-pages",
        type=int,
        default=2,
        help="Max supporting pages to fetch per candidate (default: 2).",
    )
    parser.add_argument(
        "--max-pages-for-title",
        type=int,
        default=4,
        help="Max sitemap/nav-discovered book pages to fetch per domain (default: 4).",
    )
    parser.add_argument(
        "--max-pages-for-contact",
        type=int,
        default=6,
        help="Max sitemap/nav-discovered contact/about/newsletter pages to fetch per domain (default: 6).",
    )
    parser.add_argument(
        "--max-total-fetches-per-domain-per-run",
        type=int,
        default=14,
        help="Hard cap on total fetches per domain during validation (default: 14).",
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
        help="Hard cap on total network seconds spent per domain during validation.",
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
        help="Reserved Stage B concurrency knob. Current implementation stays domain-serial but records the setting.",
    )
    parser.add_argument(
        "--listing-strict",
        action="store_true",
        help="Require format + price + purchase control on listing pages.",
    )
    parser.add_argument(
        "--stats-output",
        default="",
        help="Optional JSON output for validator reject/count stats.",
    )
    parser.add_argument(
        "--domain-cache-path",
        default="",
        help="Optional JSONL cache for per-domain robots/sitemap/failure reuse. Defaults next to --stats-output when set.",
    )
    parser.add_argument(
        "--near-miss-location-output",
        default="",
        help="Optional CSV for strict rows that fail only on U.S. author location. Defaults next to --output.",
    )
    parser.add_argument(
        "--location-recovery-mode",
        choices=("off", "same_domain"),
        default="same_domain",
        help="Targeted recovery mode for strict location near-misses.",
    )
    parser.add_argument(
        "--location-recovery-pages",
        type=int,
        default=6,
        help="Additional same-domain location-support pages to try before writing a strict location near-miss.",
    )
    parser.add_argument(
        "--validation-profile",
        choices=(
            "default",
            "fully_verified",
            EMAIL_ONLY_PROFILE,
            AGENT_HUNT_PROFILE,
            ASTRA_OUTBOUND_PROFILE,
            VERIFIED_NO_US_PROFILE,
            STRICT_INTERACTIVE_PROFILE,
            STRICT_FULL_PROFILE,
        ),
        default="default",
        help="Validation profile: default keeps staged rows; fully_verified requires all outbound-ready proof gates; email_only keeps plausible author names with visible public emails plus SourceURL without requiring strict listing/indie/recency/location proof; agent_hunt uses staged validation and is intended for scout-mode orchestration in the batch loop; astra_outbound applies the Astra strict outbound preset; verified_no_us keeps the strict proof gates but does not require U.S. location; strict_interactive and strict_full keep fully_verified acceptance rules with smaller or larger runtime budgets.",
    )
    args = parser.parse_args()
    args._cli_flags = {token for token in os.sys.argv[1:] if token.startswith("--")}
    apply_validation_profile_defaults(args)
    return args


def derive_domain_cache_path(stats_output: str, explicit_path: str) -> Optional[Path]:
    if explicit_path:
        return Path(explicit_path)
    if not stats_output:
        return None
    stats_path = Path(stats_output)
    stem = stats_path.stem
    if stem.endswith("_validate_stats"):
        stem = stem[: -len("_validate_stats")]
    return stats_path.with_name(f"{stem}_domain_cache.jsonl")


def derive_location_debug_path(stats_output: str) -> Optional[Path]:
    if not stats_output:
        return None
    stats_path = Path(stats_output)
    stem = stats_path.stem
    if stem.endswith("_validate_stats"):
        stem = stem[: -len("_validate_stats")]
    return stats_path.with_name(f"{stem}_location_debug.jsonl")


def derive_listing_debug_path(stats_output: str) -> Optional[Path]:
    if not stats_output:
        return None
    stats_path = Path(stats_output)
    stem = stats_path.stem
    if stem.endswith("_validate_stats"):
        stem = stem[: -len("_validate_stats")]
    return stats_path.with_name(f"{stem}_listing_debug.jsonl")


def derive_near_miss_location_path(output_path: str, explicit_path: str) -> Optional[Path]:
    if explicit_path:
        return Path(explicit_path)
    if not output_path:
        return None
    output = Path(output_path)
    stem = output.stem
    if stem.endswith("_validated"):
        stem = stem[: -len("_validated")]
        return output.with_name(f"{stem}_near_miss_location.csv")
    if stem == "validated":
        return output.with_name("near_miss_location.csv")
    return output.with_name(f"{stem}_near_miss_location.csv")


def parser_from_robots_text(robots_text: str) -> urllib.robotparser.RobotFileParser:
    parser = urllib.robotparser.RobotFileParser()
    parser.parse((robots_text or "").splitlines())
    return parser


def ensure_domain_metrics(metrics_map: Dict[str, DomainMetrics], domain: str) -> DomainMetrics:
    metrics = metrics_map.get(domain)
    if metrics is None:
        metrics = DomainMetrics()
        metrics_map[domain] = metrics
    return metrics


def load_domain_cache(
    path: Optional[Path],
) -> Tuple[
    Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]],
    Dict[str, List[str]],
    Dict[str, FetchResult],
    Dict[str, str],
]:
    robots_cache: Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]] = {}
    sitemap_cache: Dict[str, List[str]] = {}
    failure_cache: Dict[str, FetchResult] = {}
    robots_text_cache: Dict[str, str] = {}
    if path is None or not path.exists():
        return robots_cache, sitemap_cache, failure_cache, robots_text_cache

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            origin = compact_text(str(payload.get("origin", "") or ""))
            domain = compact_text(str(payload.get("domain", "") or ""))
            robots_mode = compact_text(str(payload.get("robots_mode", "") or ""))
            robots_text = str(payload.get("robots_text", "") or "")
            expiry_ts = float(payload.get("robots_expiry_ts", 0) or 0)
            if origin:
                if robots_mode == "allow_all":
                    robots_cache[origin] = (None, expiry_ts or (time.time() + ROBOTS_CACHE_TTL_SECONDS))
                elif robots_mode in {"parsed", "disallow_all"} and robots_text:
                    robots_cache[origin] = (
                        parser_from_robots_text(robots_text),
                        expiry_ts or (time.time() + ROBOTS_CACHE_TTL_SECONDS),
                    )
                    robots_text_cache[origin] = robots_text

            sitemap_urls = payload.get("sitemap_urls")
            if domain and isinstance(sitemap_urls, list):
                sitemap_cache[domain] = [normalize_url(str(url)) for url in sitemap_urls if normalize_url(str(url))]

            for failure in payload.get("fetch_failures", []) if isinstance(payload.get("fetch_failures", []), list) else []:
                url = normalize_url(str(failure.get("url", "") or ""))
                reason = compact_text(str(failure.get("reason", "") or ""))
                status_code = int(failure.get("status_code", 0) or 0)
                if not url or not reason:
                    continue
                if reason not in {"404", "403", "non_html", "robots_disallow"}:
                    continue
                failure_cache[url] = FetchResult(url=url, failure_reason=reason, status_code=status_code)
    return robots_cache, sitemap_cache, failure_cache, robots_text_cache


def write_domain_cache(
    path: Optional[Path],
    robots_cache: Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]],
    robots_text_cache: Dict[str, str],
    sitemap_cache: Dict[str, List[str]],
    domain_metrics: Dict[str, DomainMetrics],
) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    now_ts = time.time()
    domains = sorted(set(sitemap_cache.keys()) | set(domain_metrics.keys()))
    for domain in domains:
        metrics = domain_metrics.get(domain, DomainMetrics())
        origin = ""
        robots_mode = ""
        robots_text = ""
        robots_expiry_ts = 0.0
        for candidate_origin, (parser, expiry_ts) in robots_cache.items():
            if registrable_domain(candidate_origin) != domain:
                continue
            origin = candidate_origin
            robots_expiry_ts = float(expiry_ts or now_ts)
            if parser is None:
                robots_mode = "allow_all"
            else:
                robots_text = robots_text_cache.get(candidate_origin, "")
                robots_mode = "parsed" if robots_text else "disallow_all"
            break

        payload = {
            "domain": domain,
            "origin": origin,
            "robots_mode": robots_mode,
            "robots_text": robots_text,
            "robots_expiry_ts": robots_expiry_ts,
            "sitemap_urls": sitemap_cache.get(domain, [])[:100],
            "fetch_failures": metrics.failures[-50:],
        }
        lines.append(json.dumps(payload, ensure_ascii=True))

    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def write_location_debug_records(path: Optional[Path], records: List[Dict[str, object]]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_listing_debug_records(path: Optional[Path], records: List[Dict[str, object]]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_near_miss_location_rows(path: Optional[Path], rows: List[Dict[str, str]]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=NEAR_MISS_LOCATION_COLUMNS)
        writer.writeheader()
        writer.writerows([{col: (row.get(col, "") or "").strip() for col in NEAR_MISS_LOCATION_COLUMNS} for row in rows])


def record_domain_fetch_failure(domain_metrics: Dict[str, DomainMetrics], url: str, reason: str, status_code: int = 0) -> None:
    domain = registrable_domain(url)
    if not domain:
        return
    metrics = ensure_domain_metrics(domain_metrics, domain)
    metrics.failures.append({"url": normalize_url(url), "reason": reason, "status_code": int(status_code or 0)})


def dominant_phase_name(prefilter_seconds: float, verify_seconds: float) -> str:
    if verify_seconds > prefilter_seconds:
        return "verify"
    if prefilter_seconds > verify_seconds:
        return "prefilter"
    return "balanced"


def summarize_domain_budget_burn(domain_metrics: Dict[str, DomainMetrics], limit: int = 10) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    for domain, metrics in domain_metrics.items():
        summary.append(
            {
                "domain": domain,
                "fetch_count": metrics.fetch_count,
                "total_seconds": round(metrics.total_seconds, 4),
                "avg_latency": round((metrics.total_seconds / metrics.fetch_count), 4) if metrics.fetch_count else 0.0,
                "timeout_count": metrics.timeout_count,
                "prefilter_seconds": round(metrics.prefilter_seconds, 4),
                "verify_seconds": round(metrics.verify_seconds, 4),
                "dominant_phase": dominant_phase_name(metrics.prefilter_seconds, metrics.verify_seconds),
            }
        )
    summary.sort(
        key=lambda item: (
            float(item.get("total_seconds", 0.0) or 0.0),
            int(item.get("fetch_count", 0) or 0),
            int(item.get("timeout_count", 0) or 0),
        ),
        reverse=True,
    )
    return summary[: max(0, limit)]


def summarize_candidate_budget_burn(
    candidate_budget_records: Sequence[CandidateBudgetMetrics],
    limit: int = 10,
) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    for metrics in candidate_budget_records:
        summary.append(
            {
                "candidate_url": metrics.candidate_url,
                "candidate_domain": metrics.candidate_domain,
                "fetch_count": metrics.fetch_count,
                "total_seconds": round(metrics.total_seconds, 4),
                "prefilter_seconds": round(metrics.prefilter_seconds, 4),
                "verify_seconds": round(metrics.verify_seconds, 4),
                "dominant_phase": dominant_phase_name(metrics.prefilter_seconds, metrics.verify_seconds),
                "reject_reason": metrics.reject_reason,
                "kept": metrics.kept,
                "location_recovery_attempted": metrics.location_recovery_attempted,
                "listing_recovery_attempted": metrics.listing_recovery_attempted,
                "location_recovery_skipped_reason": metrics.location_recovery_skipped_reason,
                "listing_recovery_skipped_reason": metrics.listing_recovery_skipped_reason,
                "current_state": metrics.current_state,
                "next_action": metrics.next_action,
                "next_action_reason": metrics.next_action_reason,
                "last_attempted_action": metrics.last_attempted_action,
            }
        )
    summary.sort(
        key=lambda item: (
            float(item.get("total_seconds", 0.0) or 0.0),
            int(item.get("fetch_count", 0) or 0),
            float(item.get("verify_seconds", 0.0) or 0.0),
            float(item.get("prefilter_seconds", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return summary[: max(0, limit)]


def append_candidate_action(
    ledger: CandidateProofLedger,
    action: str,
    *,
    detail: str = "",
    limit: int = 6,
) -> None:
    if not action:
        return
    entry = action if not detail else f"{action}:{detail}"
    history = list(ledger.action_history or [])
    if not history or history[-1] != entry:
        history.append(entry)
    if limit > 0:
        history = history[-limit:]
    ledger.action_history = history
    ledger.last_attempted_action = action


def derive_candidate_working_state(
    ledger: CandidateProofLedger,
    *,
    strict_verified_mode: bool,
    require_us_location: bool,
    effective_require_email: bool,
    effective_listing_strict: bool,
) -> str:
    reject_reason = (ledger.reject_reason_if_any or "").strip()
    if reject_reason in DEAD_END_REASONS:
        return "dead_end"
    if effective_require_email and reject_reason in EMAIL_REJECT_REASONS:
        return "needs_email_proof"
    if require_us_location and reject_reason in LOCATION_REJECT_PLANNER_REASONS:
        return "needs_location_proof"
    if reject_reason.startswith(LISTING_REJECT_PREFIXES):
        return "needs_listing_proof"
    if reject_reason in RECENCY_REJECT_REASONS:
        return "needs_recency_proof"
    if ledger.current_state == "verified":
        return "verified"
    if ledger.current_state == "dead_end":
        return "dead_end"
    if strict_verified_mode and not reject_reason:
        if (
            (not effective_require_email or ledger.best_email_source_url)
            and (not require_us_location or ledger.best_location_source_url)
            and (not effective_listing_strict or ledger.best_listing_source_url)
            and ledger.best_recency_source_url
            and ledger.best_indie_source_url
        ):
            return "verified"
    if ledger.best_email_source_url and not ledger.best_listing_source_url and effective_listing_strict:
        return "needs_listing_proof"
    if require_us_location and not ledger.best_location_source_url and ledger.best_email_source_url:
        return "needs_location_proof"
    if effective_require_email and not ledger.best_email_source_url:
        return "needs_email_proof"
    if strict_verified_mode and not ledger.best_recency_source_url:
        return "needs_recency_proof"
    if ledger.last_attempted_action:
        return "triage_ready"
    return "harvested"


def plan_candidate_next_action(
    ledger: CandidateProofLedger,
    *,
    strict_verified_mode: bool,
    require_us_location: bool,
    effective_require_email: bool,
    effective_listing_strict: bool,
) -> Dict[str, str]:
    state = derive_candidate_working_state(
        ledger,
        strict_verified_mode=strict_verified_mode,
        require_us_location=require_us_location,
        effective_require_email=effective_require_email,
        effective_listing_strict=effective_listing_strict,
    )
    reject_reason = (ledger.reject_reason_if_any or "").strip()
    if state == "verified":
        return {"state": "verified", "action": "mark_verified", "reason": "strict_proofs_satisfied"}
    if reject_reason in DEAD_END_REASONS:
        return {"state": "dead_end", "action": "stop_dead_end", "reason": reject_reason}
    if reject_reason == "publisher_location_only":
        return {"state": "needs_location_proof", "action": "try_location_proof", "reason": reject_reason}
    if reject_reason == "listing_not_found":
        return {"state": "needs_listing_proof", "action": "try_listing_proof", "reason": reject_reason}
    if reject_reason == "weak_strict_email_quality":
        return {"state": "needs_email_proof", "action": "try_email_proof", "reason": reject_reason}
    if reject_reason == "bad_author_name":
        return {"state": "dead_end", "action": "stop_dead_end", "reason": reject_reason}
    if state == "needs_email_proof":
        return {"state": state, "action": "try_email_proof", "reason": reject_reason or "email_proof_missing"}
    if state == "needs_location_proof":
        return {"state": state, "action": "try_location_proof", "reason": reject_reason or "location_proof_missing"}
    if state == "needs_listing_proof":
        return {"state": state, "action": "try_listing_proof", "reason": reject_reason or "listing_proof_missing"}
    if state == "needs_recency_proof":
        return {"state": state, "action": "try_recency_proof", "reason": reject_reason or "recency_proof_missing"}
    if state == "dead_end":
        return {"state": state, "action": "stop_dead_end", "reason": reject_reason or "strict_dead_end"}
    return {"state": state, "action": "try_email_proof", "reason": reject_reason or "triage_candidate"}


def serialize_candidate_proof_ledger(ledger: CandidateProofLedger) -> Dict[str, object]:
    return {
        "candidate_url": ledger.candidate_url,
        "candidate_domain": ledger.candidate_domain,
        "current_state": ledger.current_state,
        "last_attempted_action": ledger.last_attempted_action,
        "best_email_snippet": ledger.best_email_snippet,
        "best_email_source_url": ledger.best_email_source_url,
        "best_location_snippet": ledger.best_location_snippet,
        "best_location_source_url": ledger.best_location_source_url,
        "best_listing_snippet": ledger.best_listing_snippet,
        "best_listing_source_url": ledger.best_listing_source_url,
        "best_recency_snippet": ledger.best_recency_snippet,
        "best_recency_source_url": ledger.best_recency_source_url,
        "best_indie_snippet": ledger.best_indie_snippet,
        "best_indie_source_url": ledger.best_indie_source_url,
        "reject_reason_if_any": ledger.reject_reason_if_any,
        "action_history": list(ledger.action_history or []),
        "astra_rule_checks": dict(ledger.astra_rule_checks or {}),
    }


def map_stable_fail_reason(reject_reason: str) -> str:
    reason = (reject_reason or "").strip().lower()
    if not reason:
        return ""
    if reason == "duplicate_lead":
        return "duplicate_lead"
    if reason == "blocked_candidate_url":
        return "candidate_blocked"
    if reason.startswith("page_fetch_failed_"):
        return "candidate_unreachable"
    if reason in {"enterprise_or_famous", "famous_signal", "wikipedia_signal"}:
        return "excluded_famous_or_enterprise"
    if reason in {"non_author_website", "non_author_website_after_name"}:
        return "candidate_not_author_site"
    if reason == "bad_author_name":
        return "insufficient_author_identity"
    if reason in {"no_visible_author_email", "no_author_email"}:
        return "missing_visible_email"
    if reason == "weak_strict_email_quality":
        return "email_not_visible_text"
    if reason in {"definitive_non_us_location", "non_us_location"}:
        return "non_us_author"
    if reason in {"no_location_signal", "location_ambiguous", "publisher_location_only", "weak_us_signal"}:
        return "missing_us_proof"
    if reason in {"no_indie_proof", "no_onsite_indie_proof"}:
        return "missing_indie_proof"
    if reason in {"no_recency_proof", "no_on_page_date"}:
        return "stale_publication"
    if reason == "missing_best_source_url":
        return "missing_best_source_url"
    if reason == "no_contact_or_subscribe_path":
        return "missing_contact_path"
    if reason.startswith("listing_"):
        if any(
            token in reason
            for token in (
                "title_mismatch",
                "incomplete",
                "wrong_format",
                "missing_buy",
                "no_buy_control",
                "no_price",
                "missing_price",
                "missing_title",
                "unavailable",
                "reseller_only",
                "parse_empty",
                "interstitial",
            )
        ):
            return "listing_not_buyable"
        return "missing_listing_proof"
    return "insufficient_snippet"


def candidate_outcome_confidence(metrics: CandidateBudgetMetrics) -> str:
    if metrics.kept or metrics.current_state == "verified":
        return "strong"
    if metrics.current_state in {"needs_email_proof", "needs_location_proof", "needs_listing_proof", "needs_recency_proof"}:
        return "medium"
    return "weak"


def infer_source_family(source_type: str, source_url: str = "") -> str:
    normalized = (source_type or "").strip().lower()
    if normalized:
        return normalized
    return registrable_domain(source_url) or "unknown"


def serialize_candidate_outcome(metrics: CandidateBudgetMetrics) -> Dict[str, object]:
    stable_reason = map_stable_fail_reason(metrics.reject_reason)
    fail_reasons = [] if metrics.kept else [stable_reason or "insufficient_snippet"]
    return {
        "candidate_url": metrics.candidate_url,
        "candidate_domain": metrics.candidate_domain,
        "source_type": metrics.source_type,
        "source_query": metrics.source_query,
        "source_url": metrics.source_url,
        "source_title": metrics.source_title,
        "source_snippet": metrics.source_snippet,
        "query_original": metrics.query_original,
        "query_effective": metrics.query_effective,
        "widen_level": metrics.widen_level,
        "widen_reason": metrics.widen_reason,
        "stale_run_counter": metrics.stale_run_counter,
        "author_name": metrics.author_name,
        "book_title": metrics.book_title,
        "email_source_url": metrics.email_source_url,
        "email_value": (metrics.email or "").strip().lower(),
        "location_source_url": metrics.location_source_url,
        "location_value": metrics.location_value or metrics.us_snippet,
        "indie_proof_url": metrics.indie_proof_url,
        "indie_proof_value": metrics.indie_proof_value or metrics.indie_snippet,
        "retailer_listing_url": metrics.retailer_listing_url or metrics.book_url,
        "retailer_listing_status": metrics.retailer_listing_status,
        "best_source_url": metrics.best_source_url,
        "normalized_listing_url": metrics.normalized_listing_url,
        "astra_rule_checks": dict(metrics.astra_rule_checks or {}),
        "recency_source_url": metrics.recency_source_url,
        "recency_value": metrics.recency_value,
        "source_family": metrics.source_family or infer_source_family(metrics.source_type, metrics.source_url),
        "source_score": round(float(metrics.source_score or 0.0), 2),
        "cross_run_score": round(float(metrics.cross_run_score or 0.0), 2),
        "validation_state": metrics.validation_state or metrics.current_state,
        "passed": metrics.kept,
        "primary_fail_reason": fail_reasons[0] if fail_reasons else "",
        "fail_reasons": fail_reasons,
        "confidence": candidate_outcome_confidence(metrics),
        "email": (metrics.email or "").strip().lower(),
        "email_snippet": metrics.email_snippet,
        "us_snippet": metrics.us_snippet,
        "indie_snippet": metrics.indie_snippet,
        "listing_snippet": metrics.listing_snippet,
        "book_url": metrics.book_url,
        "fetch_count": metrics.fetch_count,
        "total_seconds": round(metrics.total_seconds, 4),
        "prefilter_seconds": round(metrics.prefilter_seconds, 4),
        "verify_seconds": round(metrics.verify_seconds, 4),
        "dominant_phase": dominant_phase_name(metrics.prefilter_seconds, metrics.verify_seconds),
        "reject_reason": metrics.reject_reason,
        "kept": metrics.kept,
        "location_recovery_attempted": metrics.location_recovery_attempted,
        "listing_recovery_attempted": metrics.listing_recovery_attempted,
        "location_recovery_skipped_reason": metrics.location_recovery_skipped_reason,
        "listing_recovery_skipped_reason": metrics.listing_recovery_skipped_reason,
        "current_state": metrics.current_state,
        "next_action": metrics.next_action,
        "next_action_reason": metrics.next_action_reason,
        "last_attempted_action": metrics.last_attempted_action,
    }


def domain_budget_exhausted(
    domain: str,
    domain_metrics: Dict[str, DomainMetrics],
    *,
    max_fetches_per_domain: int,
    max_seconds_per_domain: float,
    max_timeouts_per_domain: int,
) -> str:
    metrics = ensure_domain_metrics(domain_metrics, domain)
    if max_fetches_per_domain > 0 and metrics.fetch_count >= max_fetches_per_domain:
        return "fetch_budget_exceeded"
    if max_seconds_per_domain > 0 and metrics.total_seconds >= max_seconds_per_domain:
        return "time_budget_exceeded"
    if max_timeouts_per_domain > 0 and metrics.timeout_count >= max_timeouts_per_domain:
        return "time_budget_exceeded"
    return ""


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url
    parsed = urlparse(url)
    if not parsed.netloc:
        return ""
    return parsed._replace(fragment="").geturl()


def strip_trailing_url_punctuation(url: str) -> str:
    return (url or "").strip().rstrip(").,;:!?]}>\"'")


def swap_url_scheme(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"}:
        return ""
    alt_scheme = "http" if parsed.scheme == "https" else "https"
    return parsed._replace(scheme=alt_scheme).geturl()


def build_fetch_variants(url: str, allow_root_fallback: bool = False) -> List[str]:
    raw = strip_tracking_query_params(strip_trailing_url_punctuation(url))
    normalized = normalize_url(raw)
    if not normalized:
        return []

    parsed = urlparse(normalized)
    variants: List[str] = []
    seen = set()

    def add(candidate: str) -> None:
        normalized_candidate = normalize_url(candidate)
        if not normalized_candidate or normalized_candidate in seen:
            return
        seen.add(normalized_candidate)
        variants.append(normalized_candidate)

    add(normalized)
    if parsed.query:
        add(parsed._replace(query="").geturl())
    if allow_root_fallback and parsed.path not in {"", "/"}:
        add(parsed._replace(path="/", params="", query="", fragment="").geturl())

    swapped = swap_url_scheme(normalized)
    if swapped:
        add(swapped)
        swapped_parsed = urlparse(swapped)
        if swapped_parsed.query:
            add(swapped_parsed._replace(query="").geturl())
        if allow_root_fallback and swapped_parsed.path not in {"", "/"}:
            add(swapped_parsed._replace(path="/", params="", query="", fragment="").geturl())

    return variants


def is_blocked_candidate_url(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return True
    if is_retailer_url(normalized) and not is_allowed_retailer_url(normalized):
        return True
    return url_matches_blocklist(normalized)


def get_origin(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""


def get_text(soup: BeautifulSoup) -> str:
    snapshot = BeautifulSoup(str(soup), "html.parser")
    for tag in snapshot(["script", "style", "noscript"]):
        tag.extract()
    return " ".join(snapshot.stripped_strings)


def is_allowed_by_robots(
    session: requests.Session,
    url: str,
    timeout: float,
    robots_cache: Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]],
    robots_text_cache: Optional[Dict[str, str]],
    ignore_robots: bool,
    retry_seconds: float,
) -> bool:
    if ignore_robots:
        return True

    origin = get_origin(url)
    if not origin:
        return True

    now_ts = time.time()
    cached_entry = robots_cache.get(origin)
    if cached_entry is None or now_ts >= cached_entry[1]:
        robots_url = f"{origin}/robots.txt"
        parser = urllib.robotparser.RobotFileParser()
        try:
            resp = session.get(robots_url, timeout=timeout)
            status = int(resp.status_code)
            if status == 200:
                parser.set_url(robots_url)
                parser.parse(resp.text.splitlines())
                robots_cache[origin] = (parser, now_ts + ROBOTS_CACHE_TTL_SECONDS)
                if robots_text_cache is not None:
                    robots_text_cache[origin] = resp.text
            elif 500 <= status <= 599:
                parser.set_url(robots_url)
                parser.parse(["User-agent: *", "Disallow: /"])
                robots_cache[origin] = (parser, now_ts + max(10.0, retry_seconds))
                if robots_text_cache is not None:
                    robots_text_cache[origin] = "User-agent: *\nDisallow: /"
            else:
                # RFC 9309: 4xx on robots.txt is "Unavailable", crawler MAY access.
                robots_cache[origin] = (None, now_ts + ROBOTS_CACHE_TTL_SECONDS)
                if robots_text_cache is not None:
                    robots_text_cache.pop(origin, None)
        except requests.RequestException:
            # RFC 9309: unreachable robots.txt should be treated as complete disallow.
            parser.set_url(robots_url)
            parser.parse(["User-agent: *", "Disallow: /"])
            robots_cache[origin] = (parser, now_ts + max(10.0, retry_seconds))
            if robots_text_cache is not None:
                robots_text_cache[origin] = "User-agent: *\nDisallow: /"

    cached = robots_cache.get(origin, (None, now_ts + ROBOTS_CACHE_TTL_SECONDS))[0]
    if cached is None:
        return True
    return cached.can_fetch(ROBOTS_USER_AGENT, url)

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    # Validation already does bounded per-domain crawling; deep HTTP retries mostly amplify tail latency.
    retry = Retry(
        total=1,
        read=1,
        connect=1,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def classify_request_exception(exc: requests.RequestException) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", 0) or 0
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if status_code in {403, 404, 429}:
        return str(status_code)
    if status_code:
        return f"http_{status_code}"
    return "request_error"


def fetch_with_meta(
    session: requests.Session,
    url: str,
    timeout: float,
    robots_cache: Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]],
    robots_text_cache: Optional[Dict[str, str]],
    ignore_robots: bool,
    retry_seconds: float,
    expect_html: bool = True,
) -> FetchResult:
    normalized = normalize_url(url)
    if not normalized:
        return FetchResult(url=url, failure_reason="bad_url")

    if not is_allowed_by_robots(
        session=session,
        url=normalized,
        timeout=timeout,
        robots_cache=robots_cache,
        robots_text_cache=robots_text_cache,
        ignore_robots=ignore_robots,
        retry_seconds=retry_seconds,
    ):
        return FetchResult(url=normalized, failure_reason="robots_disallow")

    try:
        resp = session.get(normalized, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return FetchResult(
            url=normalized,
            failure_reason=classify_request_exception(exc),
            status_code=getattr(getattr(exc, "response", None), "status_code", 0) or 0,
        )

    ctype = (resp.headers.get("Content-Type", "") or "").lower()
    text = resp.text or ""
    if expect_html and "html" not in ctype and "<html" not in text[:500].lower():
        return FetchResult(
            url=normalized,
            final_url=normalize_url(resp.url) or normalized,
            content_type=ctype,
            failure_reason="non_html",
            status_code=resp.status_code,
        )
    soup = BeautifulSoup(text, "html.parser") if expect_html else None
    return FetchResult(
        url=normalized,
        final_url=normalize_url(resp.url) or normalized,
        text=text,
        soup=soup,
        content_type=ctype,
        status_code=resp.status_code,
    )


def fetch(
    session: requests.Session,
    url: str,
    timeout: float,
    robots_cache: Dict[str, Tuple[Optional[urllib.robotparser.RobotFileParser], float]],
    robots_text_cache: Optional[Dict[str, str]],
    ignore_robots: bool,
    retry_seconds: float,
) -> Optional[Tuple[str, BeautifulSoup]]:
    result = fetch_with_meta(
        session=session,
        url=url,
        timeout=timeout,
        robots_cache=robots_cache,
        robots_text_cache=robots_text_cache,
        ignore_robots=ignore_robots,
        retry_seconds=retry_seconds,
        expect_html=True,
    )
    if not result.ok or result.soup is None:
        return None
    return result.text, result.soup


def parse_sitemap_xml(text: str) -> Tuple[List[str], List[str]]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return [], []

    urls: List[str] = []
    sitemap_urls: List[str] = []

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    root_name = local_name(root.tag)
    if root_name == "urlset":
        for node in root:
            if local_name(node.tag) != "url":
                continue
            for child in node:
                if local_name(child.tag) == "loc" and (child.text or "").strip():
                    urls.append((child.text or "").strip())
    elif root_name == "sitemapindex":
        for node in root:
            if local_name(node.tag) != "sitemap":
                continue
            for child in node:
                if local_name(child.tag) == "loc" and (child.text or "").strip():
                    sitemap_urls.append((child.text or "").strip())
    return urls, sitemap_urls


def has_enterprise_signal(text: str) -> bool:
    lowered = text.lower()
    if any(signal in lowered for signal in ENTERPRISE_SIGNALS):
        return True

    # Heuristic for very large social audiences.
    for m in re.finditer(
        r"(\d+(?:[.,]\d+)?)\s*(k|m)?\s*(followers|subscribers)",
        lowered,
        flags=re.IGNORECASE,
    ):
        num = float(m.group(1).replace(",", ""))
        suffix = (m.group(2) or "").lower()
        if suffix == "k":
            num *= 1_000
        elif suffix == "m":
            num *= 1_000_000
        if num >= 50_000:
            return True
    return False


def find_location(text: str) -> Optional[str]:
    compact = re.sub(r"\s+", " ", text)
    for pattern in LOCATION_PATTERNS:
        match = re.search(pattern, compact)
        if match:
            value = match.group(1).strip(" .,-")
            if len(value) >= 3 and len(value.split()) <= 7:
                return value

    # Fallback patterns for common profile labels.
    match = re.search(r"\b(location|based)\s*[:\-]\s*([A-Z][A-Za-z .,'-]{2,60})", compact)
    if match:
        value = match.group(2).strip(" .,-")
        if len(value.split()) <= 7:
            return value
    return None


def is_us_location(value: str) -> bool:
    location = re.sub(r"\s+", " ", (value or "").strip())
    if not location:
        return False
    lowered = location.lower()

    if "united states" in lowered:
        return True
    if re.search(r"\b(?:usa|u\.s\.a\.|u\.s\.)\b", lowered):
        return True

    if any(state in lowered for state in US_STATE_NAMES):
        return True

    code_pattern = r"(?:^|,\s*|\s+)(%s)\b" % "|".join(sorted(US_STATE_CODES))
    if re.search(code_pattern, location, flags=re.IGNORECASE):
        return True

    return False


def location_context_is_publisher_only(text: str, source_kind: str = "") -> bool:
    lowered = compact_text(text).lower()
    if not lowered:
        return False
    publisher_signal = any(term in lowered for term in PUBLISHER_LOCATION_CONTEXT_HINTS)
    author_signal = any(term in lowered for term in AUTHOR_LOCATION_CONTEXT_HINTS)
    kind_signal = source_kind in {"publisher", "support"}
    return (publisher_signal or kind_signal) and not author_signal


def snippet_has_business_or_event_location_context(text: str) -> bool:
    lowered = compact_text(text).lower()
    if not lowered:
        return False
    return any(term in lowered for term in BIZ_LOCATION_CONTEXT_HINTS)


def author_tokens_for_context(author_name: str) -> List[str]:
    return [token.lower() for token in re.findall(r"[A-Za-z]+", author_name or "") if len(token) >= 3]


def location_snippet_has_author_context(
    snippet: str,
    *,
    author_name: str = "",
    source_kind: str = "",
    method: str = "",
) -> bool:
    lowered = compact_text(snippet).lower()
    if not lowered:
        return False
    if any(term in lowered for term in AUTHOR_LOCATION_CONTEXT_HINTS):
        return True
    if source_kind in {"directory", "about_page", "bio_page", "author_page"} and not snippet_has_business_or_event_location_context(lowered):
        return True
    if method == "jsonld":
        return True
    name_tokens = author_tokens_for_context(author_name)
    if name_tokens and sum(1 for token in name_tokens if token in lowered) >= min(2, len(name_tokens)):
        return True
    if name_tokens and name_tokens[-1] in lowered and any(term in lowered for term in ("author", "writer", "novelist")):
        return True
    return False


def looks_ambiguous_location_value(value: str) -> bool:
    lowered = compact_text(value).lower()
    if not lowered:
        return True
    if any(term in lowered for term in AMBIGUOUS_LOCATION_TERMS):
        return True
    if lowered in {"america"}:
        return True
    return False


def detect_broad_location_phrase(text: str) -> Tuple[str, str]:
    compact = compact_text(text)
    for pattern in BROAD_LOCATION_PATTERNS:
        match = pattern.search(compact)
        if not match:
            continue
        value = compact_text(match.group(1)).strip(" .,-")
        if value:
            return value, proof_snippet_for_terms(compact, [match.group(0)])
    return "", ""


def detect_weak_us_signal(text: str) -> Tuple[str, str]:
    compact = compact_text(text)
    if not compact:
        return "", ""
    for pattern in (
        re.compile(r"\b(?:usa|u\.s\.a\.|u\.s\.|united states)\b", re.IGNORECASE),
        re.compile(r"\b(?:%s)\b" % "|".join(re.escape(name) for name in sorted(US_STATE_NAMES, key=len, reverse=True)), re.IGNORECASE),
        re.compile(r",\s*(%s)\b" % "|".join(sorted(US_STATE_CODES)), re.IGNORECASE),
    ):
        match = pattern.search(compact)
        if match:
            term = compact_text(match.group(0))
            return term, proof_snippet_for_terms(compact, [term])
    return "", ""


def detect_author_tied_city_state(text: str, author_name: str = "") -> Tuple[str, str]:
    compact = compact_text(text)
    if not compact:
        return "", ""
    for match in AUTHOR_TIED_CITY_STATE_RE.finditer(compact):
        value = compact_text(match.group(1)).strip(" .,-")
        snippet = proof_snippet_for_terms(compact, [match.group(1)])
        if location_snippet_has_author_context(
            snippet or compact,
            author_name=author_name,
            source_kind="page",
            method="author_city_state",
        ):
            return value, snippet
    return "", ""


def has_strong_author_tied_us_phrase(
    snippet: str,
    *,
    found_location: str = "",
    author_name: str = "",
    source_kind: str = "",
) -> bool:
    compact = compact_text(snippet)
    if not compact or not is_us_location(found_location):
        return False
    if source_kind not in {"page", "about_page", "bio_page", "author_page"}:
        return False
    lowered = compact.lower()
    if snippet_has_business_or_event_location_context(lowered):
        if not any(token in lowered for token in ("lives", "based in", "resides", "i live", "i'm based", "i am based", "i'm from", "i am from")):
            return False
    author_tokens = author_tokens_for_context(author_name)
    has_author_name = bool(author_tokens and sum(1 for token in author_tokens if token in lowered) >= min(2, len(author_tokens)))
    has_bio_pronoun = any(token in lowered for token in (" i live in ", " i'm based in ", " i am based in ", " i reside in ", " i'm from ", " i am from "))
    has_family_bio = any(token in lowered for token in (" with his ", " with her ", " with their ", " with my ", " wife", " husband", " children", " kids"))
    for pattern in STRONG_AUTHOR_US_LOCATION_PATTERNS:
        match = pattern.search(compact)
        if not match:
            continue
        value = compact_text(match.group(1)).strip(" .,-")
        if not is_us_location(value):
            continue
        if has_bio_pronoun:
            return True
        if has_author_name and (has_family_bio or source_kind in {"about_page", "bio_page", "author_page"}):
            return True
    return False


def classify_location_evidence(
    *,
    text: str,
    source_url: str,
    source_kind: str = "",
    soup: Optional[BeautifulSoup] = None,
    author_name: str = "",
) -> Dict[str, str]:
    found, snippet, method = find_location_proof(text, soup, author_name=author_name)
    snippet = compact_text(snippet)
    if found:
        decision = "ok"
        author_context = location_snippet_has_author_context(
            snippet or text,
            author_name=author_name,
            source_kind=source_kind,
            method=method,
        )
        if has_strong_author_tied_us_phrase(
            snippet or text,
            found_location=found,
            author_name=author_name,
            source_kind=source_kind,
        ):
            decision = "ok"
        elif location_context_is_publisher_only(snippet or text, source_kind) or snippet_has_business_or_event_location_context(snippet or text):
            decision = "publisher_location_only"
        elif method == "footer" and not author_context:
            decision = "publisher_location_only" if source_kind in {"contact_page", "press_media_page", "page"} else "location_ambiguous"
        elif method in {"text_phrase", "author_city_state"} and not author_context:
            decision = "weak_us_signal" if is_us_location(found) else "location_ambiguous"
        elif is_us_location(found):
            decision = "ok"
        elif looks_ambiguous_location_value(found):
            decision = "location_ambiguous"
        else:
            decision = "non_us_location"
        return {
            "decision": decision,
            "location": compact_text(found),
            "snippet": snippet,
            "method": method,
            "source_url": normalize_url(source_url),
            "source_kind": source_kind,
        }

    ambiguous_value, ambiguous_snippet = detect_broad_location_phrase(text)
    if ambiguous_value:
        decision = "location_ambiguous"
        lowered_value = compact_text(ambiguous_value).lower()
        author_context = location_snippet_has_author_context(
            ambiguous_snippet or text,
            author_name=author_name,
            source_kind=source_kind,
            method="broad_phrase",
        )
        if location_context_is_publisher_only(ambiguous_snippet or text, source_kind) or snippet_has_business_or_event_location_context(ambiguous_snippet or text):
            decision = "publisher_location_only"
        elif any(term in lowered_value for term in NON_US_LOCATION_TERMS):
            decision = "non_us_location"
        elif is_us_location(ambiguous_value) and author_context:
            decision = "weak_us_signal"
        elif looks_ambiguous_location_value(ambiguous_value):
            decision = "location_ambiguous"
        else:
            decision = "non_us_location"
        return {
            "decision": decision,
            "location": compact_text(ambiguous_value),
            "snippet": compact_text(ambiguous_snippet),
            "method": "broad_phrase",
            "source_url": normalize_url(source_url),
            "source_kind": source_kind,
        }

    weak_value, weak_snippet = detect_weak_us_signal(text)
    if weak_value:
        decision = "publisher_location_only" if (
            location_context_is_publisher_only(weak_snippet or text, source_kind)
            or snippet_has_business_or_event_location_context(weak_snippet or text)
        ) else "weak_us_signal"
        return {
            "decision": decision,
            "location": compact_text(weak_value),
            "snippet": compact_text(weak_snippet),
            "method": "weak_us_signal",
            "source_url": normalize_url(source_url),
            "source_kind": source_kind,
        }

    return {
        "decision": "no_location_signal",
        "location": "",
        "snippet": "",
        "method": "",
        "source_url": normalize_url(source_url),
        "source_kind": source_kind,
    }


def choose_location_assessment(attempts: List[Dict[str, str]]) -> Dict[str, str]:
    if not attempts:
        return {
            "decision": "no_location_signal",
            "location": "",
            "snippet": "",
            "method": "",
            "source_url": "",
            "source_kind": "",
        }

    for attempt in attempts:
        if attempt.get("decision") == "ok":
            return attempt

    ranked = sorted(
        enumerate(attempts),
        key=lambda item: (-LOCATION_DECISION_PRIORITY.get(item[1].get("decision", ""), 0), item[0]),
    )
    return ranked[0][1]


def publisher_location_only_dead_end(
    attempts: Sequence[Dict[str, str]],
    *,
    origin_url: str = "",
) -> bool:
    origin_domain = registrable_domain(origin_url)
    supporting_pages = set()
    for attempt in attempts:
        if (attempt.get("decision", "") or "").strip() != "publisher_location_only":
            continue
        source_url = normalize_url(attempt.get("source_url", "") or "")
        if origin_domain and registrable_domain(source_url) != origin_domain:
            continue
        source_kind = (attempt.get("source_kind", "") or "").strip()
        if source_kind not in {"page", "about_page", "bio_page", "author_page", "contact_page"}:
            continue
        if source_url:
            supporting_pages.add(source_url)
    return len(supporting_pages) >= 1


def find_indie_keyword(text: str) -> bool:
    lowered = text.lower()
    return any(key in lowered for key in INDIE_KEYWORDS)


def is_listing_domain(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    if host == "amazon.com" or host.endswith(".amazon.com"):
        return True
    if host == "barnesandnoble.com" or host.endswith(".barnesandnoble.com"):
        return True
    return False


def normalize_listing_url(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return parsed._replace(query="", fragment="").geturl()


def is_listing_url(url: str) -> bool:
    url = normalize_listing_url(url)
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host == "amazon.com" or host.endswith(".amazon.com"):
        return is_allowed_retailer_url(url)
    if host == "barnesandnoble.com" or host.endswith(".barnesandnoble.com"):
        return "/w/" in path
    if host == "target.com" or host.endswith(".target.com"):
        return False
    return False


def is_listing_shortlink_url(url: str) -> bool:
    host = urlparse(normalize_url(url)).netloc.lower()
    return any(host == domain or host.endswith("." + domain) for domain in LISTING_SHORTLINK_HOSTS)


def extract_links(base_url: str, soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = normalize_url(urljoin(base_url, a["href"]))
        if href:
            links.append(href)
    return links


def dedupe_urls(urls: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def select_supporting_pages(base_url: str, links: Iterable[str], limit: int = 4) -> List[str]:
    origin = get_origin(base_url)
    scored: List[Tuple[int, int, str]] = []

    for link in links:
        if not link:
            continue
        if origin and not link.startswith(origin):
            continue
        lowered = link.lower()
        if any(bad in lowered for bad in SUPPORT_PAGE_BLOCK_HINTS):
            continue

        score = 0
        if "/author/show/" in lowered:
            score += 10
        if any(hint in lowered for hint in ("/about", "about-", "about_", "/author", "/bio")):
            score += 5
        if "contact" in lowered:
            score += 4
        if any(hint in lowered for hint in ("/book", "/books", "novel")):
            score += 3
        if any(hint in lowered for hint in ("press", "media", "news")):
            score += 2
        if score > 0:
            scored.append((score, len(lowered), link))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return dedupe_urls([item[2] for item in scored])[:limit]


def find_contact_page(base_url: str, links: Iterable[str], soup: BeautifulSoup) -> str:
    origin = get_origin(base_url)
    scored: List[Tuple[int, str]] = []
    for a in soup.find_all("a", href=True):
        href = normalize_url(urljoin(base_url, a["href"]))
        if not href or not href.startswith(origin):
            continue
        if url_matches_blocklist(href):
            continue
        label = (a.get_text(" ", strip=True) + " " + href).lower()
        score = 0
        if "contact" in label:
            score += 5
        if "about" in label:
            score += 2
        if "press" in label or "media" in label:
            score += 1
        if score > 0:
            scored.append((score, href))

    if scored:
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    # Fallback to homepage if no dedicated contact URL is discoverable.
    return origin or base_url


def find_contact_link_from_links(origin: str, links: Iterable[str]) -> str:
    scored: List[Tuple[int, str]] = []
    for link in links:
        if not link:
            continue
        if origin and not link.startswith(origin):
            continue
        if url_matches_blocklist(link):
            continue
        lowered = link.lower()
        score = 0
        if "contact" in lowered:
            score += 5
        if "about" in lowered:
            score += 2
        if "press" in lowered or "media" in lowered:
            score += 1
        if score > 0:
            scored.append((score, link))

    if not scored:
        return ""
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def classify_location_page_kind(url: str) -> str:
    lowered = normalize_url(url).lower()
    if not lowered:
        return "page"
    if any(hint in lowered for hint in ("/about-author", "/about-the-author", "/author", "/meet-the-author")):
        return "author_page"
    if any(hint in lowered for hint in ("/about", "/bio")):
        return "about_page" if "/about" in lowered else "bio_page"
    if "contact" in lowered:
        return "contact_page"
    if any(hint in lowered for hint in PRESS_PAGE_HINTS + MEDIA_PAGE_HINTS):
        return "press_media_page"
    return "page"


def find_subscribe_link_from_links(origin: str, links: Iterable[str]) -> str:
    scored: List[Tuple[int, str]] = []
    for link in links:
        if not link:
            continue
        if origin and not link.startswith(origin):
            continue
        lowered = link.lower()
        host = urlparse(link).netloc.lower()
        if url_matches_blocklist(link):
            continue
        if any(snippet in host for snippet in NON_AUTHOR_SITE_SNIPPETS):
            continue
        if any(hint in lowered for hint in SUBSCRIBE_BLOCK_HINTS):
            continue
        score = 0
        if any(hint in lowered for hint in SUBSCRIBE_PATH_HINTS):
            score += 5
        if "contact" in lowered:
            score += 1
        if score > 0:
            scored.append((score, link))
    if not scored:
        return ""
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def classify_press_media_url(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    lowered = normalized.lower()
    if any(hint in lowered for hint in PRESS_PAGE_HINTS):
        return "press"
    if any(hint in lowered for hint in MEDIA_PAGE_HINTS):
        return "media"
    return ""


def find_press_media_links_from_links(origin: str, links: Iterable[str]) -> Tuple[str, str]:
    press_scored: List[Tuple[int, str]] = []
    media_scored: List[Tuple[int, str]] = []

    for link in links:
        normalized = normalize_url(link)
        if not normalized:
            continue
        if origin and not normalized.startswith(origin):
            continue
        if url_matches_blocklist(normalized):
            continue
        host = urlparse(normalized).netloc.lower()
        if any(snippet in host for snippet in NON_AUTHOR_SITE_SNIPPETS):
            continue

        kind = classify_press_media_url(normalized)
        if not kind:
            continue
        score = 0
        lowered = normalized.lower()
        if kind == "press":
            if "/press-kit" in lowered or "/presskit" in lowered:
                score += 3
            if "/press" in lowered:
                score += 2
            press_scored.append((score, normalized))
        else:
            if "/media-kit" in lowered or "/mediakit" in lowered:
                score += 3
            if "/media" in lowered:
                score += 2
            media_scored.append((score, normalized))

    press_url = sorted(press_scored, key=lambda item: item[0], reverse=True)[0][1] if press_scored else ""
    media_url = sorted(media_scored, key=lambda item: item[0], reverse=True)[0][1] if media_scored else ""
    return press_url, media_url


def is_actionable_contact_path(url: str, author_website: str, candidate_url: str, strict: bool) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    if url_matches_blocklist(normalized):
        return False

    path = urlparse(normalized).path.lower().strip()
    host = urlparse(normalized).netloc.lower()
    if any(snippet in host for snippet in NON_AUTHOR_SITE_SNIPPETS):
        return False
    if path in ("", "/"):
        return False

    lowered = normalized.lower()
    hints = CONTACT_PATH_HINTS_STRICT if strict else CONTACT_PATH_HINTS
    if any(hint in lowered for hint in hints):
        return True

    if strict:
        return False

    author_origin = get_origin(author_website).lower()
    candidate_origin = get_origin(candidate_url).lower()
    target_origin = get_origin(normalized).lower()
    # Keep non-homepage deep paths on author/candidate domains as acceptable contact paths.
    return bool(path and path != "/" and target_origin in {author_origin, candidate_origin})


def choose_contact_url(
    *,
    author_email: str,
    author_email_source_url: str,
    contact_page: str,
    subscribe_page: str,
    press_kit_url: str,
    media_url: str,
    author_website: str,
    candidate_url: str,
) -> Tuple[str, str]:
    email = (author_email or "").strip()
    email_source = normalize_url(author_email_source_url)
    if email and email_source:
        return email_source, "email_source"

    normalized_contact_page = normalize_url(contact_page)
    if normalized_contact_page and not classify_press_media_url(normalized_contact_page):
        if is_actionable_contact_path(normalized_contact_page, author_website, candidate_url, strict=False):
            return normalized_contact_page, "contact_page"

    normalized_subscribe = normalize_url(subscribe_page)
    if normalized_subscribe:
        return normalized_subscribe, "subscribe"

    normalized_press = normalize_url(press_kit_url)
    normalized_media = normalize_url(media_url)
    if normalized_press:
        return normalized_press, "press_media"
    if normalized_media:
        return normalized_media, "press_media"

    normalized_website = normalize_url(author_website) or normalize_url(candidate_url)
    if normalized_website:
        return normalized_website, "website_fallback"
    return "", ""


def find_listing_candidates(candidate_url: str, links: Iterable[str]) -> List[str]:
    out = []
    normalized_candidate = normalize_listing_url(candidate_url)
    if normalized_candidate and not url_matches_blocklist(normalized_candidate) and is_listing_url(normalized_candidate):
        out.append(normalized_candidate)
    for link in links:
        normalized_link = normalize_listing_url(link)
        if normalized_link and not url_matches_blocklist(normalized_link) and is_listing_url(normalized_link):
            out.append(normalized_link)
    # Preserve order while deduping.
    deduped = []
    seen = set()
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def listing_has_required_signals(text: str, strict: bool) -> bool:
    lowered = text.lower()
    has_price = bool(PRICE_RE.search(text))
    has_format = any(f in lowered for f in FORMAT_HINTS)
    has_buy = any(f in lowered for f in BUY_HINTS)
    if strict:
        return has_format and has_price and has_buy
    return has_format and (has_price or has_buy)


def detect_recency(
    text: str,
    min_year: int,
    max_year: int,
    now: dt.datetime,
) -> bool:
    lowered = text.lower()
    for year in range(min_year, max_year + 1):
        if str(year) in lowered and any(k in lowered for k in ("published", "release", "updated", "new")):
            return True

    # Parse concrete dates and allow anything within the last 24 months.
    date_candidates: List[dt.datetime] = []

    for match in RECENCY_DATE_PATTERNS[0].finditer(text):
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            date_candidates.append(dt.datetime(y, m, d, tzinfo=dt.timezone.utc))
        except ValueError:
            pass

    for match in RECENCY_DATE_PATTERNS[1].finditer(text):
        m, d, y = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            date_candidates.append(dt.datetime(y, m, d, tzinfo=dt.timezone.utc))
        except ValueError:
            pass

    for match in RECENCY_DATE_PATTERNS[2].finditer(text):
        month_name = match.group(1).lower()
        d = int(match.group(2))
        y = int(match.group(3))
        try:
            date_candidates.append(dt.datetime(y, MONTH_MAP[month_name], d, tzinfo=dt.timezone.utc))
        except ValueError:
            pass

    threshold = now - dt.timedelta(days=730)
    return any(candidate >= threshold for candidate in date_candidates)


def clean_email(value: str) -> str:
    email = (value or "").strip().lower()
    email = email.replace("mailto:", "", 1).split("?", 1)[0].strip(".,;:()[]{}<>\"'")
    if not email or len(email) > 254:
        return ""
    if not EMAIL_RE.fullmatch(email):
        return ""
    local, _, domain = email.partition("@")
    if not local or not domain:
        return ""
    if domain.startswith(BLOCKED_EMAIL_DOMAIN_PREFIXES):
        return ""
    if "." not in domain:
        return ""
    tld = domain.rsplit(".", 1)[-1].strip().lower()
    if not tld or tld in BLOCKED_EMAIL_TLDS:
        return ""
    if any(snippet in local for snippet in BLOCKED_EMAIL_LOCALPART_SNIPPETS):
        return ""
    if any(snippet in domain for snippet in BLOCKED_EMAIL_DOMAIN_SNIPPETS):
        return ""
    return email


def compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def find_location_proof(text: str, soup: Optional[BeautifulSoup] = None, author_name: str = "") -> Tuple[str, str, str]:
    compact = compact_text(text)
    for pattern in LOCATION_PATTERNS:
        match = re.search(pattern, compact)
        if match:
            value = match.group(1).strip(" .,-")
            if len(value) >= 3 and len(value.split()) <= 7:
                return value, proof_snippet_for_terms(compact, [match.group(0)]), "text_phrase"

    author_city_state, author_city_state_snippet = detect_author_tied_city_state(compact, author_name=author_name)
    if author_city_state:
        return author_city_state, author_city_state_snippet, "author_city_state"

    footer_match = re.search(
        r"\b([A-Z][A-Za-z .'-]{1,40},\s*(?:[A-Z]{2}|[A-Z][A-Za-z .'-]{2,40})(?:,\s*[A-Z][A-Za-z .'-]{2,40})?)\b",
        compact,
    )
    if footer_match:
        value = footer_match.group(1).strip(" .,-")
        if len(value.split()) <= 8:
            return value, proof_snippet_for_terms(compact, [value]), "footer"

    if soup is not None:
        jsonld_location = extract_location_from_jsonld(soup)
        if jsonld_location:
            return jsonld_location, proof_snippet_for_terms(compact or jsonld_location, [jsonld_location]), "jsonld"

    return "", "", ""


def clean_book_title_candidate(value: str) -> str:
    cleaned = compact_text(value)
    prefixes = (
        "menu",
        "navigation menu",
        "home",
        "books",
        "series",
        "mailing list",
        "free books",
        "about",
        "contact",
        "view more",
        "view all",
        "previous",
        "next",
        "skip to",
        "skip to content",
        "skip to main content",
        "search",
        "search for",
        "bibliography and links",
    )
    changed = True
    while changed and cleaned:
        changed = False
        lowered = cleaned.lower()
        for prefix in prefixes:
            token = prefix + " "
            if lowered.startswith(token):
                cleaned = cleaned[len(token) :].strip()
                changed = True
                break
    return cleaned.strip(" .,-")


def extract_book_title_from_links(base_url: str, soup: BeautifulSoup, author_name: str = "") -> str:
    base_domain = registrable_domain(base_url)
    scored: List[Tuple[int, str]] = []

    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(base_url, anchor.get("href", "").strip()))
        if not href or registrable_domain(href) != base_domain:
            continue

        text = normalize_book_title(anchor.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if any(snippet in lowered for snippet in BOOK_LINK_BLOCK_SNIPPETS):
            continue
        if not is_plausible_book_title(text, author_name=author_name):
            continue

        parsed = urlparse(href)
        path = parsed.path.lower()
        context = compact_text(anchor.parent.get_text(" ", strip=True) if anchor.parent else "")
        score = 0
        if path not in ("", "/"):
            score += 2
        if any(hint in path for hint in ("/book", "/books", "/novel", "/series", "/story")):
            score += 2
        if any(hint in context.lower() for hint in BOOK_LINK_CONTEXT_HINTS):
            score += 2
        words = re.findall(r"[A-Za-z0-9']+", text)
        if 1 <= len(words) <= 6:
            score += 1
        uppercase_like = sum(1 for word in words if word[:1].isupper() or word.isupper())
        if words and uppercase_like >= max(1, len(words) - 1):
            score += 1
        if score > 0:
            scored.append((score, text))

    if not scored:
        return ""
    scored.sort(key=lambda item: (item[0], len(item[1])), reverse=True)
    return normalize_book_title(scored[0][1])


def proof_snippet_for_terms(text: str, terms: Iterable[str], radius: int = 90) -> str:
    compact = compact_text(text)
    lowered = compact.lower()
    for term in terms:
        idx = lowered.find((term or "").lower())
        if idx < 0:
            continue
        start = max(0, idx - radius)
        end = min(len(compact), idx + len(term) + radius)
        return compact[start:end]
    return compact[:180]


def recency_proof_snippet(text: str) -> str:
    compact = compact_text(text)
    for pattern in RECENCY_DATE_PATTERNS:
        match = pattern.search(compact)
        if match:
            start = max(0, match.start() - 90)
            end = min(len(compact), match.end() + 90)
            return compact[start:end]
    for keyword in ("published", "release", "updated", "new release"):
        idx = compact.lower().find(keyword)
        if idx >= 0:
            start = max(0, idx - 90)
            end = min(len(compact), idx + len(keyword) + 90)
            return compact[start:end]
    return compact[:180]


def extract_visible_text_emails(text: str) -> List[str]:
    return dedupe_urls([email for email, _ in extract_visible_text_email_candidates(text)])


def extract_visible_text_email_candidates(text: str) -> List[Tuple[str, str]]:
    candidates: List[Tuple[str, str]] = []
    seen = set()

    for match in EMAIL_RE.findall(text):
        cleaned = clean_email(match)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        candidates.append((cleaned, match))

    for match in OBFUSCATED_EMAIL_RE.finditer(text):
        local, domain, tld = match.groups()
        candidate = f"{local}@{domain}.{tld}"
        cleaned = clean_email(candidate)
        if not cleaned or cleaned in seen:
            continue
        raw_match = compact_text(match.group(0))
        prefix_window = compact_text(text[max(0, match.start() - 35) : match.start()]).lower()
        context_window = compact_text(text[max(0, match.start() - 60) : min(len(text), match.end() + 60)]).lower()
        has_explicit_marker = any(token in raw_match.lower() for token in ("(at)", "[at]", "(dot)", "[dot]", "@"))
        if not has_explicit_marker and not any(hint in prefix_window for hint in EMAIL_OBFUSCATION_CONTEXT_HINTS):
            continue
        if not has_explicit_marker and not any(hint in context_window for hint in EMAIL_OBFUSCATION_CONTEXT_HINTS):
            continue
        seen.add(cleaned)
        candidates.append((cleaned, raw_match))

    return candidates


def extract_visible_text_emails_with_source(
    source_url: str,
    text: str,
    expected_domain: str,
) -> List[Tuple[str, str]]:
    if expected_domain and registrable_domain(source_url) != expected_domain:
        return []
    pairs: List[Tuple[str, str]] = []
    seen = set()
    for email, _ in extract_visible_text_email_candidates(text):
        if email in seen:
            continue
        seen.add(email)
        pairs.append((email, source_url))
    return pairs


def email_quality_for_candidate(email: str, author_website: str, context_text: str) -> str:
    domain = registrable_domain(domain_from_email(email))
    author_domain = registrable_domain(author_website)
    local = email.split("@", 1)[0].lower()
    context = compact_text(context_text).lower()

    if local in ROLE_RISKY_LOCALPARTS:
        return "risky_role"
    if domain and author_domain and domain == author_domain:
        return "same_domain"
    if any(hint in context for hint in EMAIL_CONTEXT_BUSINESS_HINTS):
        return "labeled_off_domain"
    return ""


def extract_page_email_records(source_url: str, text: str, soup: BeautifulSoup, author_website: str) -> List[Dict[str, str]]:
    source_domain = registrable_domain(source_url)
    author_domain = registrable_domain(author_website)
    if author_domain and source_domain and source_domain != author_domain:
        return []

    records: List[Dict[str, str]] = []
    seen = set()

    for email, proof_term in extract_visible_text_email_candidates(text):
        quality = email_quality_for_candidate(email, author_website, text)
        if not quality:
            continue
        if email in seen:
            continue
        seen.add(email)
        records.append(
            {
                "email": email,
                "source_url": source_url,
                "proof_snippet": proof_snippet_for_terms(text, [proof_term, email]),
                "quality": quality,
                "visible_text": "true",
                "method": "visible_text",
            }
        )

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href.lower().startswith("mailto:"):
            continue
        email = clean_email(href)
        if not email or email in seen:
            continue
        context_text = compact_text(" ".join([anchor.get_text(" ", strip=True), anchor.parent.get_text(" ", strip=True) if anchor.parent else ""]))
        visible_context_pairs = extract_visible_text_email_candidates(context_text)
        visible_context_emails = {visible_email for visible_email, _ in visible_context_pairs}
        visible_text = email in visible_context_emails
        quality = email_quality_for_candidate(email, author_website, context_text)
        if not quality:
            continue
        seen.add(email)
        records.append(
            {
                "email": email,
                "source_url": source_url,
                "proof_snippet": proof_snippet_for_terms(context_text or email, [email]),
                "quality": quality,
                "visible_text": "true" if visible_text else "false",
                "method": "mailto_visible" if visible_text else "mailto_only",
            }
        )

    return records


def domain_from_email(email: str) -> str:
    _, _, domain = (email or "").partition("@")
    return domain.lower().strip()


def choose_author_email(emails: Iterable[str], author_website: str, author_name: str = "") -> str:
    preferred = urlparse(author_website or "").netloc.lower().replace("www.", "")
    name_tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", author_name) if len(token) >= 3]
    scored: List[Tuple[int, str]] = []

    for email in dedupe_urls(emails):
        domain = domain_from_email(email)
        local = email.split("@", 1)[0]
        score = 0
        if preferred and (domain == preferred or domain.endswith("." + preferred)):
            score += 10
        elif preferred:
            if domain in PERSONAL_EMAIL_DOMAINS:
                score += 1
            else:
                score -= 1
        if any(token in local for token in name_tokens):
            score += 3
        if any(word in local for word in ("author", "contact", "hello", "info", "book", "books")):
            score += 2
        if any(snippet in domain for snippet in BLOCKED_EMAIL_DOMAIN_SNIPPETS):
            score -= 8
        if domain.startswith(BLOCKED_EMAIL_DOMAIN_PREFIXES):
            score -= 8
        scored.append((score, email))

    if not scored:
        return ""
    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_email = scored[0]
    return best_email if best_score >= -1 else ""


def choose_author_email_record(
    records: Iterable[Dict[str, str]],
    author_website: str,
    author_name: str = "",
    require_visible_text: bool = False,
) -> Dict[str, str]:
    preferred = urlparse(author_website or "").netloc.lower().replace("www.", "")
    name_tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", author_name) if len(token) >= 3]
    scored: List[Tuple[int, Dict[str, str]]] = []

    for record in records:
        email = (record.get("email", "") or "").strip().lower()
        if not email:
            continue
        if require_visible_text and str(record.get("visible_text", "") or "").strip().lower() != "true":
            continue
        domain = domain_from_email(email)
        local = email.split("@", 1)[0]
        quality = (record.get("quality", "") or "").strip()
        score = 0
        if preferred and (domain == preferred or domain.endswith("." + preferred)):
            score += 10
        elif quality == "labeled_off_domain":
            score += 2
        if quality == "risky_role":
            score -= 2
        if any(token in local for token in name_tokens):
            score += 3
        if any(word in local for word in ("author", "contact", "hello", "info", "book", "books")):
            score += 1
        if any(snippet in domain for snippet in BLOCKED_EMAIL_DOMAIN_SNIPPETS):
            score -= 8
        if domain.startswith(BLOCKED_EMAIL_DOMAIN_PREFIXES):
            score -= 8
        scored.append((score, record))

    if not scored:
        return {}
    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_record = scored[0]
    return best_record if best_score >= -1 else {}


def email_record_is_visible_text(record: Dict[str, str]) -> bool:
    return str(record.get("visible_text", "") or "").strip().lower() == "true"


def email_quality_is_fully_verified(quality: str) -> bool:
    return (quality or "").strip() in {"same_domain", "labeled_off_domain"}


def title_keys_match(left: str, right: str) -> bool:
    left_key = normalize_title_key(left)
    right_key = normalize_title_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    return len(shorter) >= 8 and shorter in longer


def normalize_listing_fail_reason(
    *,
    listing_status: str,
    listing_fail_reason: str,
    book_title: str = "",
    listing_title: str = "",
) -> str:
    normalized_status = (listing_status or "").strip().lower()
    normalized_reason = (listing_fail_reason or "").strip()

    if book_title and listing_title and title_keys_match(book_title, listing_title) is False:
        if normalized_status == "verified":
            return "listing_title_mismatch"

    legacy_map = {
        "": "",
        "no_valid_listing": "",
        "no_listing_candidate": "listing_not_found",
        "missing_required_signals": "listing_wrong_format_or_incomplete",
        "listing_fetch_failed": "listing_fetch_failed",
        "listing_fetch_failed_timeout": "listing_fetch_failed_timeout",
        "listing_fetch_failed_404": "listing_fetch_failed_404",
        "listing_fetch_failed_non_html": "listing_fetch_failed_non_html",
        "listing_fetch_failed_other": "listing_fetch_failed_other",
        "listing_not_fetched": "listing_fetch_failed",
    }
    if normalized_reason in legacy_map:
        normalized_reason = legacy_map[normalized_reason]

    if normalized_reason:
        return normalized_reason
    if normalized_status == "missing":
        return "listing_not_found"
    if normalized_status == "fetch_failed":
        return "listing_fetch_failed"
    if normalized_status == "unverified":
        return "listing_wrong_format_or_incomplete"
    if normalized_status == "verified" and book_title and listing_title and not title_keys_match(book_title, listing_title):
        return "listing_title_mismatch"
    return "no_valid_listing"


def should_track_listing_reject_reason(
    *,
    strict_verified_mode: bool,
    listing_status: str,
    listing_fail_reason: str,
    listing_recovery_skipped_reason: str,
    listing_candidates: Sequence[str],
    listing_attempts: Sequence[Dict[str, str]],
) -> bool:
    normalized_reason = normalize_listing_fail_reason(
        listing_status=listing_status,
        listing_fail_reason=listing_fail_reason,
    )
    if (
        strict_verified_mode
        and normalized_reason == "listing_not_found"
        and (listing_recovery_skipped_reason or "").strip() == "missing_strict_email"
        and not listing_candidates
        and not listing_attempts
    ):
        return False
    return True


def row_meets_fully_verified_profile(
    *,
    author_name: str,
    book_title: str,
    book_title_status: str,
    book_title_method: str,
    author_email: str,
    author_email_quality: str,
    author_email_record: Dict[str, str],
    location: str,
    indie_proof_strength: str,
    listing_status: str,
    listing_fail_reason: str,
    recency_status: str,
    listing_title: str,
    require_us_location: bool = True,
    location_decision: str = "",
    best_source_url: str = "",
    require_best_source_url: bool = False,
) -> Tuple[bool, str]:
    if not is_plausible_author_name(author_name):
        return False, "bad_author_name"
    if not author_email or not email_quality_is_fully_verified(author_email_quality):
        return False, "no_visible_author_email"
    if not email_record_is_visible_text(author_email_record):
        return False, "no_visible_author_email"
    if require_us_location:
        normalized_location = (location or "").strip()
        if not normalized_location or normalized_location.lower() in {"unknown", "n/a", "na"}:
            return False, location_decision or "no_location_signal"
        if not is_us_location(normalized_location):
            return False, location_decision or "non_us_location"
    if (indie_proof_strength or "").strip().lower() not in {"onsite", "both"}:
        return False, "no_onsite_indie_proof"
    if (listing_status or "").strip().lower() != "verified":
        return False, normalize_listing_fail_reason(
            listing_status=listing_status,
            listing_fail_reason=listing_fail_reason,
            book_title=book_title,
            listing_title=listing_title,
        )
    if (recency_status or "").strip().lower() != "verified":
        return False, "no_recency_proof"
    if listing_title and not title_keys_match(book_title, listing_title):
        return False, "listing_title_mismatch"
    if require_best_source_url and not (best_source_url or "").strip():
        return False, "missing_best_source_url"
    return True, ""


def row_meets_email_only_profile(
    *,
    author_name: str,
    author_email: str,
    author_email_record: Dict[str, str],
    source_url: str,
) -> Tuple[bool, str]:
    if not is_plausible_author_name(author_name):
        return False, "bad_author_name"
    if not author_email or not email_record_is_visible_text(author_email_record):
        return False, "no_visible_author_email"
    if not (source_url or "").strip():
        return False, "missing_source_url"
    return True, ""


def row_meets_astra_outbound_profile(
    *,
    author_name: str,
    book_title: str,
    book_title_status: str,
    book_title_method: str,
    author_email: str,
    author_email_quality: str,
    author_email_record: Dict[str, str],
    author_email_source_url: str,
    location: str,
    location_proof_url: str,
    indie_proof_strength: str,
    indie_proof_url: str,
    listing_url: str,
    listing_status: str,
    listing_fail_reason: str,
    recency_status: str,
    recency_url: str,
    listing_title: str,
    location_decision: str,
    best_source_url: str,
    require_us_location: bool = True,
) -> Tuple[bool, str]:
    ok, reason = row_meets_fully_verified_profile(
        author_name=author_name,
        book_title=book_title,
        book_title_status=book_title_status,
        book_title_method=book_title_method,
        author_email=author_email,
        author_email_quality=author_email_quality,
        author_email_record=author_email_record,
        location=location,
        indie_proof_strength=indie_proof_strength,
        listing_status=listing_status,
        listing_fail_reason=listing_fail_reason,
        recency_status=recency_status,
        listing_title=listing_title,
        require_us_location=require_us_location,
        location_decision=location_decision,
        best_source_url=best_source_url,
        require_best_source_url=True,
    )
    if not ok:
        return ok, reason
    if not (author_email_source_url or "").strip():
        return False, "no_visible_author_email"
    if require_us_location and not (location_proof_url or "").strip():
        return False, location_decision or "no_location_signal"
    if not (indie_proof_url or "").strip():
        return False, "no_onsite_indie_proof"
    normalized_listing_url = normalize_astra_listing_url(listing_url)
    if not normalized_listing_url or not is_allowed_retailer_url(listing_url):
        return False, normalize_listing_fail_reason(
            listing_status=listing_status,
            listing_fail_reason=listing_fail_reason,
            book_title=book_title,
            listing_title=listing_title,
        )
    if not (recency_url or "").strip():
        return False, "no_recency_proof"
    return True, ""


def normalize_astra_listing_url(value: str) -> str:
    normalized = canonical_listing_key((value or "").strip())
    if normalized:
        return normalized
    return normalize_url(strip_tracking_query_params(value))


def choose_best_astra_source_url(*values: str) -> str:
    for value in values:
        normalized = normalize_url(value)
        if normalized:
            return normalized
    return ""


def _astra_rule_check(
    *,
    passed: bool,
    reason: str = "",
    source_url: str = "",
    snippet: str = "",
    value: str = "",
) -> Dict[str, object]:
    return {
        "passed": bool(passed),
        "reason": str(reason or "").strip(),
        "source_url": str(source_url or "").strip(),
        "snippet": str(snippet or "").strip(),
        "value": str(value or "").strip(),
    }


def build_astra_rule_checks(
    *,
    author_name: str,
    author_email: str,
    author_email_record: Dict[str, str],
    author_email_source_url: str,
    author_email_proof_snippet: str,
    location: str,
    location_decision: str,
    location_proof_url: str,
    location_proof_snippet: str,
    indie_proof_strength: str,
    indie_proof_url: str,
    indie_proof_snippet: str,
    listing_url: str,
    listing_status: str,
    listing_fail_reason: str,
    listing_title: str,
    listing_snippet: str,
    book_title: str,
    recency_status: str,
    recency_url: str,
    recency_proof_snippet: str,
    source_url: str,
    contact_page_url: str,
    author_website: str,
    reject_reason: str,
) -> Dict[str, Dict[str, object]]:
    normalized_listing_url = normalize_astra_listing_url(listing_url)
    listing_url_normalized = normalize_url(listing_url)
    listing_reason = ""
    if (listing_status or "").strip().lower() != "verified":
        listing_reason = normalize_listing_fail_reason(
            listing_status=listing_status,
            listing_fail_reason=listing_fail_reason,
            book_title=book_title,
            listing_title=listing_title,
        )
    listing_passed = bool(
        normalized_listing_url
        and listing_url_normalized
        and is_allowed_retailer_url(listing_url_normalized)
        and (listing_status or "").strip().lower() == "verified"
    )
    best_source_url = choose_best_astra_source_url(
        author_email_source_url,
        contact_page_url,
        author_website,
        source_url,
        location_proof_url,
        indie_proof_url,
        recency_url,
        listing_url,
    )
    visible_public_email_passed = bool(
        (author_email or "").strip()
        and email_record_is_visible_text(author_email_record)
        and (author_email_source_url or "").strip()
    )
    us_location_passed = bool(
        (location or "").strip()
        and (location or "").strip().lower() not in {"unknown", "n/a", "na"}
        and is_us_location(location)
        and (location_proof_url or "").strip()
    )
    indie_passed = bool((indie_proof_strength or "").strip().lower() in {"onsite", "both"} and (indie_proof_url or "").strip())
    recency_passed = bool((recency_status or "").strip().lower() == "verified" and (recency_url or "").strip())
    non_famous_passed = (reject_reason or "").strip() not in {"enterprise_or_famous", "famous_signal", "wikipedia_signal"}
    strict_dedupe_passed = bool(
        is_plausible_author_name(author_name)
        and (author_email or "").strip()
        and normalized_listing_url
    )

    return {
        ASTRA_RULE_VISIBLE_PUBLIC_EMAIL_ONLY: _astra_rule_check(
            passed=visible_public_email_passed,
            reason="" if visible_public_email_passed else "no_visible_author_email",
            source_url=author_email_source_url,
            snippet=author_email_proof_snippet,
            value=(author_email or "").strip().lower(),
        ),
        ASTRA_RULE_US_LOCATION_PROOF_REQUIRED: _astra_rule_check(
            passed=us_location_passed,
            reason="" if us_location_passed else str(location_decision or "no_location_signal"),
            source_url=location_proof_url,
            snippet=location_proof_snippet,
            value=location,
        ),
        ASTRA_RULE_INDIE_SELF_PUB_PROOF_REQUIRED: _astra_rule_check(
            passed=indie_passed,
            reason="" if indie_passed else "no_onsite_indie_proof",
            source_url=indie_proof_url,
            snippet=indie_proof_snippet,
            value=indie_proof_strength,
        ),
        ASTRA_RULE_LISTING_PROOF_REQUIRED: _astra_rule_check(
            passed=listing_passed,
            reason="" if listing_passed else (listing_reason or "listing_not_buyable"),
            source_url=listing_url,
            snippet=listing_snippet,
            value=normalized_listing_url,
        ),
        ASTRA_RULE_NON_FAMOUS_FILTER_REQUIRED: _astra_rule_check(
            passed=non_famous_passed,
            reason="" if non_famous_passed else ((reject_reason or "").strip() or "enterprise_or_famous"),
            source_url=source_url,
            snippet="",
            value=(author_name or "").strip(),
        ),
        ASTRA_RULE_RECENCY_PROOF_REQUIRED: _astra_rule_check(
            passed=recency_passed,
            reason="" if recency_passed else "no_recency_proof",
            source_url=recency_url,
            snippet=recency_proof_snippet,
            value=recency_status,
        ),
        ASTRA_RULE_STRICT_DEDUPE_REQUIRED: _astra_rule_check(
            passed=strict_dedupe_passed,
            reason="" if strict_dedupe_passed else "missing_dedupe_identity",
            source_url=listing_url,
            snippet="",
            value="|".join(
                [
                    normalize_person_name(author_name),
                    (author_email or "").strip().lower(),
                    normalized_listing_url,
                ]
            ).strip("|"),
        ),
        ASTRA_RULE_BEST_SOURCE_URL_REQUIRED: _astra_rule_check(
            passed=bool(best_source_url),
            reason="" if best_source_url else "missing_best_source_url",
            source_url=best_source_url,
            snippet="",
            value=best_source_url,
        ),
    }


def extract_author_name(soup: BeautifulSoup, text: str) -> str:
    goodreads_name = soup.select_one("span.ContributorLink__name")
    if goodreads_name and goodreads_name.get_text(strip=True):
        return goodreads_name.get_text(" ", strip=True)

    author_header = soup.select_one("h1.authorName span")
    if author_header and author_header.get_text(strip=True):
        return author_header.get_text(" ", strip=True)

    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        value = h1.get_text(" ", strip=True)
        if 2 <= len(value) <= 120:
            return value
    title = soup.title.get_text(strip=True) if soup.title else ""
    title = re.split(r"[|\-:]", title)[0].strip()
    if 2 <= len(title) <= 120:
        return title

    match = re.search(r"\bby\s+([A-Z][A-Za-z .'-]{2,80})", text)
    if match:
        return match.group(1).strip()
    return "Unknown Author"


def extract_book_title(soup: BeautifulSoup, text: str) -> str:
    goodreads_title = soup.select_one("h1.Text__title1")
    if goodreads_title and goodreads_title.get_text(strip=True):
        return normalize_book_title(goodreads_title.get_text(" ", strip=True))[:180]

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        return normalize_book_title(og_title["content"].strip())[:180]

    # Prefer second-level heading if available.
    h2 = soup.find("h2")
    if h2 and h2.get_text(strip=True):
        return normalize_book_title(h2.get_text(" ", strip=True))[:180]

    title = soup.title.get_text(strip=True) if soup.title else ""
    return normalize_book_title(title)[:180] if title else "Unknown Book"


def extract_book_title_from_source_text(text: str) -> str:
    compact = compact_text(text)
    title_pattern = (
        r"([A-Z][A-Za-z0-9'&:\-]*"
        r"(?:\s+(?:[A-Z][A-Za-z0-9'&:\-]*|of|the|and|for|to|in|on|a|an)){0,7})"
    )
    series_title_pattern = (
        r"([A-Z][A-Za-z0-9'&:\-]*"
        r"(?:\s+(?:[A-Z][A-Za-z0-9'&:\-]*|of|the|for|to|in|on|a|an)){0,7}"
        r"(?:\s+(?:Series|Saga|Chronicles))?)"
    )
    patterns = [
        r"\bauthor of (?:the [^.,;]{0,60} book, )?" + series_title_pattern,
        r"\bauthor of " + series_title_pattern,
        title_pattern + r"\s+\([^)]*Book\s+\d+[^)]*\)",
        title_pattern + r"\s+is now available\b",
        r"\bauthor of (?:the [^.,;]{0,60} book, )?" + title_pattern,
        r"\bauthor of " + title_pattern,
    ]
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            cleaned = normalize_book_title(match.group(1))
            cleaned = re.sub(r"^(?:search\s+)?for:\s+", "", cleaned, flags=re.IGNORECASE)
            lowered = cleaned.lower()
            if any(marker in lowered for marker in (" saga", " series", " chronicles")):
                for splitter in (" and the ", " plus "):
                    idx = lowered.find(splitter)
                    if idx > 0:
                        cleaned = cleaned[:idx].strip(" ,.-")
                        lowered = cleaned.lower()
                        break
            if lowered.startswith("the "):
                cleaned = "The " + cleaned[4:]
            return cleaned
    return ""


def is_homepage_like_url(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    path = urlparse(normalized).path.strip("/").lower()
    return path in {"", "home", "index", "homepage"}


def normalize_book_title(value: str) -> str:
    cleaned = clean_book_title_candidate(value)
    cleaned = re.sub(
        r"\s*\((?=[^)]*\b(?:series|saga|chronicles|trilogy|book)\b)[^)]*\)$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s*[:\-|]\s*(?:series|saga|chronicles|trilogy)\b$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\(?(?:book|bk)\s*\d+\)?$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+\b(?:series|saga|chronicles|trilogy)\b$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" .,-|:")


def is_series_wrapper_title(value: str) -> bool:
    lowered = normalize_book_title(value).lower()
    original = compact_text(value).lower()
    if not lowered:
        return False
    return any(token in original for token in (" series", " saga", " chronicles", " trilogy")) and " book " not in original


def iter_jsonld_objects(soup: BeautifulSoup) -> Iterable[Dict[str, object]]:
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.IGNORECASE)}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

        stack = payload if isinstance(payload, list) else [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                stack.extend(graph)
            yield item


def jsonld_has_type(obj: Dict[str, object], wanted: str) -> bool:
    value = obj.get("@type")
    if isinstance(value, list):
        return any(str(item).lower() == wanted.lower() for item in value)
    return str(value).lower() == wanted.lower()


def jsonld_type_names(obj: Dict[str, object]) -> set[str]:
    value = obj.get("@type")
    if isinstance(value, list):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    lowered = str(value).strip().lower()
    return {lowered} if lowered else set()


def iter_jsonld_name_values(value: object) -> Iterable[str]:
    if isinstance(value, str):
        cleaned = compact_text(value)
        if cleaned:
            yield cleaned
        return
    if isinstance(value, list):
        for item in value:
            yield from iter_jsonld_name_values(item)
        return
    if isinstance(value, dict) and "name" in value:
        yield from iter_jsonld_name_values(value.get("name"))


def iter_jsonld_itemlist_names(value: object) -> Iterable[str]:
    stack = value if isinstance(value, list) else [value]
    while stack:
        item = stack.pop()
        if isinstance(item, list):
            stack.extend(item)
            continue
        if isinstance(item, dict):
            if "name" in item:
                yield from iter_jsonld_name_values(item.get("name"))
            if "item" in item:
                stack.append(item.get("item"))
            if "itemListElement" in item:
                stack.append(item.get("itemListElement"))
            continue
        if isinstance(item, str):
            cleaned = compact_text(item)
            if cleaned:
                yield cleaned


def extract_jsonld_title_candidates(
    soup: BeautifulSoup,
    author_name: str = "",
    include_itemlists: bool = True,
) -> List[Tuple[str, str]]:
    seen = set()
    candidates: List[Tuple[str, str]] = []

    def add_candidate(title: str, method_kind: str, strong: bool) -> None:
        normalized = normalize_book_title(title)
        if not normalized:
            return
        title_key = normalize_title_key(normalized)
        if not title_key or title_key in seen:
            return
        context = {
            "source_method": "jsonld_book" if strong else "source_title",
            "strong_book_evidence": strong,
            "book_list_count": 2 if method_kind == "itemlist" else 0,
        }
        if not is_plausible_book_title(normalized, author_name=author_name, context=context):
            return
        seen.add(title_key)
        candidates.append((normalized, method_kind))

    for obj in iter_jsonld_objects(soup):
        types = jsonld_type_names(obj)
        if types & {"book", "creativework", "product"}:
            for name in iter_jsonld_name_values(obj.get("name")):
                add_candidate(name, "jsonld", strong=True)
        if include_itemlists and "itemlist" in types:
            for name in iter_jsonld_itemlist_names(obj.get("itemListElement")):
                add_candidate(name, "itemlist", strong=False)

    return candidates


def extract_book_title_from_jsonld(soup: BeautifulSoup, author_name: str = "") -> str:
    for title, method_kind in extract_jsonld_title_candidates(soup, author_name=author_name, include_itemlists=False):
        if method_kind == "jsonld":
            return title
    return ""


def extract_location_from_jsonld(soup: BeautifulSoup) -> str:
    for obj in iter_jsonld_objects(soup):
        if not jsonld_has_type(obj, "Person"):
            continue
        for field in ("homeLocation", "address"):
            value = obj.get(field)
            if isinstance(value, str):
                location = compact_text(value)
                if location:
                    return location
            if isinstance(value, dict):
                pieces = [
                    compact_text(str(value.get(key, "") or ""))
                    for key in ("addressLocality", "addressRegion", "addressCountry")
                ]
                location = ", ".join(piece for piece in pieces if piece)
                if location:
                    return location
    return ""


def score_filtered_url(url: str, hints: Tuple[str, ...]) -> Tuple[int, int, int]:
    parsed = urlparse(url)
    path = parsed.path.lower()
    score = 0
    if hints == BOOK_PAGE_PATH_HINTS:
        hint_weights = {
            "/book": 6,
            "/books": 6,
            "/works": 5,
            "/novels": 5,
            "/titles": 5,
            "/series": 3,
            "/bibliography": 2,
        }
    elif hints == CONTACT_PAGE_PATH_HINTS:
        hint_weights = {
            "/contact": 6,
            "/about": 5,
            "/about-author": 7,
            "/about-the-author": 7,
            "/author": 6,
            "/bio": 5,
            "/press": 4,
            "/media": 4,
            "/newsletter": 4,
            "/subscribe": 4,
        }
    elif hints == LOCATION_PAGE_PATH_HINTS:
        hint_weights = {
            "/about-author": 8,
            "/about-the-author": 8,
            "/author": 7,
            "/bio": 7,
            "/about": 6,
            "/contact": 4,
            "/media": 3,
            "/press": 3,
        }
    elif hints == STORE_LISTING_PAGE_PATH_HINTS:
        hint_weights = {
            "/product-page": 8,
            "/products": 6,
            "/shop": 5,
            "/store": 4,
        }
    else:
        hint_weights = {hint: 4 for hint in hints}

    for hint in hints:
        if hint in path:
            score += hint_weights.get(hint, 4)
    depth = len([part for part in path.split("/") if part])
    if depth >= 2:
        score += 2
    if path.endswith("/index") or path.endswith("/home"):
        score -= 2
    return score, depth, len(path)


def path_contains_segment_hint(path: str, hint: str) -> bool:
    token = (hint or "").strip("/").lower()
    if not token:
        return False
    segments = [segment for segment in path.split("/") if segment]
    return token in segments


def discover_filtered_urls(
    origin: str,
    urls: Iterable[str],
    hints: Tuple[str, ...],
    limit: int,
) -> List[str]:
    domain = registrable_domain(origin)
    scored: List[Tuple[int, int, int, str]] = []
    seen = set()
    for url in urls:
        normalized = normalize_url(url)
        if not normalized or normalized in seen:
            continue
        if registrable_domain(normalized) != domain:
            continue
        path = urlparse(normalized).path.lower()
        if hints == BOOK_PAGE_PATH_HINTS:
            if re.search(r"/20\d{2}/\d{2}/", path):
                continue
            if not any(path_contains_segment_hint(path, hint) for hint in hints):
                continue
        elif hints == LOCATION_PAGE_PATH_HINTS:
            if not any(hint in path for hint in hints):
                continue
        elif not any(hint in path for hint in hints):
            continue
        seen.add(normalized)
        score, depth, path_len = score_filtered_url(normalized, hints)
        scored.append((-score, -depth, path_len, normalized))
    scored.sort()
    return [item[3] for item in scored[: max(0, limit)]]


def discover_book_page_urls(origin: str, candidate_urls: Iterable[str], limit: int) -> List[str]:
    return discover_filtered_urls(origin, candidate_urls, BOOK_PAGE_PATH_HINTS, limit=max(0, limit))


def discover_store_listing_page_urls(origin: str, candidate_urls: Iterable[str], limit: int) -> List[str]:
    return discover_filtered_urls(origin, candidate_urls, STORE_LISTING_PAGE_PATH_HINTS, limit=max(0, limit))


def score_same_domain_listing_support_url(url: str, context_text: str, author_name: str = "", anchor_text: str = "") -> int:
    normalized = normalize_url(url)
    if not normalized:
        return 0
    path = urlparse(normalized).path.lower()
    lowered_context = compact_text(context_text).lower()
    score = 0
    for hint in BOOK_PAGE_PATH_HINTS:
        if hint in path:
            score += 5
    for hint in LISTING_RECOVERY_TEXT_HINTS:
        if hint in lowered_context:
            score += 3
    if any(token in lowered_context for token in ("buy", "amazon", "barnes", "noble")):
        score += 2
    title_hint = extract_book_title_from_source_text(context_text)
    if title_hint and is_plausible_book_title(title_hint, author_name=author_name):
        score += 4
    anchor_label = compact_text(anchor_text)
    if anchor_label and is_plausible_book_title(anchor_label, author_name=author_name):
        score += 4
    depth = len([part for part in path.split("/") if part])
    if depth >= 2:
        score += 1
    if any(bad in path for bad in SUPPORT_PAGE_BLOCK_HINTS):
        score -= 8
    if path in {"", "/"}:
        score -= 5
    return score


def discover_same_domain_listing_support_urls(
    base_url: str,
    soup: BeautifulSoup,
    limit: int,
    author_name: str = "",
) -> List[str]:
    origin = get_origin(base_url)
    domain = registrable_domain(origin or base_url)
    if not domain or soup is None:
        return []

    scored: List[Tuple[int, int, int, str]] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(base_url, anchor.get("href", "")))
        if not href or href in seen:
            continue
        if registrable_domain(href) != domain:
            continue
        if url_matches_blocklist(href):
            continue
        anchor_label = compact_text(anchor.get_text(" ", strip=True))
        label = compact_text(
            " ".join(
                part
                for part in (
                    anchor_label,
                    str(anchor.get("title", "") or ""),
                    str(anchor.get("aria-label", "") or ""),
                    href,
                )
                if part
            )
        )
        score = score_same_domain_listing_support_url(href, label, author_name=author_name, anchor_text=anchor_label)
        if score <= 0:
            continue
        seen.add(href)
        path = urlparse(href).path
        scored.append((-score, -len([part for part in path.split("/") if part]), len(path), href))

    scored.sort()
    return [item[3] for item in scored[: max(0, limit)]]


def canonical_book_recovery_urls(origin: str) -> List[str]:
    normalized_origin = get_origin(origin)
    if not normalized_origin:
        return []
    return [
        normalize_url(urljoin(normalized_origin + "/", slug))
        for slug in ("books", "book", "series", "bibliography", "works", "novels", "titles")
    ]


def build_listing_recovery_urls(
    origin: str,
    candidate_urls: Iterable[str],
    sitemap_urls: Iterable[str],
    hinted_urls: Iterable[str],
    limit: int,
) -> List[str]:
    seed_urls = list(candidate_urls) + list(sitemap_urls)
    discovered = discover_book_page_urls(origin, seed_urls, limit=max(0, max(limit, 4)))
    discovered_store = discover_store_listing_page_urls(origin, seed_urls, limit=max(0, max(limit, 3)))
    canonical_discovered = discover_book_page_urls(
        origin,
        canonical_book_recovery_urls(origin),
        limit=max(0, max(limit, 3)),
    )
    ranked: List[str] = []
    seen = set()
    for url in list(hinted_urls) + discovered + discovered_store + canonical_discovered:
        normalized = normalize_url(url)
        if not normalized or normalized in seen:
            continue
        if registrable_domain(normalized) != registrable_domain(origin):
            continue
        seen.add(normalized)
        ranked.append(normalized)
    return ranked[: max(0, limit)]


def discover_contact_support_urls(origin: str, candidate_urls: Iterable[str], limit: int) -> List[str]:
    return discover_filtered_urls(origin, candidate_urls, CONTACT_PAGE_PATH_HINTS, limit=max(0, limit))


def discover_location_support_urls(origin: str, candidate_urls: Iterable[str], limit: int) -> List[str]:
    return discover_filtered_urls(origin, candidate_urls, LOCATION_PAGE_PATH_HINTS, limit=max(0, limit))


def build_location_recovery_urls(origin: str, links: Iterable[str], sitemap_urls: Iterable[str], limit: int) -> List[str]:
    normalized_origin = get_origin(origin)
    if not normalized_origin:
        return []
    heuristic_urls = [normalize_url(urljoin(normalized_origin.rstrip("/") + "/", hint.lstrip("/"))) for hint in LOCATION_PAGE_PATH_HINTS]
    seeds = [url for url in heuristic_urls if url]
    seeds.extend(sitemap_urls)
    seeds.extend(links)
    return discover_location_support_urls(normalized_origin, seeds, limit=max(0, limit))


def collect_sitemap_urls(
    origin: str,
    fetcher,
    sitemap_cache: Dict[str, List[str]],
    max_sitemaps: int = 8,
) -> List[str]:
    domain = registrable_domain(origin)
    if not domain:
        return []
    if domain in sitemap_cache:
        return sitemap_cache[domain]

    start_url = normalize_url(urljoin(origin.rstrip("/") + "/", "sitemap.xml"))
    if not start_url:
        sitemap_cache[domain] = []
        return []

    queue = [start_url]
    seen_sitemaps = set()
    page_urls: List[str] = []
    while queue and len(seen_sitemaps) < max(1, max_sitemaps):
        sitemap_url = queue.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        result = fetcher(sitemap_url, expect_html=False)
        if not result.ok:
            continue
        urls, sitemap_urls = parse_sitemap_xml(result.text)
        for url in urls:
            normalized = normalize_url(url)
            if normalized and registrable_domain(normalized) == domain:
                page_urls.append(normalized)
        for child in sitemap_urls:
            normalized = normalize_url(child)
            if normalized and registrable_domain(normalized) == domain and normalized not in seen_sitemaps:
                queue.append(normalized)
    sitemap_cache[domain] = dedupe_urls(page_urls)
    return sitemap_cache[domain]


def build_prefilter_page_plan(
    primary_url: str,
    candidate_urls: Iterable[str],
    sitemap_urls: Iterable[str],
    max_book_pages: int,
    max_contact_pages: int,
) -> List[str]:
    planned: List[str] = []
    primary_normalized = normalize_url(primary_url)
    if primary_normalized:
        planned.append(primary_normalized)

    origin = get_origin(primary_normalized or "")
    seeds = list(sitemap_urls) or list(candidate_urls)
    if origin:
        book_urls = discover_book_page_urls(origin, seeds, limit=max(0, max_book_pages))
        location_urls = discover_location_support_urls(origin, seeds, limit=max(0, max_contact_pages))
        contact_urls = discover_contact_support_urls(origin, seeds, limit=max(0, max_contact_pages))
        if book_urls:
            planned.append(book_urls[0])
        if location_urls:
            planned.append(location_urls[0])
        elif contact_urls:
            planned.append(contact_urls[0])

    return dedupe_urls(planned)[:3]


def extract_listing_candidates_from_page(
    source_url: str,
    soup: BeautifulSoup,
    shortlink_expander: Optional[Callable[[str], str]] = None,
) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    seen = set()
    for link in extract_links(source_url, soup):
        normalized = normalize_listing_url(link)
        if normalized and normalized in seen:
            continue
        if normalized and is_listing_url(normalized):
            seen.add(normalized)
            out.append((normalized, source_url, "book_page_outbound"))
            continue
        if shortlink_expander and is_listing_shortlink_url(link):
            expanded = normalize_listing_url(shortlink_expander(link))
            if not expanded or expanded in seen or not is_listing_url(expanded):
                continue
            seen.add(expanded)
            out.append((expanded, source_url, "book_page_shortlink"))
    return out


def directory_indie_proof(
    source_query: str,
    source_type: str,
    source_url: str,
    source_title: str,
    source_snippet: str,
) -> Tuple[str, str, str]:
    lowered = (source_query or "").strip().lower()
    lowered_type = (source_type or "").strip().lower()
    if lowered_type not in {"epic_directory", "ian_directory", "iabx_directory", "indieview_directory"} and not any(
        hint in lowered for hint in DIRECTORY_SOURCE_HINTS
    ):
        return "", "", ""
    snippet = compact_text(source_snippet or source_title)
    return source_url, snippet, "directory"


def extract_book_title_from_book_page(url: str, soup: BeautifulSoup, text: str, author_name: str = "") -> Tuple[str, str]:
    jsonld_title = extract_book_title_from_jsonld(soup, author_name=author_name)
    if jsonld_title and not is_series_wrapper_title(jsonld_title):
        return jsonld_title, "jsonld_book"

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        og_value = normalize_book_title(str(og_title.get("content", "") or ""))
        if is_plausible_book_title(og_value, author_name=author_name) and not is_series_wrapper_title(og_value):
            return og_value, "og"

    for selector in ("h1", "h2"):
        heading = soup.find(selector)
        if not heading or not heading.get_text(strip=True):
            continue
        title = normalize_book_title(heading.get_text(" ", strip=True))
        if is_plausible_book_title(title, author_name=author_name) and not is_series_wrapper_title(title):
            return title, "h1_books_page"

    link_title = normalize_book_title(extract_book_title_from_links(url, soup, author_name=author_name))
    if is_plausible_book_title(link_title, author_name=author_name) and not is_series_wrapper_title(link_title):
        return link_title, "h1_books_page"

    if jsonld_title:
        return jsonld_title, "jsonld_book"
    if og_title and og_title.get("content"):
        fallback_title = normalize_book_title(str(og_title.get("content", "") or ""))
        if is_plausible_book_title(fallback_title, author_name=author_name):
            return fallback_title, "og"
    if link_title and is_plausible_book_title(link_title, author_name=author_name):
        return link_title, "fallback"
    return "", ""


def normalize_title_key(value: str) -> str:
    return re.sub(r"\W+", "", normalize_book_title(value).lower())


def normalize_name_key(value: str) -> str:
    return re.sub(r"\W+", "", compact_text(value).lower())


def is_loose_title_candidate(value: str, author_name: str = "", context: Optional[Dict[str, object]] = None) -> bool:
    title = normalize_book_title(value)
    if not title or not re.search(r"[A-Za-z]", title):
        return False
    if not is_plausible_book_title(title, author_name=author_name, context=context):
        return False
    lowered = title.lower()
    if author_name and lowered == compact_text(author_name).lower():
        return False
    if lowered in GENERIC_BOOK_TITLE_WORDS:
        return False
    if lowered in {"books", "series", "view all", "view more", "learn more", "read more", "free books", "mailing list"}:
        return False
    return True


def title_junk_penalty(value: str) -> int:
    lowered = compact_text(value).lower()
    penalty = 0
    for snippet in BAD_BOOK_TITLE_SNIPPETS:
        if snippet in lowered:
            penalty += 18
    if lowered in GENERIC_BOOK_TITLE_WORDS:
        penalty += 12
    if lowered in {"books", "series", "view all", "view more", "learn more", "read more"}:
        penalty += 12
    return penalty


def title_without_author_suffix(value: str) -> str:
    cleaned = compact_text(value)
    cleaned = re.sub(
        r"\s*(?:[\-|:|])?\s*(?:author|writer|official site|official website|books?)\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return normalize_book_title(cleaned)


def looks_like_person_name_title(title: str, author_name: str = "") -> bool:
    normalized = compact_text(title)
    if not normalized:
        return False
    if PERSON_NAME_STYLE_RE.fullmatch(normalized):
        return True

    tokens = re.findall(r"[A-Za-z]+", normalized)
    if not tokens or len(tokens) > 3:
        return False
    author_tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", author_name)]
    title_tokens = [token.lower() for token in tokens]
    if author_tokens:
        author_surname = author_tokens[-1]
        if title_tokens[-1] == author_surname:
            return True
        if len(title_tokens) <= 2 and all(token in author_tokens for token in title_tokens):
            return True
        initials = "".join(token[0] for token in author_tokens[:-1] if token)
        if normalized.replace(" ", "").replace(".", "").lower().startswith(initials.lower()) and title_tokens[-1] == author_surname:
            return True
    return False


def has_strong_title_evidence(context: Optional[Dict[str, object]]) -> bool:
    if not context:
        return False
    source_method = compact_text(str(context.get("source_method", "") or ""))
    if source_method in STRICT_MASTER_TITLE_METHODS:
        return True
    if bool(context.get("listing_match")):
        return True
    if int(context.get("book_list_count", 0) or 0) >= 2:
        return True
    return bool(context.get("strong_book_evidence"))


def assess_book_title(value: str, author_name: str = "", context: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    title = compact_text(value)
    normalized_title = normalize_title_key(title)
    normalized_author = normalize_name_key(author_name)
    stripped_title = title_without_author_suffix(title)
    stripped_key = normalize_title_key(stripped_title)
    lowered = title.lower()
    strong_evidence = has_strong_title_evidence(context)
    source_method = compact_text(str(context.get("source_method", "") or "")) if context else ""

    if not title or title.lower() == "unknown book":
        return {"plausible": False, "confidence": "weak", "reject_reason": "empty_or_unknown"}
    if not re.search(r"[A-Za-z]", title):
        return {"plausible": False, "confidence": "weak", "reject_reason": "non_alpha"}
    if any(snippet in lowered for snippet in BAD_BOOK_TITLE_SNIPPETS):
        return {"plausible": False, "confidence": "weak", "reject_reason": "bad_snippet"}
    if normalized_author and (normalized_title == normalized_author or stripped_key == normalized_author):
        return {"plausible": False, "confidence": "weak", "reject_reason": "matches_author_name"}
    if lowered in TITLE_NAV_LABELS or stripped_title.lower() in TITLE_NAV_LABELS:
        return {"plausible": False, "confidence": "weak", "reject_reason": "nav_label"}
    if any(
        pattern in lowered
        for pattern in ("| books", "writer |", "official site", "official website", "| author", "author |")
    ):
        return {"plausible": False, "confidence": "weak", "reject_reason": "author_suffix_label"}

    words = re.findall(r"[A-Za-z0-9]+", title)
    if not words:
        return {"plausible": False, "confidence": "weak", "reject_reason": "no_words"}
    if len(words) <= 2 and all(word.lower() in GENERIC_BOOK_TITLE_WORDS for word in words):
        return {"plausible": False, "confidence": "weak", "reject_reason": "generic_label"}
    if not strong_evidence and looks_like_person_name_title(title, author_name=author_name):
        return {"plausible": False, "confidence": "weak", "reject_reason": "looks_like_person_name"}

    if source_method in STRICT_MASTER_TITLE_METHODS:
        return {"plausible": True, "confidence": "strong", "reject_reason": ""}
    return {"plausible": True, "confidence": "weak", "reject_reason": "weak_method"}


def is_secondary_title_label(value: str) -> bool:
    lowered = compact_text(value).lower()
    if any(token in lowered for token in (" season", "episode", " volume", " vol.", " part ", " issue ")):
        return True
    book_match = re.search(r"\bbook\s+(\d+)\b", lowered)
    if book_match and int(book_match.group(1)) > 1:
        return True
    hash_match = re.search(r"#\s*(\d+)\b", lowered)
    if hash_match and int(hash_match.group(1)) > 1:
        return True
    return False


def is_weak_book_title(value: str, author_name: str = "") -> bool:
    title = compact_text(value)
    if not title:
        return True
    assessment = assess_book_title(title, author_name=author_name)
    lowered = title.lower()
    if not assessment["plausible"]:
        return True
    if any(label in lowered for label in WEAK_BOOK_TITLE_LABELS):
        return True

    author_tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", author_name) if len(token) >= 2]
    if any(token in lowered for token in (" official website", " official site", " author website", " author site")):
        return True
    if lowered.startswith("author ") or lowered.startswith("get in touch "):
        return True
    if any(token in lowered for token in ("books by ", "author of ")):
        return True

    parts = [part.strip() for part in re.split(r"\s*[-|]\s*", title) if part.strip()]
    if len(parts) >= 2 and author_tokens:
        left = " ".join(parts[:-1]).lower()
        right = parts[-1].lower()
        if any(label in left for label in WEAK_BOOK_TITLE_LABELS) and any(token in right for token in author_tokens):
            return True
        if any(label in lowered for label in WEAK_BOOK_TITLE_LABELS) and any(token in lowered for token in author_tokens):
            return True

    return False


def serialize_title_candidates(candidates: Iterable[Dict[str, object]], limit: int = 3) -> str:
    payload = []
    for item in list(candidates)[: max(0, limit)]:
        payload.append(
            {
                "title": compact_text(str(item.get("title", "") or "")),
                "method": compact_text(str(item.get("method", "") or "")),
                "score": int(item.get("score", 0) or 0),
                "source_url": compact_text(str(item.get("source_url", "") or "")),
            }
        )
    if not payload:
        return ""
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def add_title_candidate(
    candidates: List[TitleCandidate],
    title: str,
    source_url: str,
    method: str,
    author_name: str = "",
    page_kind: str = "",
    position: int = 0,
    raw_title: str = "",
) -> None:
    cleaned = normalize_book_title(title)
    candidate_context: Dict[str, object] = {}
    if method in {"book_detail_jsonld", "jsonld_book"}:
        candidate_context["source_method"] = "jsonld_book"
    elif method.startswith("listing_"):
        candidate_context["source_method"] = "listing_match"
    if not is_loose_title_candidate(cleaned, author_name=author_name, context=candidate_context):
        return
    candidates.append(
        TitleCandidate(
            title=cleaned,
            source_url=source_url,
            method=method,
            page_kind=page_kind,
            position=position,
            raw_title=compact_text(raw_title or title),
        )
    )


def add_card_context_candidate(
    candidates: List[TitleCandidate],
    title: str,
    source_url: str,
    method: str,
    author_name: str = "",
    page_kind: str = "",
    position: int = 0,
) -> None:
    token_count = len(re.findall(r"[A-Za-z0-9]+", compact_text(title)))
    if token_count == 0 or token_count > 12:
        return
    add_title_candidate(
        candidates,
        title,
        source_url,
        method,
        author_name=author_name,
        page_kind=page_kind,
        position=position,
        raw_title=title,
    )


def classify_title_page_kind(url: str, soup: Optional[BeautifulSoup]) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    path = urlparse(normalized).path.lower().strip("/")
    if any(path_contains_segment_hint("/" + path, hint) for hint in ("/books", "/works", "/novels", "/titles")) and "/" in path:
        return "book_detail"
    if path_contains_segment_hint("/" + path, "/books"):
        return "books_index"
    if path_contains_segment_hint("/" + path, "/series"):
        return "series_index"
    if soup is not None and is_homepage_like_url(normalized):
        for heading in soup.find_all(["h2", "h3"]):
            text = compact_text(heading.get_text(" ", strip=True)).lower()
            if text in {"books", "series"}:
                return "homepage_books"
    return ""


def extract_title_candidates_from_source_metadata(
    source_title: str,
    source_snippet: str,
    source_url: str,
    author_name: str = "",
) -> List[TitleCandidate]:
    candidates: List[TitleCandidate] = []
    combined = compact_text(" ".join(part for part in (source_title, source_snippet) if part))

    quoted_matches = re.findall(r"[\"'“”]([^\"'“”]{2,140})[\"'“”]", combined)
    for idx, match in enumerate(quoted_matches):
        add_title_candidate(
            candidates,
            match,
            source_url,
            "source_quote",
            author_name=author_name,
            page_kind="directory",
            position=idx,
            raw_title=match,
        )

    source_snippet_title = extract_book_title_from_source_text(combined)
    if source_snippet_title:
        add_title_candidate(
            candidates,
            source_snippet_title,
            source_url,
            "source_snippet",
            author_name=author_name,
            page_kind="directory",
            position=0,
            raw_title=source_snippet_title,
        )

    cleaned_source_title = normalize_book_title(clean_source_author_label(source_title))
    if cleaned_source_title:
        add_title_candidate(
            candidates,
            cleaned_source_title,
            source_url,
            "source_title",
            author_name=author_name,
            page_kind="directory",
            position=0,
            raw_title=source_title,
        )

    return candidates


def extract_title_candidates_from_books_page(
    url: str,
    soup: BeautifulSoup,
    author_name: str = "",
    page_kind: str = "",
) -> List[TitleCandidate]:
    candidates: List[TitleCandidate] = []
    base_domain = registrable_domain(url)
    position = 0

    for idx, (title, method_kind) in enumerate(
        extract_jsonld_title_candidates(soup, author_name=author_name, include_itemlists=True)
    ):
        method = "books_page_jsonld" if method_kind == "jsonld" else "books_page_jsonld_itemlist"
        add_title_candidate(
            candidates,
            title,
            url,
            method,
            author_name=author_name,
            page_kind=page_kind,
            position=idx,
            raw_title=title,
        )

    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(url, anchor.get("href", "").strip()))
        if not href or registrable_domain(href) != base_domain:
            continue
        href_path = urlparse(href).path.lower()
        if not any(path_contains_segment_hint(href_path, hint) for hint in ("/books", "/works", "/novels", "/titles")):
            continue
        anchor_text = compact_text(anchor.get_text(" ", strip=True))
        if anchor_text:
            add_title_candidate(
                candidates,
                anchor_text,
                url,
                "books_page_link",
                author_name=author_name,
                page_kind=page_kind,
                position=position,
                raw_title=anchor_text,
            )
        for attr, method in (("title", "books_page_card_title_attr"), ("aria-label", "books_page_card_aria")):
            value = anchor.get(attr)
            if value:
                add_card_context_candidate(
                    candidates,
                    str(value),
                    url,
                    method,
                    author_name=author_name,
                    page_kind=page_kind,
                    position=position,
                )
        img = anchor.find("img", alt=True)
        if img and img.get("alt"):
            add_title_candidate(
                candidates,
                img.get("alt", ""),
                url,
                "books_page_img_alt",
                author_name=author_name,
                page_kind=page_kind,
                position=position,
                raw_title=img.get("alt", ""),
            )
        if img is not None:
            for attr, method in (("title", "books_page_card_title_attr"), ("aria-label", "books_page_card_aria")):
                value = img.get(attr)
                if value:
                    add_card_context_candidate(
                        candidates,
                        str(value),
                        url,
                        method,
                        author_name=author_name,
                        page_kind=page_kind,
                        position=position,
                    )
        container = anchor.find_parent(["article", "figure", "li", "section", "div"])
        if container is not None:
            seen_context = set()
            figcaption = container.find("figcaption")
            if figcaption and figcaption.get_text(strip=True):
                figcaption_text = compact_text(figcaption.get_text(" ", strip=True))
                if figcaption_text:
                    add_card_context_candidate(
                        candidates,
                        figcaption_text,
                        url,
                        "books_page_card_figcaption",
                        author_name=author_name,
                        page_kind=page_kind,
                        position=position,
                    )
                    seen_context.add(figcaption_text.lower())
            for selector in ("h2", "h3", "h4", "p", "span"):
                for element in container.find_all(selector, limit=2):
                    if not element.get_text(strip=True):
                        continue
                    element_text = compact_text(element.get_text(" ", strip=True))
                    if not element_text or element_text.lower() in seen_context:
                        continue
                    add_card_context_candidate(
                        candidates,
                        element_text,
                        url,
                        "books_page_card_text",
                        author_name=author_name,
                        page_kind=page_kind,
                        position=position,
                    )
                    seen_context.add(element_text.lower())
        position += 1

    heading_position = 0
    for heading in soup.find_all(["h2", "h3"]):
        heading_text = compact_text(heading.get_text(" ", strip=True))
        if not heading_text or heading_text.lower() in {"books", "series"}:
            continue
        href = ""
        heading_link = heading.find("a", href=True)
        if heading_link:
            href = normalize_url(urljoin(url, heading_link.get("href", "").strip())) or ""
        elif heading.find_next("a", href=True):
            next_link = heading.find_next("a", href=True)
            href = normalize_url(urljoin(url, next_link.get("href", "").strip())) if next_link else ""
        href_path = urlparse(href).path.lower() if href else ""
        if href and registrable_domain(href) == registrable_domain(url) and any(
            path_contains_segment_hint(href_path, hint) for hint in ("/books", "/works", "/novels", "/titles")
        ):
            add_title_candidate(
                candidates,
                heading_text,
                url,
                "books_page_heading",
                author_name=author_name,
                page_kind=page_kind,
                position=heading_position,
                raw_title=heading_text,
            )
            heading_position += 1

    list_position = 0
    for item in soup.find_all("li"):
        anchor = item.find("a", href=True)
        if not anchor:
            continue
        href = normalize_url(urljoin(url, anchor.get("href", "").strip()))
        if not href or registrable_domain(href) != registrable_domain(url):
            continue
        href_path = urlparse(href).path.lower()
        if not any(path_contains_segment_hint(href_path, hint) for hint in ("/books", "/works", "/novels", "/titles")):
            continue
        list_text = compact_text(item.get_text(" ", strip=True))
        add_title_candidate(
            candidates,
            list_text,
            url,
            "books_page_list",
            author_name=author_name,
            page_kind=page_kind,
            position=list_position,
            raw_title=list_text,
        )
        list_position += 1

    return candidates


def extract_title_candidates_from_detail_page(
    url: str,
    soup: BeautifulSoup,
    text: str,
    author_name: str = "",
    page_kind: str = "book_detail",
) -> List[TitleCandidate]:
    candidates: List[TitleCandidate] = []
    method_prefix = "listing" if page_kind == "listing" else "book_detail"

    for jsonld_title, _ in extract_jsonld_title_candidates(soup, author_name=author_name, include_itemlists=False):
        add_title_candidate(
            candidates,
            jsonld_title,
            url,
            f"{method_prefix}_jsonld",
            author_name=author_name,
            page_kind=page_kind,
            raw_title=jsonld_title,
        )

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        og_value = compact_text(str(og_title.get("content", "") or ""))
        add_title_candidate(
            candidates,
            og_value,
            url,
            f"{method_prefix}_og",
            author_name=author_name,
            page_kind=page_kind,
            raw_title=og_value,
        )

    for idx, selector in enumerate(("h1", "h2")):
        heading = soup.find(selector)
        if not heading or not heading.get_text(strip=True):
            continue
        heading_text = compact_text(heading.get_text(" ", strip=True))
        add_title_candidate(
            candidates,
            heading_text,
            url,
            f"{method_prefix}_{selector}",
            author_name=author_name,
            page_kind=page_kind,
            position=idx,
            raw_title=heading_text,
        )

    link_title = extract_book_title_from_links(url, soup, author_name=author_name)
    if link_title:
        add_title_candidate(
            candidates,
            link_title,
            url,
            f"{method_prefix}_link",
            author_name=author_name,
            page_kind=page_kind,
            raw_title=link_title,
        )

    return candidates


def resolve_listing_title_oracle(
    listing_url: str,
    listing_text: str,
    listing_soup: BeautifulSoup,
    author_name: str = "",
) -> Dict[str, object]:
    listing_candidates: List[Tuple[str, str, int]] = []
    for title, _ in extract_jsonld_title_candidates(listing_soup, author_name=author_name, include_itemlists=False):
        listing_candidates.append((title, "jsonld", 64))

    og_title = listing_soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        listing_candidates.append((compact_text(str(og_title.get("content", "") or "")), "og", 62))

    for selector, score in (("h1", 60), ("h2", 58)):
        heading = listing_soup.find(selector)
        if heading and heading.get_text(strip=True):
            listing_candidates.append((compact_text(heading.get_text(" ", strip=True)), selector, score))

    assessed: List[Dict[str, object]] = []
    seen = set()
    for title, raw_method, score in listing_candidates:
        normalized = normalize_book_title(title)
        title_key = normalize_title_key(normalized)
        if not normalized or not title_key or title_key in seen:
            continue
        seen.add(title_key)
        assessment = assess_book_title(
            normalized,
            author_name=author_name,
            context={"source_method": "listing_title_oracle", "strong_book_evidence": True},
        )
        assessed.append(
            {
                "title": normalized,
                "source_url": listing_url,
                "method": "listing_title_oracle",
                "raw_method": raw_method,
                "score": score,
                "confidence": assessment["confidence"],
                "reject_reason": assessment["reject_reason"],
                "plausible": assessment["plausible"],
            }
        )

    for item in assessed:
        if item["plausible"]:
            return {
                "title": str(item["title"]),
                "source_url": str(item["source_url"]),
                "method": "listing_title_oracle",
                "score": int(item["score"]),
                "confidence": "strong",
                "status": "ok",
                "reject_reason": "",
                "top_candidates": assessed[:5],
            }
    return {}


def extract_title_candidates_from_recency_proof(
    recency_url: str,
    recency_proof: str,
    author_name: str = "",
) -> List[TitleCandidate]:
    candidates: List[TitleCandidate] = []
    recency_title = extract_book_title_from_source_text(recency_proof)
    if recency_title:
        add_title_candidate(
            candidates,
            recency_title,
            recency_url,
            "recency_snippet",
            author_name=author_name,
            page_kind="recency",
            raw_title=recency_title,
        )
    return candidates


def score_title_candidate(
    candidate: TitleCandidate,
    source_anchor_keys: set[str],
    listing_anchor_keys: set[str],
    recency_anchor_keys: set[str],
    has_clean_primary_alternative: bool,
) -> int:
    title_key = normalize_title_key(candidate.title)
    score = TITLE_METHOD_BASE_SCORES.get(candidate.method, 10)
    score += TITLE_PAGE_KIND_SCORES.get(candidate.page_kind, 0)
    score += max(0, 6 - min(candidate.position, 6))
    if title_key in source_anchor_keys and not candidate.method.startswith("source_"):
        score += 5
    if title_key in listing_anchor_keys and not candidate.method.startswith("listing_"):
        score += 7
    if title_key in recency_anchor_keys and candidate.method != "recency_snippet":
        score += 4
    if is_series_wrapper_title(candidate.raw_title or candidate.title) and has_clean_primary_alternative:
        score -= 15
    if is_secondary_title_label(candidate.raw_title or candidate.title) and has_clean_primary_alternative:
        score -= 10
    score -= title_junk_penalty(candidate.raw_title or candidate.title)
    return score


def resolve_primary_book_title(
    candidate_url: str,
    title_pages: Iterable[Tuple[str, str, BeautifulSoup]],
    source_title: str,
    source_snippet: str,
    source_url: str,
    listing_url: str = "",
    listing_status: str = "",
    listing_page: Optional[Tuple[str, str, BeautifulSoup]] = None,
    recency_url: str = "",
    recency_proof: str = "",
    author_name: str = "",
) -> Dict[str, object]:
    raw_candidates: List[TitleCandidate] = []

    if listing_status in {"verified", "unverified"} and listing_page is not None:
        listing_oracle = resolve_listing_title_oracle(
            listing_page[0],
            listing_page[1],
            listing_page[2],
            author_name=author_name,
        )
        if listing_oracle:
            return listing_oracle

    source_candidates = extract_title_candidates_from_source_metadata(
        source_title=source_title,
        source_snippet=source_snippet,
        source_url=source_url or candidate_url,
        author_name=author_name,
    )
    raw_candidates.extend(source_candidates)

    seen_pages = set()
    for page_url, page_text, page_soup in title_pages:
        normalized = normalize_url(page_url)
        if not normalized or page_soup is None or normalized in seen_pages:
            continue
        seen_pages.add(normalized)
        page_kind = classify_title_page_kind(normalized, page_soup)
        if normalized == normalize_url(candidate_url) and not page_kind and not is_homepage_like_url(normalized):
            candidate_page_title = extract_book_title(page_soup, page_text)
            if candidate_page_title:
                add_title_candidate(
                    raw_candidates,
                    candidate_page_title,
                    normalized,
                    "candidate_page",
                    author_name=author_name,
                    raw_title=candidate_page_title,
                )
        if page_kind in {"homepage_books", "books_index", "series_index"}:
            raw_candidates.extend(
                extract_title_candidates_from_books_page(
                    normalized,
                    page_soup,
                    author_name=author_name,
                    page_kind=page_kind,
                )
            )
        if page_kind == "book_detail":
            raw_candidates.extend(
                extract_title_candidates_from_detail_page(
                    normalized,
                    page_soup,
                    page_text,
                    author_name=author_name,
                    page_kind=page_kind,
                )
            )

    listing_candidates: List[TitleCandidate] = []
    if listing_status == "verified" and listing_page is not None:
        listing_candidates = extract_title_candidates_from_detail_page(
            listing_page[0],
            listing_page[2],
            listing_page[1],
            author_name=author_name,
            page_kind="listing",
        )
        raw_candidates.extend(listing_candidates)

    recency_candidates = extract_title_candidates_from_recency_proof(
        recency_url=recency_url,
        recency_proof=recency_proof,
        author_name=author_name,
    )
    raw_candidates.extend(recency_candidates)

    if not raw_candidates:
        return {
            "title": "",
            "source_url": "",
            "method": "fallback",
            "score": 0,
            "confidence": "weak",
            "status": "missing_or_weak",
            "reject_reason": "no_title_candidate",
            "top_candidates": [],
        }

    source_anchor_keys = {normalize_title_key(candidate.title) for candidate in source_candidates if normalize_title_key(candidate.title)}
    listing_anchor_keys = {normalize_title_key(candidate.title) for candidate in listing_candidates if normalize_title_key(candidate.title)}
    recency_anchor_keys = {normalize_title_key(candidate.title) for candidate in recency_candidates if normalize_title_key(candidate.title)}
    books_index_title_counts: Dict[str, set[str]] = {}
    for candidate in raw_candidates:
        title_key = normalize_title_key(candidate.title)
        if not title_key:
            continue
        if candidate.page_kind not in {"homepage_books", "books_index", "series_index"}:
            continue
        if not candidate.method.startswith("books_page_"):
            continue
        books_index_title_counts.setdefault(candidate.source_url, set()).add(title_key)
    books_index_card_keys = {
        normalize_title_key(candidate.title)
        for candidate in raw_candidates
        if candidate.page_kind in {"homepage_books", "books_index", "series_index"}
        and candidate.method.startswith("books_page_")
        and len(books_index_title_counts.get(candidate.source_url, set())) >= 2
    }
    has_clean_primary_alternative = any(
        not is_series_wrapper_title(candidate.raw_title or candidate.title)
        and not is_secondary_title_label(candidate.raw_title or candidate.title)
        and title_junk_penalty(candidate.raw_title or candidate.title) == 0
        for candidate in raw_candidates
    )

    occurrence_counts = Counter(normalize_title_key(candidate.title) for candidate in raw_candidates if normalize_title_key(candidate.title))
    grouped: Dict[str, Dict[str, object]] = {}
    for candidate in raw_candidates:
        title_key = normalize_title_key(candidate.title)
        if not title_key:
            continue
        single_score = score_title_candidate(
            candidate,
            source_anchor_keys=source_anchor_keys,
            listing_anchor_keys=listing_anchor_keys,
            recency_anchor_keys=recency_anchor_keys,
            has_clean_primary_alternative=has_clean_primary_alternative,
        )
        entry = grouped.setdefault(
            title_key,
            {
                "title": candidate.title,
                "source_url": candidate.source_url,
                "method": candidate.method,
                "best_single_score": single_score,
                "score": 0,
                "occurrences": 0,
                "methods": set(),
                "page_kinds": set(),
                "first_position": candidate.position,
                "source_match": title_key in source_anchor_keys,
                "listing_match": title_key in listing_anchor_keys,
                "recency_match": title_key in recency_anchor_keys,
                "books_index_card": title_key in books_index_card_keys,
            },
        )
        entry["occurrences"] = int(entry["occurrences"]) + 1
        entry["methods"].add(candidate.method)
        if candidate.page_kind:
            entry["page_kinds"].add(candidate.page_kind)
        entry["first_position"] = min(int(entry["first_position"]), candidate.position)
        entry["books_index_card"] = bool(entry["books_index_card"]) or title_key in books_index_card_keys
        if single_score > int(entry["best_single_score"]):
            entry["best_single_score"] = single_score
            entry["title"] = candidate.title
            entry["source_url"] = candidate.source_url
            entry["method"] = candidate.method

    ranked: List[Dict[str, object]] = []
    for title_key, entry in grouped.items():
        score = int(entry["best_single_score"])
        score += min(18, max(0, occurrence_counts[title_key] - 1) * 4)
        score += min(6, max(0, len(entry["methods"]) - 1) * 2)
        if entry["source_match"]:
            score += 4
        if entry["listing_match"]:
            score += 5
        if entry["books_index_card"]:
            score += 6
        if entry["recency_match"]:
            score += 4
        canonical_method = str(entry["method"])
        if entry["listing_match"]:
            canonical_method = "listing_match"
        elif "book_detail_jsonld" in entry["methods"]:
            canonical_method = "jsonld_book"
        elif entry["books_index_card"]:
            canonical_method = "books_index_card"
        ranked.append(
            {
                "title": entry["title"],
                "source_url": entry["source_url"],
                "method": canonical_method,
                "raw_method": entry["method"],
                "score": score,
                "occurrences": occurrence_counts[title_key],
                "source_match": entry["source_match"],
                "listing_match": entry["listing_match"],
                "books_index_card": entry["books_index_card"],
                "first_position": entry["first_position"],
            }
        )

    ranked.sort(
        key=lambda item: (
            int(item["score"]),
            int(bool(item["listing_match"])),
            int(bool(item["source_match"])),
            int(item["occurrences"]),
            -int(item["first_position"]),
            -len(str(item["title"])),
        ),
        reverse=True,
    )

    assessed_candidates: List[Dict[str, object]] = []
    for item in ranked:
        context = {
            "source_method": item["method"],
            "listing_match": item.get("listing_match", False),
            "book_list_count": 2 if item.get("books_index_card", False) else 0,
            "strong_book_evidence": item["method"] in STRICT_MASTER_TITLE_METHODS,
        }
        assessment = assess_book_title(str(item["title"]), author_name=author_name, context=context)
        assessed_candidates.append(
            {
                **item,
                "confidence": assessment["confidence"],
                "reject_reason": assessment["reject_reason"],
                "plausible": assessment["plausible"],
            }
        )

    for item in assessed_candidates:
        if item["plausible"] and item["confidence"] == "strong":
            return {
                "title": str(item["title"]),
                "source_url": str(item["source_url"]),
                "method": str(item["method"]),
                "score": int(item["score"]),
                "confidence": "strong",
                "status": "ok",
                "reject_reason": "",
                "top_candidates": assessed_candidates[:5],
            }

    for item in assessed_candidates:
        if item["plausible"]:
            return {
                "title": str(item["title"]),
                "source_url": str(item["source_url"]),
                "method": str(item["method"]),
                "score": int(item["score"]),
                "confidence": str(item["confidence"]),
                "status": "missing_or_weak",
                "reject_reason": str(item["reject_reason"] or "weak_method"),
                "top_candidates": assessed_candidates[:5],
            }

    return {
        "title": "",
        "source_url": "",
        "method": "fallback",
        "score": 0,
        "confidence": "weak",
        "status": "missing_or_weak",
        "reject_reason": str(assessed_candidates[0]["reject_reason"]) if assessed_candidates else "no_plausible_title",
        "top_candidates": assessed_candidates[:5],
    }


def choose_ranked_nonweak_title(
    ranked_candidates: Iterable[Dict[str, object]],
    author_name: str = "",
) -> Dict[str, object]:
    for item in ranked_candidates:
        title = compact_text(str(item.get("title", "") or ""))
        if not title:
            continue
        if is_weak_book_title(title, author_name=author_name):
            continue
        if not is_plausible_book_title(title, author_name=author_name):
            continue
        return {
            "title": title,
            "source_url": compact_text(str(item.get("source_url", "") or "")),
            "method": compact_text(str(item.get("method", "") or "fallback")),
            "score": int(item.get("score", 0) or 0),
        }
    return {}


def find_explicit_book_page_title(
    title_pages: Iterable[Tuple[str, str, BeautifulSoup]],
    author_name: str = "",
) -> Dict[str, object]:
    detail_pages: List[Tuple[str, str, BeautifulSoup]] = []
    index_pages: List[Tuple[str, str, BeautifulSoup]] = []

    for page_url, page_text, page_soup in title_pages:
        page_kind = classify_title_page_kind(page_url, page_soup)
        if page_kind == "book_detail":
            detail_pages.append((page_url, page_text, page_soup))
        elif page_kind in {"homepage_books", "books_index", "series_index"}:
            index_pages.append((page_url, page_text, page_soup))

    for page_url, page_text, page_soup in detail_pages:
        title, method = extract_book_title_from_book_page(page_url, page_soup, page_text, author_name=author_name)
        if title and not is_weak_book_title(title, author_name=author_name):
            score = {
                "jsonld_book": TITLE_METHOD_BASE_SCORES.get("book_detail_jsonld", 30),
                "og": TITLE_METHOD_BASE_SCORES.get("book_detail_og", 26),
                "h1_books_page": TITLE_METHOD_BASE_SCORES.get("book_detail_h1", 24),
                "fallback": TITLE_METHOD_BASE_SCORES.get("books_page_link", 18),
            }.get(method, 24)
            return {
                "title": title,
                "source_url": page_url,
                "method": method,
                "score": score,
            }

    for page_url, _, page_soup in index_pages:
        page_kind = classify_title_page_kind(page_url, page_soup)
        for candidate in extract_title_candidates_from_books_page(
            page_url,
            page_soup,
            author_name=author_name,
            page_kind=page_kind,
        ):
            if is_weak_book_title(candidate.title, author_name=author_name):
                continue
            if not is_plausible_book_title(candidate.title, author_name=author_name):
                continue
            return {
                "title": candidate.title,
                "source_url": candidate.source_url,
                "method": candidate.method,
                "score": TITLE_METHOD_BASE_SCORES.get(candidate.method, 18),
            }

    return {}


def classify_listing_page_kind(url: str) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return "listing"
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host == "amazon.com" or host.endswith(".amazon.com"):
        return "amazon_product" if is_allowed_retailer_url(normalized) else "amazon_other"
    if host == "barnesandnoble.com" or host.endswith(".barnesandnoble.com"):
        return "barnesandnoble_product" if "/w/" in path else "barnesandnoble_other"
    return "listing"


def collect_listing_title_hints(
    candidate_url: str,
    evidence_pages: Iterable[Tuple[str, str, BeautifulSoup]],
    source_title: str,
    source_snippet: str,
    author_name: str = "",
) -> List[str]:
    hints: List[str] = []
    seen = set()

    def add_hint(value: str) -> None:
        normalized = normalize_book_title(value)
        key = normalize_title_key(normalized)
        if not normalized or not key or key in seen:
            return
        seen.add(key)
        hints.append(normalized)

    source_hint = extract_book_title_from_source_text(" ".join(part for part in (source_title, source_snippet) if part))
    if source_hint:
        add_hint(source_hint)

    for page_url, page_text, page_soup in evidence_pages:
        page_kind = classify_title_page_kind(page_url, page_soup)
        if page_kind == "book_detail":
            title, _ = extract_book_title_from_book_page(page_url, page_soup, page_text, author_name=author_name)
            if title:
                add_hint(title)
            continue
        if page_kind in {"homepage_books", "books_index", "series_index"}:
            explicit = find_explicit_book_page_title([(page_url, page_text, page_soup)], author_name=author_name)
            if explicit.get("title"):
                add_hint(str(explicit["title"]))
            continue
        if normalize_url(page_url) == normalize_url(candidate_url) and not is_homepage_like_url(page_url):
            candidate_page_title = extract_book_title(page_soup, page_text)
            if candidate_page_title:
                add_hint(candidate_page_title)

    return hints


def listing_signal_terms(text: str) -> Dict[str, str]:
    lowered = text.lower()
    price_match = PRICE_RE.search(text)
    return {
        "price": compact_text(price_match.group(0)) if price_match else "",
        "format": next((hint for hint in FORMAT_HINTS if hint in lowered), ""),
        "buy": next((hint for hint in BUY_HINTS if hint in lowered), ""),
        "unavailable": next((hint for hint in LISTING_UNAVAILABLE_HINTS if hint in lowered), ""),
        "reseller": next((hint for hint in LISTING_RESELLER_HINTS if hint in lowered), ""),
        "blocked": next((hint for hint in LISTING_BLOCKED_HINTS if hint in lowered), ""),
    }


def classify_listing_fetch_failure(result: FetchResult) -> Tuple[str, str]:
    reason = (result.failure_reason or "").strip().lower()
    if reason in {"robots_disallow", "403", "429"}:
        return "listing_redirect_or_blocked", reason
    if reason == "timeout":
        return "listing_fetch_failed_timeout", reason
    if reason == "404":
        return "listing_fetch_failed_404", reason
    if reason == "non_html":
        return "listing_fetch_failed_non_html", reason
    return "listing_fetch_failed_other", reason or "fetch_failed"


def listing_has_interstitial_state(text: str, page_kind: str, listing_title: str) -> bool:
    lowered = compact_text(text).lower()
    if not lowered:
        return False
    if page_kind != "amazon_product":
        return False
    if listing_title:
        return False
    return all(hint in lowered for hint in LISTING_INTERSTITIAL_HINTS)


def classify_listing_failure(
    *,
    listing_url: str,
    listing_text: str,
    listing_soup: Optional[BeautifulSoup],
    strict: bool,
    expected_titles: Iterable[str],
    author_name: str,
) -> Dict[str, str]:
    normalized_url = normalize_url(listing_url)
    page_kind = classify_listing_page_kind(normalized_url)
    text = listing_text or ""
    lowered = text.lower()
    terms = listing_signal_terms(text)

    if terms["blocked"]:
        return {
            "reason": "listing_redirect_or_blocked",
            "page_kind": page_kind,
            "evidence_snippet": proof_snippet_for_terms(text, [terms["blocked"]]),
            "listing_title": "",
        }

    listing_title = ""
    if listing_soup is not None:
        listing_title_oracle = resolve_listing_title_oracle(
            normalized_url,
            text,
            listing_soup,
            author_name=author_name,
        )
        listing_title = compact_text(str(listing_title_oracle.get("title", "") or ""))

    expected_title_keys = {normalize_title_key(title) for title in expected_titles if normalize_title_key(title)}
    listing_title_key = normalize_title_key(listing_title)
    if listing_title_key and expected_title_keys and not any(
        title_keys_match(listing_title, expected_title) for expected_title in expected_titles
    ):
        return {
            "reason": "listing_title_mismatch",
            "page_kind": page_kind,
            "evidence_snippet": proof_snippet_for_terms(text or listing_title, [listing_title]) or listing_title,
            "listing_title": listing_title,
        }

    if terms["unavailable"]:
        return {
            "reason": "listing_unavailable",
            "page_kind": page_kind,
            "evidence_snippet": proof_snippet_for_terms(text, [terms["unavailable"]]),
            "listing_title": listing_title,
        }

    has_price = bool(terms["price"])
    has_format = bool(terms["format"])
    has_buy = bool(terms["buy"])
    if listing_has_interstitial_state(text, page_kind, listing_title):
        return {
            "reason": "listing_interstitial_or_redirect_state",
            "page_kind": page_kind,
            "evidence_snippet": proof_snippet_for_terms(text, list(LISTING_INTERSTITIAL_HINTS)) or compact_text(text)[:180],
            "listing_title": listing_title,
        }
    if terms["reseller"] and not has_buy:
        return {
            "reason": "listing_reseller_only",
            "page_kind": page_kind,
            "evidence_snippet": proof_snippet_for_terms(text, [terms["reseller"]]),
            "listing_title": listing_title,
        }

    if strict:
        if has_format and has_buy and not has_price:
            return {
                "reason": "listing_no_price",
                "page_kind": page_kind,
                "evidence_snippet": proof_snippet_for_terms(text, [terms["format"], terms["buy"]]),
                "listing_title": listing_title,
            }
        if has_format and has_price and not has_buy:
            return {
                "reason": "listing_no_buy_control",
                "page_kind": page_kind,
                "evidence_snippet": proof_snippet_for_terms(text, [terms["price"], terms["format"]]),
                "listing_title": listing_title,
            }
        if not has_format and not has_price and not has_buy and not listing_title:
            return {
                "reason": "listing_parse_empty",
                "page_kind": page_kind,
                "evidence_snippet": compact_text(text)[:180],
                "listing_title": listing_title,
            }
        if not has_format and (has_price or has_buy):
            evidence_terms = [term for term in (terms["price"], terms["buy"]) if term]
            return {
                "reason": "listing_wrong_format",
                "page_kind": page_kind,
                "evidence_snippet": proof_snippet_for_terms(text, evidence_terms) if evidence_terms else compact_text(text)[:180],
                "listing_title": listing_title,
            }
        if not has_format or (not has_price and not has_buy):
            evidence_terms = [term for term in (terms["format"], terms["price"], terms["buy"]) if term]
            return {
                "reason": "listing_incomplete_product_state" if listing_title or has_format or has_price or has_buy else "listing_wrong_format_or_incomplete",
                "page_kind": page_kind,
                "evidence_snippet": proof_snippet_for_terms(text, evidence_terms) if evidence_terms else compact_text(text)[:180],
                "listing_title": listing_title,
            }

    return {
        "reason": "listing_wrong_format_or_incomplete" if strict else "missing_required_signals",
        "page_kind": page_kind,
        "evidence_snippet": proof_snippet_for_terms(text, [term for term in terms.values() if term]) if any(terms.values()) else compact_text(text)[:180],
        "listing_title": listing_title,
    }


def choose_best_listing_failure(attempts: Iterable[Dict[str, str]]) -> Dict[str, str]:
    ranked = sorted(
        attempts,
        key=lambda item: (
            -LISTING_FAILURE_PRIORITY.get(str(item.get("reason", "") or ""), 0),
            0 if item.get("evidence_snippet") else 1,
            len(str(item.get("url", "") or "")),
        ),
    )
    return ranked[0] if ranked else {}


def terminal_amazon_interstitial_reason(
    *,
    amazon_interstitial_confirmed: bool,
    listing_candidates: Sequence[str],
    listing_attempts: Sequence[Dict[str, str]],
    next_index: int,
) -> str:
    if not amazon_interstitial_confirmed:
        return ""
    remaining_bn_candidates = [
        url
        for url in listing_candidates[next_index:]
        if classify_listing_page_kind(url).startswith("barnesandnoble_")
    ]
    if remaining_bn_candidates:
        return ""
    bn_attempts = [
        attempt
        for attempt in listing_attempts
        if str(attempt.get("page_kind", "") or "").startswith("barnesandnoble_")
    ]
    if not bn_attempts:
        return "listing_amazon_interstitial_no_bn_candidate"
    if all((attempt.get("reason", "") or "") == "listing_redirect_or_blocked" for attempt in bn_attempts):
        return "listing_amazon_interstitial_bn_blocked"
    return "listing_amazon_interstitial_bn_failed_other"


def clean_source_author_label(value: str) -> str:
    cleaned = compact_text(value)
    cleaned = re.sub(r"\b(official website|official site|website|site|books|bookstore|home)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-|:")
    return cleaned


def extract_author_name_from_source_text(title: str, snippet: str) -> str:
    cleaned_title = clean_source_author_label(title)
    if cleaned_title:
        return cleaned_title

    compact = compact_text(snippet)
    patterns = [
        r"\bwebsite of (?:indie |independent |self-published )?(?:fantasy |sci-fi |scifi |romance |horror |thriller )?author ([A-Z][A-Za-z .'-]{2,80})",
        r"\bthe website of .*? author ([A-Z][A-Za-z .'-]{2,80})",
        r"\bauthor ([A-Z][A-Za-z .'-]{2,80})",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip(" .,-")
    return ""


def choose_author_website(candidate_url: str, links: Iterable[str]) -> str:
    candidate_origin = get_origin(candidate_url)
    if candidate_origin and not is_listing_domain(candidate_url):
        return candidate_origin

    for link in links:
        if not is_listing_domain(link):
            origin = get_origin(link)
            if origin:
                return origin
    return candidate_origin or candidate_url


def choose_external_author_site(links: Iterable[str], author_name: str = "") -> str:
    name_tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", author_name) if len(token) >= 3]
    scored: List[Tuple[int, str]] = []

    for link in links:
        parsed = urlparse(link)
        host = parsed.netloc.lower()
        if not host:
            continue
        if is_listing_domain(link):
            continue
        if domain_matches_blocklist(host) or url_matches_blocklist(link):
            continue
        if any(snippet in host for snippet in SOCIAL_HOST_SNIPPETS):
            continue
        if any(snippet in host for snippet in NON_AUTHOR_SITE_SNIPPETS):
            continue

        score = 0
        signal = f"{host}{parsed.path.lower()}"
        if "author" in signal or "books" in signal:
            score += 3
        if any(token in signal for token in name_tokens):
            score += 5
        if parsed.path in ("", "/"):
            score += 1

        origin = get_origin(link)
        if origin:
            scored.append((score, origin))

    if not scored:
        return ""
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def is_likely_author_website(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    host = urlparse(normalized).netloc.lower()
    if not host:
        return False
    if is_listing_domain(normalized):
        return False
    if domain_matches_blocklist(host) or url_matches_blocklist(normalized):
        return False
    if any(snippet in host for snippet in NON_AUTHOR_SITE_SNIPPETS):
        return False
    return True


def is_plausible_author_name(value: str) -> bool:
    name = re.sub(r"\s+", " ", (value or "").strip())
    if not name or name.lower() == "unknown author":
        return False
    if "@" in name or EMAIL_RE.search(name):
        return False
    lowered = name.lower()
    if any(snippet in lowered for snippet in BAD_AUTHOR_NAME_SNIPPETS):
        return False

    tokens = re.findall(r"[A-Za-z]+", name)
    if len(tokens) < 2 or len(tokens) > 6:
        return False
    if any(char.isdigit() for char in name):
        return False
    if any(token.lower() in BAD_AUTHOR_NAME_TOKENS for token in tokens):
        return False
    if name.isupper() and any(token.lower() in {"story", "stories", "anthology", "anthologies"} for token in tokens):
        return False
    return True


def author_name_looks_like_content_brand(value: str) -> bool:
    name = compact_text(value)
    if not name:
        return False
    tokens = [token.lower() for token in re.findall(r"[A-Za-z]+", name)]
    meaningful_tokens = [token for token in tokens if len(token) >= 3]
    if len(meaningful_tokens) < 2 or len(tokens) > 5:
        return False
    return all(token in AUTHOR_BRAND_NAME_TOKENS for token in meaningful_tokens)


def has_non_author_editorial_signal(text: str) -> bool:
    normalized = compact_text(text).lower()
    if not normalized:
        return False
    return any(hint in normalized for hint in NON_AUTHOR_EDITORIAL_TEXT_HINTS)


def default_profile_has_strong_author_identity(author_name: str, evidence_text: str) -> bool:
    if not author_name_looks_like_content_brand(author_name):
        return True
    return not has_non_author_editorial_signal(evidence_text)


def is_plausible_book_title(value: str, author_name: str = "", context: Optional[Dict[str, object]] = None) -> bool:
    return bool(assess_book_title(value, author_name=author_name, context=context)["plausible"])


def read_candidates(path: str, max_candidates: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = normalize_url(row.get("CandidateURL", ""))
            if url:
                rows.append(
                    {
                        "CandidateURL": url,
                        "SourceQuery": (row.get("SourceQuery", "") or "").strip(),
                        "QueryOriginal": (row.get("QueryOriginal", "") or "").strip(),
                        "QueryEffective": (row.get("QueryEffective", "") or "").strip(),
                        "WidenLevel": (row.get("WidenLevel", "") or "").strip(),
                        "WidenReason": (row.get("WidenReason", "") or "").strip(),
                        "StaleRunCounter": (row.get("StaleRunCounter", "") or "").strip(),
                        "SourceType": (row.get("SourceType", "") or "").strip(),
                        "SourceURL": normalize_url(row.get("SourceURL", "")) if (row.get("SourceURL", "") or "").strip() else "",
                        "SourceTitle": (row.get("SourceTitle", "") or "").strip(),
                        "SourceSnippet": (row.get("SourceSnippet", "") or "").strip(),
                    }
                )
            if max_candidates > 0 and len(rows) >= max_candidates:
                break
    return rows


def write_rows(path: str, rows: List[Dict[str, str]]) -> None:
    ensure_parent(Path(path))
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def validate_candidates(args: argparse.Namespace) -> Tuple[List[Dict[str, str]], Counter[str], int]:
    candidates = read_candidates(args.input, args.max_candidates)
    now = dt.datetime.now(dt.timezone.utc)
    validation_profile = str(getattr(args, "validation_profile", "default") or "default").strip().lower()
    strict_verified_mode = validation_profile in STRICT_VERIFIED_PROFILES
    astra_outbound_mode = is_astra_outbound_profile(validation_profile)
    email_only_mode = validation_profile == EMAIL_ONLY_PROFILE
    require_us_location = validation_profile in US_STRICT_VERIFIED_PROFILES
    if not strict_verified_mode and not str(getattr(args, "near_miss_location_output", "") or "").strip():
        near_miss_location_path = None
    effective_listing_strict = bool(getattr(args, "listing_strict", False) or strict_verified_mode)
    effective_require_email = bool(getattr(args, "require_email", False) or strict_verified_mode or email_only_mode)
    effective_require_location = bool(getattr(args, "require_location_proof", False) or require_us_location)
    effective_us_only = bool(getattr(args, "us_only", False) or require_us_location)
    session = build_session()
    max_pages_for_title = max(0, int(getattr(args, "max_pages_for_title", 4) or 0))
    max_pages_for_contact = max(0, int(getattr(args, "max_pages_for_contact", 6) or 0))
    legacy_max_fetches = int(getattr(args, "max_total_fetches_per_domain_per_run", 14) or 0)
    max_fetches_per_domain = int(getattr(args, "max_fetches_per_domain", 0) or 0) or legacy_max_fetches
    max_seconds_per_domain = float(getattr(args, "max_seconds_per_domain", 25.0) or 0.0)
    max_timeouts_per_domain = int(getattr(args, "max_timeouts_per_domain", 2) or 0)
    max_total_runtime = float(getattr(args, "max_total_runtime", 900.0) or 0.0)
    max_concurrency = int(getattr(args, "max_concurrency", 4) or 0)
    domain_cache_path = derive_domain_cache_path(
        str(getattr(args, "stats_output", "") or ""),
        str(getattr(args, "domain_cache_path", "") or ""),
    )
    location_debug_path = derive_location_debug_path(str(getattr(args, "stats_output", "") or ""))
    listing_debug_path = derive_listing_debug_path(str(getattr(args, "stats_output", "") or ""))
    near_miss_location_path = derive_near_miss_location_path(
        str(getattr(args, "output", "") or ""),
        str(getattr(args, "near_miss_location_output", "") or ""),
    )
    location_recovery_mode = str(getattr(args, "location_recovery_mode", "same_domain") or "same_domain").strip().lower()
    location_recovery_pages = int(getattr(args, "location_recovery_pages", 6) or 0)
    robots_cache, sitemap_cache, persisted_failure_cache, robots_text_cache = load_domain_cache(domain_cache_path)
    html_cache: Dict[str, FetchResult] = {}
    raw_cache: Dict[str, FetchResult] = {}
    domain_metrics: Dict[str, DomainMetrics] = {}
    cache_hits: Counter[str] = Counter()
    batch_start = time.monotonic()
    batch_runtime_exceeded = False
    prefilter_seconds_total = 0.0
    verify_seconds_total = 0.0
    prefilter_candidates = 0
    verify_candidates = 0

    results: List[Dict[str, str]] = []
    seen_listing = set()
    reject_counts: Counter[str] = Counter()
    location_decision_counts: Counter[str] = Counter()
    location_method_counts: Counter[str] = Counter()
    listing_reject_reason_counts: Counter[str] = Counter()
    location_debug_records: List[Dict[str, object]] = []
    listing_debug_records: List[Dict[str, object]] = []
    near_miss_location_rows: List[Dict[str, str]] = []
    candidate_budget_records: List[CandidateBudgetMetrics] = []
    candidate_ledger_records: List[Dict[str, object]] = []
    candidate_outcome_records: List[Dict[str, object]] = []
    current_candidate_metrics: Optional[CandidateBudgetMetrics] = None
    current_candidate_ledger: Optional[CandidateProofLedger] = None
    current_candidate_email = ""
    current_candidate_email_snippet = ""
    current_candidate_us_snippet = ""
    current_candidate_indie_snippet = ""
    current_candidate_listing_snippet = ""
    current_candidate_book_url = ""

    def record_page_fetch_failure(reason: str) -> str:
        mapping = {
            "timeout": "page_fetch_failed_timeout",
            "403": "page_fetch_failed_403",
            "404": "page_fetch_failed_404",
            "429": "page_fetch_failed_429",
            "non_html": "page_fetch_failed_non_html",
            "robots_disallow": "page_fetch_failed_robots_disallow",
            "time_budget_exceeded": "time_budget_exceeded",
            "fetch_budget_exceeded": "fetch_budget_exceeded",
            "batch_runtime_exceeded": "batch_runtime_exceeded",
        }
        reject_reason = mapping.get(reason, "page_fetch_failed_other")
        reject_counts[reject_reason] += 1
        return reject_reason

    def add_stage_seconds(domain: str, stage: str, started_at: float) -> None:
        nonlocal prefilter_seconds_total, verify_seconds_total
        elapsed = max(0.0, time.monotonic() - started_at)
        metrics = ensure_domain_metrics(domain_metrics, domain)
        if stage == "prefilter":
            metrics.prefilter_seconds += elapsed
            prefilter_seconds_total += elapsed
        else:
            metrics.verify_seconds += elapsed
            verify_seconds_total += elapsed
        if current_candidate_metrics is not None:
            if stage == "prefilter":
                current_candidate_metrics.prefilter_seconds += elapsed
            else:
                current_candidate_metrics.verify_seconds += elapsed

    def fetch_cached(url: str, expect_html: bool = True) -> FetchResult:
        normalized = normalize_url(url)
        if not normalized:
            return FetchResult(url=url, failure_reason="bad_url")
        cache = html_cache if expect_html else raw_cache
        if normalized in cache:
            return cache[normalized]

        domain = registrable_domain(normalized)
        if max_total_runtime > 0 and (time.monotonic() - batch_start) >= max_total_runtime:
            result = FetchResult(url=normalized, failure_reason="batch_runtime_exceeded")
            cache[normalized] = result
            return result
        if normalized in persisted_failure_cache:
            cache_hits["persisted_failure"] += 1
            cache[normalized] = persisted_failure_cache[normalized]
            return cache[normalized]
        if domain:
            budget_reason = domain_budget_exhausted(
                domain,
                domain_metrics,
                max_fetches_per_domain=max_fetches_per_domain,
                max_seconds_per_domain=max_seconds_per_domain,
                max_timeouts_per_domain=max_timeouts_per_domain,
            )
            if budget_reason:
                result = FetchResult(url=normalized, failure_reason=budget_reason)
                cache[normalized] = result
                return result
            metrics = ensure_domain_metrics(domain_metrics, domain)
        else:
            metrics = DomainMetrics()

        started_at = time.monotonic()
        result = fetch_with_meta(
            session=session,
            url=normalized,
            timeout=args.timeout,
            robots_cache=robots_cache,
            robots_text_cache=robots_text_cache,
            ignore_robots=args.ignore_robots,
            retry_seconds=args.robots_retry_seconds,
            expect_html=expect_html,
        )
        elapsed = max(0.0, time.monotonic() - started_at)
        if domain:
            metrics.fetch_count += 1
            metrics.total_seconds += elapsed
            if result.failure_reason == "timeout":
                metrics.timeout_count += 1
            if result.failure_reason:
                record_domain_fetch_failure(domain_metrics, normalized, result.failure_reason, result.status_code)
        if current_candidate_metrics is not None:
            current_candidate_metrics.fetch_count += 1
            current_candidate_metrics.total_seconds += elapsed
        cache[normalized] = result
        return result

    def fetch_resilient_html(url: str, allow_root_fallback: bool = False) -> FetchResult:
        last_result = FetchResult(url=url, failure_reason="bad_url")
        for candidate_url in build_fetch_variants(url, allow_root_fallback=allow_root_fallback):
            result = fetch_cached(candidate_url, expect_html=True)
            if result.ok and result.soup is not None:
                return result
            last_result = result
        return last_result

    def finalize_candidate_budget(
        *,
        reason: str,
        kept: bool,
        domain: str,
        location_recovery_attempted: bool = False,
        listing_recovery_attempted: bool = False,
        location_recovery_skipped_reason: str = "",
        listing_recovery_skipped_reason: str = "",
    ) -> None:
        nonlocal current_candidate_metrics
        nonlocal current_candidate_ledger
        nonlocal current_candidate_email
        nonlocal current_candidate_email_snippet
        nonlocal current_candidate_us_snippet
        nonlocal current_candidate_indie_snippet
        nonlocal current_candidate_listing_snippet
        nonlocal current_candidate_book_url
        if current_candidate_metrics is None:
            return
        current_candidate_metrics.candidate_domain = domain or current_candidate_metrics.candidate_domain
        current_candidate_metrics.reject_reason = reason
        current_candidate_metrics.kept = kept
        current_candidate_metrics.location_recovery_attempted = location_recovery_attempted
        current_candidate_metrics.listing_recovery_attempted = listing_recovery_attempted
        current_candidate_metrics.location_recovery_skipped_reason = location_recovery_skipped_reason
        current_candidate_metrics.listing_recovery_skipped_reason = listing_recovery_skipped_reason
        if current_candidate_ledger is not None:
            current_candidate_ledger.candidate_domain = domain or current_candidate_ledger.candidate_domain
            current_candidate_ledger.reject_reason_if_any = "" if kept else reason
            if kept:
                current_candidate_ledger.current_state = "verified"
            else:
                current_candidate_ledger.current_state = derive_candidate_working_state(
                    current_candidate_ledger,
                    strict_verified_mode=strict_verified_mode,
                    require_us_location=require_us_location,
                    effective_require_email=effective_require_email,
                    effective_listing_strict=effective_listing_strict,
                )
            planned_action = plan_candidate_next_action(
                current_candidate_ledger,
                strict_verified_mode=strict_verified_mode,
                require_us_location=require_us_location,
                effective_require_email=effective_require_email,
                effective_listing_strict=effective_listing_strict,
            )
            current_candidate_metrics.current_state = current_candidate_ledger.current_state
            current_candidate_metrics.validation_state = current_candidate_ledger.current_state
            current_candidate_metrics.next_action = planned_action.get("action", "")
            current_candidate_metrics.next_action_reason = planned_action.get("reason", "")
            current_candidate_metrics.last_attempted_action = current_candidate_ledger.last_attempted_action
            current_candidate_metrics.email_source_url = (
                current_candidate_ledger.best_email_source_url or current_candidate_metrics.email_source_url
            )
            current_candidate_metrics.location_source_url = (
                current_candidate_ledger.best_location_source_url or current_candidate_metrics.location_source_url
            )
            current_candidate_metrics.indie_proof_url = (
                current_candidate_ledger.best_indie_source_url or current_candidate_metrics.indie_proof_url
            )
            current_candidate_metrics.retailer_listing_url = (
                current_candidate_ledger.best_listing_source_url
                or current_candidate_metrics.retailer_listing_url
                or current_candidate_book_url
                or current_candidate_metrics.book_url
            )
            current_candidate_metrics.recency_source_url = (
                current_candidate_ledger.best_recency_source_url or current_candidate_metrics.recency_source_url
            )
            current_candidate_metrics.email_snippet = (
                current_candidate_email_snippet
                or current_candidate_ledger.best_email_snippet
                or current_candidate_metrics.email_snippet
            )
            current_candidate_metrics.location_value = (
                current_candidate_metrics.location_value
                or current_candidate_ledger.best_location_snippet
                or current_candidate_metrics.us_snippet
            )
            current_candidate_metrics.us_snippet = (
                current_candidate_us_snippet
                or current_candidate_ledger.best_location_snippet
                or current_candidate_metrics.us_snippet
            )
            current_candidate_metrics.indie_proof_value = (
                current_candidate_metrics.indie_proof_value
                or current_candidate_ledger.best_indie_snippet
                or current_candidate_metrics.indie_snippet
            )
            current_candidate_metrics.indie_snippet = (
                current_candidate_indie_snippet
                or current_candidate_ledger.best_indie_snippet
                or current_candidate_metrics.indie_snippet
            )
            current_candidate_metrics.listing_snippet = (
                current_candidate_listing_snippet
                or current_candidate_ledger.best_listing_snippet
                or current_candidate_metrics.listing_snippet
            )
            current_candidate_metrics.recency_value = (
                current_candidate_metrics.recency_value
                or current_candidate_ledger.best_recency_snippet
            )
            if astra_outbound_mode:
                current_candidate_ledger.astra_rule_checks = build_astra_rule_checks(
                    author_name=current_candidate_metrics.author_name or author_name,
                    author_email=current_candidate_metrics.email or current_candidate_email or author_email,
                    author_email_record=best_email,
                    author_email_source_url=current_candidate_metrics.email_source_url or author_email_source_url,
                    author_email_proof_snippet=current_candidate_metrics.email_snippet or author_email_proof_snippet,
                    location=current_candidate_metrics.location_value or location,
                    location_decision=location_decision,
                    location_proof_url=current_candidate_metrics.location_source_url or location_proof_url,
                    location_proof_snippet=current_candidate_metrics.us_snippet or location_proof_snippet,
                    indie_proof_strength=indie_proof_strength,
                    indie_proof_url=current_candidate_metrics.indie_proof_url or indie_proof_url,
                    indie_proof_snippet=current_candidate_metrics.indie_snippet or indie_proof_snippet,
                    listing_url=current_candidate_metrics.retailer_listing_url or valid_listing_url or row_listing_url,
                    listing_status=listing_status,
                    listing_fail_reason=listing_fail_reason,
                    listing_title=listing_title,
                    listing_snippet=current_candidate_metrics.listing_snippet or current_candidate_listing_snippet,
                    book_title=current_candidate_metrics.book_title or book_title,
                    recency_status=recency_status,
                    recency_url=current_candidate_metrics.recency_source_url or recency_url,
                    recency_proof_snippet=current_candidate_metrics.recency_value or recency_proof,
                    source_url=current_candidate_metrics.source_url or candidate_source_url,
                    contact_page_url=contact_page,
                    author_website=author_website,
                    reject_reason=reason,
                )
            candidate_ledger_records.append(serialize_candidate_proof_ledger(current_candidate_ledger))
        current_candidate_metrics.email = (current_candidate_email or current_candidate_metrics.email).strip().lower()
        current_candidate_metrics.book_url = current_candidate_book_url or current_candidate_metrics.book_url
        if astra_outbound_mode:
            current_candidate_metrics.best_source_url = choose_best_astra_source_url(
                current_candidate_metrics.email_source_url or author_email_source_url,
                contact_page,
                author_website,
                current_candidate_metrics.source_url or candidate_source_url,
                current_candidate_metrics.location_source_url or location_proof_url,
                current_candidate_metrics.indie_proof_url or indie_proof_url,
                current_candidate_metrics.recency_source_url or recency_url,
                current_candidate_metrics.retailer_listing_url or valid_listing_url or row_listing_url,
            )
            current_candidate_metrics.normalized_listing_url = normalize_astra_listing_url(
                current_candidate_metrics.retailer_listing_url or valid_listing_url or row_listing_url
            )
            if current_candidate_ledger is not None:
                current_candidate_metrics.astra_rule_checks = dict(current_candidate_ledger.astra_rule_checks or {})
        candidate_outcome_records.append(serialize_candidate_outcome(current_candidate_metrics))
        candidate_budget_records.append(current_candidate_metrics)
        current_candidate_metrics = None
        current_candidate_ledger = None

    def mark_strict_location_skip(reason: str) -> None:
        nonlocal location_recovery_skipped_reason
        if (
            strict_verified_mode
            and require_us_location
            and not location_recovery_attempted
            and not location_recovery_skipped_reason
        ):
            location_recovery_skipped_reason = reason

    def candidate_planner_decision(*, reject_reason: str = "") -> Dict[str, str]:
        if current_candidate_ledger is None:
            return {"state": "harvested", "action": "", "reason": reject_reason}
        current_candidate_ledger.reject_reason_if_any = reject_reason
        current_candidate_ledger.current_state = derive_candidate_working_state(
            current_candidate_ledger,
            strict_verified_mode=strict_verified_mode,
            require_us_location=require_us_location,
            effective_require_email=effective_require_email,
            effective_listing_strict=effective_listing_strict,
        )
        return plan_candidate_next_action(
            current_candidate_ledger,
            strict_verified_mode=strict_verified_mode,
            require_us_location=require_us_location,
            effective_require_email=effective_require_email,
            effective_listing_strict=effective_listing_strict,
        )

    def record_location_debug(
        *,
        candidate_url: str,
        candidate_domain: str,
        location_assessment: Dict[str, str],
        location_attempts: List[Dict[str, str]],
        location_decision: str,
        recovery_candidate_urls: List[str],
        location_recovery_attempted: bool,
        location_recovery_skipped_reason: str,
    ) -> None:
        if not location_decision:
            return
        location_decision_counts[location_decision] += 1
        location_method_counts[location_assessment.get("method", "") or "none"] += 1
        location_debug_records.append(
            {
                "candidate_url": normalize_url(candidate_url),
                "candidate_domain": candidate_domain,
                "validation_profile": validation_profile,
                "require_location_proof": bool(effective_require_location),
                "us_only": bool(effective_us_only),
                "final_decision": location_decision,
                "selected_location": location_assessment.get("location", ""),
                "selected_source_url": location_assessment.get("source_url", ""),
                "selected_method": location_assessment.get("method", ""),
                "selected_snippet": location_assessment.get("snippet", ""),
                "recovery_mode": location_recovery_mode,
                "recovery_attempted": location_recovery_attempted,
                "recovery_skipped_reason": location_recovery_skipped_reason,
                "recovery_candidate_urls": recovery_candidate_urls,
                "attempts": location_attempts,
            }
        )

    for idx, candidate in enumerate(candidates, 1):
        if max_total_runtime > 0 and (time.monotonic() - batch_start) >= max_total_runtime:
            batch_runtime_exceeded = True
            break
        candidate_url = candidate.get("CandidateURL", "")
        candidate_source_query = candidate.get("SourceQuery", "")
        candidate_query_original = candidate.get("QueryOriginal", "")
        candidate_query_effective = candidate.get("QueryEffective", "")
        candidate_widen_level = candidate.get("WidenLevel", "")
        candidate_widen_reason = candidate.get("WidenReason", "")
        candidate_stale_run_counter = candidate.get("StaleRunCounter", "")
        candidate_source_url = candidate.get("SourceURL", "")
        candidate_source_title = candidate.get("SourceTitle", "")
        candidate_source_snippet = candidate.get("SourceSnippet", "")
        candidate_domain = registrable_domain(candidate_url) or "unknown"
        candidate_reject_reason = ""
        candidate_kept = False
        location_attempts: List[Dict[str, str]] = []
        location_assessment: Dict[str, str] = {}
        location_decision = ""
        recovery_candidate_urls: List[str] = []
        location_recovery_attempted = False
        location_recovery_skipped_reason = ""
        listing_recovery_attempted = False
        listing_recovery_skipped_reason = ""
        current_candidate_email = ""
        current_candidate_email_snippet = ""
        current_candidate_us_snippet = ""
        current_candidate_indie_snippet = ""
        current_candidate_listing_snippet = ""
        current_candidate_book_url = ""
        author_name = ""
        author_name_source_url = ""
        author_email = ""
        author_email_source_url = ""
        author_email_proof_snippet = ""
        email_quality = ""
        best_email: Dict[str, str] = {}
        author_website = ""
        contact_page = ""
        location = ""
        location_proof_url = ""
        location_proof_snippet = ""
        indie_proof_strength = ""
        indie_proof_url = ""
        indie_proof_snippet = ""
        row_listing_url = ""
        valid_listing_url = ""
        listing_status = ""
        listing_fail_reason = ""
        listing_title = ""
        recency_url = ""
        recency_proof = ""
        recency_status = ""
        current_candidate_metrics = CandidateBudgetMetrics(
            candidate_url=normalize_url(candidate_url) or candidate_url,
            candidate_domain=candidate_domain,
            source_type=(candidate.get("SourceType", "") or "").strip(),
            source_query=(candidate_source_query or "").strip(),
            source_url=normalize_url(candidate_source_url) or candidate_source_url,
            source_title=(candidate_source_title or "").strip(),
            source_snippet=(candidate_source_snippet or "").strip(),
            query_original=(candidate_query_original or "").strip(),
            query_effective=(candidate_query_effective or candidate_source_query or "").strip(),
            widen_level=int(candidate_widen_level or 0) if str(candidate_widen_level or "").strip() else 0,
            widen_reason=(candidate_widen_reason or "").strip(),
            stale_run_counter=int(candidate_stale_run_counter or 0) if str(candidate_stale_run_counter or "").strip() else 0,
            source_family=infer_source_family((candidate.get("SourceType", "") or "").strip(), candidate_source_url),
        )
        current_candidate_ledger = CandidateProofLedger(
            candidate_url=normalize_url(candidate_url) or candidate_url,
            candidate_domain=candidate_domain,
            current_state="harvested",
        )
        if is_blocked_candidate_url(candidate_url):
            reject_counts["blocked_candidate_url"] += 1
            candidate_reject_reason = "blocked_candidate_url"
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        prefilter_started_at = time.monotonic()
        page = fetch_resilient_html(candidate_url, allow_root_fallback=True)
        if not page.ok or page.soup is None:
            add_stage_seconds(candidate_domain, "prefilter", prefilter_started_at)
            candidate_reject_reason = record_page_fetch_failure(page.failure_reason)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue
        soup = page.soup
        text = get_text(soup)
        if not text:
            add_stage_seconds(candidate_domain, "prefilter", prefilter_started_at)
            reject_counts["empty_text"] += 1
            candidate_reject_reason = "empty_text"
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        candidate_domain = registrable_domain(page.url) or candidate_domain
        current_candidate_metrics.candidate_domain = candidate_domain
        links = extract_links(page.url, soup)
        planned_origin = get_origin(page.url or candidate_url)
        if planned_origin and registrable_domain(planned_origin) in sitemap_cache:
            cache_hits["sitemap"] += 1
        sitemap_urls = collect_sitemap_urls(planned_origin or candidate_url, fetch_cached, sitemap_cache)
        planned_urls = build_prefilter_page_plan(
            page.url or candidate_url,
            links,
            sitemap_urls,
            max_book_pages=min(1, max_pages_for_title),
            max_contact_pages=min(1, max_pages_for_contact),
        )
        add_stage_seconds(candidate_domain, "prefilter", prefilter_started_at)
        prefilter_candidates += 1
        if current_candidate_ledger is not None:
            current_candidate_ledger.current_state = "triage_ready"
            append_candidate_action(current_candidate_ledger, "triage_candidate", detail="prefilter_pass")
            _ = candidate_planner_decision()
        if not planned_urls:
            reject_counts["prefilter_no_planned_pages"] += 1
            candidate_reject_reason = "prefilter_no_planned_pages"
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        verify_started_at = time.monotonic()
        verify_candidates += 1
        candidate_budget_failure = ""
        evidence_pages: List[Tuple[str, str, BeautifulSoup]] = [(page.url, text, soup)]
        evidence_urls = {page.url}
        evidence_texts: List[Tuple[str, str]] = []
        if candidate_source_snippet or candidate_source_title:
            evidence_texts.append(
                (
                    candidate_source_url or page.url,
                    " ".join(part for part in (candidate_source_title, candidate_source_snippet) if part),
                )
            )
        all_links = list(links)
        for planned_url in planned_urls[1:]:
            planned_page = fetch_resilient_html(planned_url, allow_root_fallback=True)
            time.sleep(max(0.0, args.delay))
            if planned_page.failure_reason in {"time_budget_exceeded", "fetch_budget_exceeded", "batch_runtime_exceeded"}:
                candidate_budget_failure = planned_page.failure_reason
                break
            if not planned_page.ok or planned_page.soup is None:
                continue
            planned_text = get_text(planned_page.soup)
            if not planned_text or planned_page.url in evidence_urls:
                continue
            evidence_pages.append((planned_page.url, planned_text, planned_page.soup))
            evidence_urls.add(planned_page.url)
            all_links.extend(extract_links(planned_page.url, planned_page.soup))

        all_links = dedupe_urls(all_links)
        if candidate_budget_failure:
            reject_counts[candidate_budget_failure] += 1
            candidate_reject_reason = candidate_budget_failure
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue
        combined_text = " ".join([part[1] for part in evidence_pages] + [part[1] for part in evidence_texts if part[1]])
        candidate_host = urlparse(candidate_url).netloc.lower()
        if "goodreads.com" not in candidate_host and has_enterprise_signal(combined_text):
            reject_counts["famous_signal" if strict_verified_mode else "enterprise_or_famous"] += 1
            candidate_reject_reason = "famous_signal" if strict_verified_mode else "enterprise_or_famous"
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        if any("wikipedia.org" in urlparse(source_url).netloc.lower() for source_url, _, _ in evidence_pages):
            reject_counts["famous_signal" if strict_verified_mode else "wikipedia_signal"] += 1
            candidate_reject_reason = "famous_signal" if strict_verified_mode else "wikipedia_signal"
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        author_page_links: List[str] = []
        for source_url, _, source_soup in evidence_pages:
            if "/author/show/" in source_url.lower():
                author_page_links.extend(extract_links(source_url, source_soup))
        author_link_pool = dedupe_urls(author_page_links) if author_page_links else all_links

        author_website = choose_external_author_site(author_link_pool) or choose_author_website(candidate_url, all_links)
        if not is_likely_author_website(author_website):
            reject_counts["non_author_website"] += 1
            candidate_reject_reason = "non_author_website"
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue
        contact_page = find_contact_page(candidate_url, links, soup)
        contact_origin = get_origin(author_website) or get_origin(candidate_url)
        if not contact_page or contact_page == get_origin(candidate_url):
            fallback_contact = find_contact_link_from_links(contact_origin, all_links)
            if fallback_contact:
                contact_page = fallback_contact
        if not contact_page or contact_page in (get_origin(candidate_url), candidate_url):
            for source_url, _, _ in evidence_pages[1:]:
                lowered = source_url.lower()
                if "/author/show/" in lowered or "contact" in lowered or "about" in lowered:
                    contact_page = source_url
                    break
        if "goodreads.com/about/us" in (contact_page or "").lower() and "goodreads.com" not in (author_website or "").lower():
            contact_page = author_website
        if not contact_page:
            contact_page = author_website or candidate_url
        subscribe_page = find_subscribe_link_from_links(contact_origin, all_links)

        author_name = "Unknown Author"
        author_name_source_url = ""
        for source_url, source_text, source_soup in evidence_pages:
            maybe_name = extract_author_name(source_soup, source_text)
            if maybe_name != "Unknown Author":
                author_name = maybe_name
                author_name_source_url = source_url
                break
        if author_name == "Unknown Author" and (candidate_source_title or candidate_source_snippet):
            author_name = extract_author_name_from_source_text(candidate_source_title, candidate_source_snippet) or candidate_source_title
            author_name_source_url = candidate_source_url or author_name_source_url
        if not is_plausible_author_name(author_name) and (candidate_source_title or candidate_source_snippet):
            rescued_name = extract_author_name_from_source_text(candidate_source_title, candidate_source_snippet)
            if rescued_name:
                author_name = rescued_name
                author_name_source_url = candidate_source_url or author_name_source_url
        if not is_plausible_author_name(author_name):
            reject_counts["bad_author_name"] += 1
            candidate_reject_reason = "bad_author_name"
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue
        if validation_profile in {"default", EMAIL_ONLY_PROFILE}:
            author_identity_evidence = " ".join(
                part
                for part in (
                    candidate_source_title,
                    candidate_source_snippet,
                    combined_text,
                )
                if part
            )
            if not default_profile_has_strong_author_identity(author_name, author_identity_evidence):
                reject_counts["bad_author_name"] += 1
                candidate_reject_reason = "bad_author_name"
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
                continue
        if current_candidate_metrics is not None:
            current_candidate_metrics.author_name = author_name
        author_website = choose_external_author_site(author_link_pool, author_name) or author_website
        if not is_likely_author_website(author_website):
            reject_counts["non_author_website_after_name"] += 1
            candidate_reject_reason = "non_author_website_after_name"
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(reason=candidate_reject_reason, kept=False, domain=candidate_domain)
            continue

        evidence_urls = {u for u, _, _ in evidence_pages}
        evidence_plus = list(evidence_pages)

        def append_evidence_page(url: str, allow_root_fallback: bool = False) -> Optional[Tuple[str, str, BeautifulSoup]]:
            nonlocal candidate_budget_failure
            normalized = normalize_url(url)
            if not normalized:
                return None
            result = fetch_resilient_html(normalized, allow_root_fallback=allow_root_fallback)
            time.sleep(max(0.0, args.delay))
            if result.failure_reason in {"time_budget_exceeded", "fetch_budget_exceeded", "batch_runtime_exceeded"} and not candidate_budget_failure:
                candidate_budget_failure = result.failure_reason
                return None
            if not result.ok or result.soup is None:
                return None
            page_text = get_text(result.soup)
            if not page_text:
                return None
            if result.url not in evidence_urls:
                evidence_plus.append((result.url, page_text, result.soup))
                evidence_urls.add(result.url)
            return result.url, page_text, result.soup

        def expand_listing_shortlink_candidate(url: str) -> str:
            nonlocal candidate_budget_failure
            result = fetch_cached(url, expect_html=False)
            time.sleep(max(0.0, args.delay))
            if result.failure_reason in {"time_budget_exceeded", "fetch_budget_exceeded", "batch_runtime_exceeded"}:
                if not candidate_budget_failure:
                    candidate_budget_failure = result.failure_reason
                return ""
            if result.failure_reason:
                return ""
            final_url = normalize_listing_url(result.final_url or result.url)
            return final_url if is_listing_url(final_url) else ""

        listing_enriched_from_url = ""
        listing_enrichment_method = ""
        enriched_listing_candidates: List[Tuple[str, str, str]] = []
        for page_url, _, page_soup in evidence_plus:
            enriched_listing_candidates.extend(extract_listing_candidates_from_page(page_url, page_soup))

        all_links = dedupe_urls(all_links)
        contact_origin = get_origin(author_website) or get_origin(candidate_url)
        if "goodreads.com" in urlparse(contact_page or "").netloc.lower() and "goodreads.com" not in (author_website or "").lower():
            contact_page = author_website

        discovered_contact_url = next((url for url in planned_urls if "contact" in url.lower()), "")
        discovered_subscribe_url = next((url for url in planned_urls if any(hint in url.lower() for hint in SUBSCRIBE_PATH_HINTS)), "")
        discovered_location_urls = [
            url for url in planned_urls if any(hint in url.lower() for hint in LOCATION_PAGE_PATH_HINTS)
        ]
        if discovered_contact_url:
            contact_page = discovered_contact_url
        elif not contact_page or contact_page == get_origin(candidate_url):
            fallback_contact = find_contact_link_from_links(contact_origin, all_links)
            if fallback_contact:
                contact_page = fallback_contact
        if not contact_page or contact_page in (get_origin(candidate_url), candidate_url):
            contact_page = author_website or candidate_url
        subscribe_page = discovered_subscribe_url or find_subscribe_link_from_links(contact_origin, all_links)
        if contact_page and contact_page not in evidence_urls:
            appended = append_evidence_page(contact_page, allow_root_fallback=True)
            if appended:
                all_links.extend(extract_links(appended[0], appended[2]))
        if subscribe_page and subscribe_page not in evidence_urls and subscribe_page != contact_page:
            appended = append_evidence_page(subscribe_page, allow_root_fallback=True)
            if appended:
                all_links.extend(extract_links(appended[0], appended[2]))
        location_support_urls = build_location_recovery_urls(
            contact_origin,
            discovered_location_urls + list(all_links),
            sitemap_urls,
            limit=max(0, min(2, max_pages_for_contact)),
        )
        if not strict_verified_mode:
            for location_url in location_support_urls:
                if location_url in evidence_urls or location_url in {contact_page, subscribe_page}:
                    continue
                appended = append_evidence_page(location_url, allow_root_fallback=True)
                if appended:
                    all_links.extend(extract_links(appended[0], appended[2]))
        all_links = dedupe_urls(all_links)
        press_kit_url, media_url = find_press_media_links_from_links(
            contact_origin,
            list(all_links) + [url for url, _, _ in evidence_plus],
        )
        contact_page_kind = classify_press_media_url(contact_page)
        if contact_page_kind == "press" and not press_kit_url:
            press_kit_url = normalize_url(contact_page)
        elif contact_page_kind == "media" and not media_url:
            media_url = normalize_url(contact_page)

        def collect_best_email_record() -> Dict[str, str]:
            email_records: List[Dict[str, str]] = []
            for source_url, source_text, source_soup in evidence_plus:
                email_records.extend(extract_page_email_records(source_url, source_text, source_soup, author_website))
            return choose_author_email_record(
                email_records,
                author_website,
                author_name=author_name,
                require_visible_text=(strict_verified_mode or email_only_mode),
            )

        preliminary_email_record = collect_best_email_record()
        preliminary_author_email = (preliminary_email_record.get("email", "") or "").strip()
        preliminary_email_quality = (preliminary_email_record.get("quality", "") or "").strip()
        current_candidate_email = preliminary_author_email
        current_candidate_email_snippet = (preliminary_email_record.get("proof_snippet", "") or "").strip()
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_email_snippet = (
                preliminary_email_record.get("proof_snippet", "") or current_candidate_ledger.best_email_snippet
            )
            current_candidate_ledger.best_email_source_url = (
                preliminary_email_record.get("source_url", "") or current_candidate_ledger.best_email_source_url
            )
            append_candidate_action(
                current_candidate_ledger,
                "try_email_proof",
                detail="found" if preliminary_author_email else "missing",
            )
        location_source_type = candidate.get("SourceType", "").strip().lower()
        source_is_directory = location_source_type in {
            "epic_directory",
            "ian_directory",
            "iabx_directory",
            "indieview_directory",
        } or any(hint in candidate_source_query.lower() for hint in DIRECTORY_SOURCE_HINTS)

        def collect_location_attempts() -> List[Dict[str, str]]:
            attempts: List[Dict[str, str]] = []
            for source_url, source_text in evidence_texts:
                assessment = classify_location_evidence(
                    text=source_text,
                    source_url=source_url,
                    source_kind="directory" if source_is_directory else "source_text",
                    author_name=author_name,
                )
                if source_is_directory and assessment.get("decision") != "no_location_signal":
                    assessment["method"] = "directory"
                attempts.append(assessment)

            for source_url, source_text, source_soup in evidence_plus:
                source_kind = classify_location_page_kind(source_url)
                assessment = classify_location_evidence(
                    text=source_text,
                    soup=source_soup,
                    source_url=source_url,
                    source_kind=source_kind,
                    author_name=author_name,
                )
                attempts.append(assessment)
            return attempts

        preliminary_location_attempts = collect_location_attempts()
        preliminary_location_assessment = choose_location_assessment(preliminary_location_attempts)
        preliminary_location_decision = (
            preliminary_location_assessment.get("decision", "no_location_signal") or "no_location_signal"
        )
        preliminary_publisher_location_dead_end = (
            preliminary_location_decision == "publisher_location_only"
            and publisher_location_only_dead_end(
                preliminary_location_attempts,
                origin_url=contact_origin or candidate_url,
            )
        )
        preliminary_planner_reason = ""
        if strict_verified_mode and effective_require_email and not preliminary_author_email:
            preliminary_planner_reason = "no_visible_author_email"
        elif (
            strict_verified_mode
            and effective_require_email
            and preliminary_author_email
            and not email_quality_is_fully_verified(preliminary_email_quality)
        ):
            preliminary_planner_reason = "weak_strict_email_quality"
        elif strict_verified_mode and require_us_location and preliminary_location_decision == "non_us_location":
            preliminary_planner_reason = "non_us_location"
        elif strict_verified_mode and require_us_location and preliminary_publisher_location_dead_end:
            preliminary_planner_reason = "publisher_location_only"
        preliminary_listing_plan = candidate_planner_decision(reject_reason=preliminary_planner_reason)

        listing_candidates = find_listing_candidates(candidate_url, all_links)
        listing_sources: Dict[str, Tuple[str, str]] = {}
        for listing_url, source_url, method in enriched_listing_candidates:
            if listing_url not in listing_candidates:
                listing_candidates.append(listing_url)
            listing_sources.setdefault(listing_url, (source_url, method))
        listing_recovery_urls: List[str] = []
        hinted_listing_urls: List[str] = []
        if not listing_candidates:
            if preliminary_listing_plan.get("action") == "stop_dead_end":
                if preliminary_listing_plan.get("reason") == "non_us_location":
                    listing_recovery_skipped_reason = "definitive_non_us_location"
            elif strict_verified_mode and effective_require_email and not preliminary_author_email:
                listing_recovery_skipped_reason = "missing_strict_email"
            elif (
                strict_verified_mode
                and effective_require_email
                and preliminary_author_email
                and not email_quality_is_fully_verified(preliminary_email_quality)
            ):
                listing_recovery_skipped_reason = "weak_strict_email_quality"
            elif strict_verified_mode and require_us_location and preliminary_publisher_location_dead_end:
                listing_recovery_skipped_reason = "publisher_location_only_precheck"
            elif preliminary_listing_plan.get("action") != "try_listing_proof":
                listing_recovery_skipped_reason = preliminary_listing_plan.get("reason", "") or "planner_skip"
            else:
                listing_recovery_attempted = True
                for page_url, _, page_soup in evidence_plus:
                    hinted_listing_urls.extend(
                        discover_same_domain_listing_support_urls(
                            page_url,
                            page_soup,
                            limit=max(0, min(3, max_pages_for_title + 1)),
                            author_name=author_name,
                        )
                    )
                listing_recovery_urls = build_listing_recovery_urls(
                    contact_origin,
                    all_links,
                    sitemap_urls,
                    hinted_listing_urls,
                    limit=max(0, min(2, max_pages_for_title or 2)),
                )
                listing_followup_budget = max(0, min(2, max_pages_for_title or 2))
                for recovery_url in listing_recovery_urls:
                    if recovery_url in evidence_urls:
                        continue
                    appended = append_evidence_page(recovery_url, allow_root_fallback=True)
                    if not appended:
                        continue
                    appended_links = extract_links(appended[0], appended[2])
                    all_links.extend(appended_links)
                    for listing_url, source_url, method in extract_listing_candidates_from_page(
                        appended[0],
                        appended[2],
                        shortlink_expander=expand_listing_shortlink_candidate,
                    ):
                        if listing_url not in listing_candidates:
                            listing_candidates.append(listing_url)
                        listing_sources.setdefault(listing_url, (source_url, method))
                    if listing_candidates or listing_followup_budget <= 0:
                        break
                    page_kind = classify_title_page_kind(appended[0], appended[2])
                    followup_urls = discover_same_domain_listing_support_urls(
                        appended[0],
                        appended[2],
                        limit=min(1, listing_followup_budget),
                        author_name=author_name,
                    )
                    if page_kind not in {"homepage_books", "books_index", "series_index", "book_detail"} and not followup_urls:
                        continue
                    for followup_url in followup_urls:
                        if followup_url in evidence_urls:
                            continue
                        followup_page = append_evidence_page(followup_url, allow_root_fallback=True)
                        if not followup_page:
                            continue
                        listing_followup_budget -= 1
                        followup_links = extract_links(followup_page[0], followup_page[2])
                        all_links.extend(followup_links)
                        for listing_url, source_url, method in extract_listing_candidates_from_page(
                            followup_page[0],
                            followup_page[2],
                            shortlink_expander=expand_listing_shortlink_candidate,
                        ):
                            if listing_url not in listing_candidates:
                                listing_candidates.append(listing_url)
                            listing_sources.setdefault(listing_url, (source_url, method))
                        if listing_candidates or listing_followup_budget <= 0:
                            break
            all_links = dedupe_urls(all_links)

        proof_evidence: List[Tuple[str, str]] = [(url, text) for url, text in evidence_texts if text]
        proof_evidence.extend((url, text) for url, text, _ in evidence_plus)

        location = ""
        location_proof_url = ""
        location_proof_snippet = ""
        location_method = ""
        recovery_candidate_urls = dedupe_urls(location_support_urls)

        def apply_location_assessment_state() -> str:
            nonlocal location
            nonlocal location_proof_url
            nonlocal location_proof_snippet
            nonlocal location_method
            location_gate_reason = ""
            if location_decision == "ok":
                location = location_assessment.get("location", "") or "Unknown"
                location_proof_url = location_assessment.get("source_url", "")
                location_proof_snippet = location_assessment.get("snippet", "")
                location_method = location_assessment.get("method", "")
            elif location_decision == "non_us_location":
                if location_assessment.get("method", "") in STRONG_LOCATION_METHODS:
                    location = location_assessment.get("location", "") or "Unknown"
                    location_proof_url = location_assessment.get("source_url", "")
                    location_proof_snippet = location_assessment.get("snippet", "")
                    location_method = location_assessment.get("method", "")
                    location_gate_reason = "non_us_location"
                else:
                    location_gate_reason = "non_us_location"
                    location = "Unknown"
                    location_proof_url = ""
                    location_proof_snippet = ""
                    location_method = ""
            else:
                location_gate_reason = location_decision
                location = "Unknown"
                location_proof_url = ""
                location_proof_snippet = ""
                location_method = ""
            return location_gate_reason

        location_attempts = collect_location_attempts()
        location_assessment = choose_location_assessment(location_attempts)
        location_decision = location_assessment.get("decision", "no_location_signal") or "no_location_signal"
        location_gate_reason = apply_location_assessment_state()
        current_candidate_us_snippet = (
            (location_assessment.get("snippet", "") or "").strip()
            or (location_proof_snippet or "").strip()
        )
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_location_snippet = (
                location_assessment.get("snippet", "") or current_candidate_ledger.best_location_snippet
            )
            current_candidate_ledger.best_location_source_url = (
                location_assessment.get("source_url", "") or current_candidate_ledger.best_location_source_url
            )
            append_candidate_action(
                current_candidate_ledger,
                "try_location_proof",
                detail=location_decision or "no_location_signal",
            )
        strict_location_plan = candidate_planner_decision(reject_reason=location_decision if strict_verified_mode else "")
        if strict_verified_mode and strict_location_plan.get("action") == "stop_dead_end":
            reject_counts[location_decision] += 1
            candidate_reject_reason = location_decision
            record_location_debug(
                candidate_url=candidate_url,
                candidate_domain=candidate_domain,
                location_assessment=location_assessment,
                location_attempts=location_attempts,
                location_decision=location_decision,
                recovery_candidate_urls=recovery_candidate_urls,
                location_recovery_attempted=location_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
            )
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(
                reason=candidate_reject_reason,
                kept=False,
                domain=candidate_domain,
                location_recovery_attempted=location_recovery_attempted,
                listing_recovery_attempted=listing_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
            )
            continue
        if location_decision == "non_us_location":
            if effective_us_only and not (strict_verified_mode and require_us_location):
                record_location_debug(
                    candidate_url=candidate_url,
                    candidate_domain=candidate_domain,
                    location_assessment=location_assessment,
                    location_attempts=location_attempts,
                    location_decision=location_decision,
                    recovery_candidate_urls=recovery_candidate_urls,
                    location_recovery_attempted=location_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                )
                reject_counts["non_us_location"] += 1
                candidate_reject_reason = "non_us_location"
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(
                    reason=candidate_reject_reason,
                    kept=False,
                    domain=candidate_domain,
                    location_recovery_attempted=location_recovery_attempted,
                    listing_recovery_attempted=listing_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                    listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                )
                continue
        elif location_decision != "ok":
            if (effective_require_location or effective_us_only) and not (strict_verified_mode and require_us_location):
                record_location_debug(
                    candidate_url=candidate_url,
                    candidate_domain=candidate_domain,
                    location_assessment=location_assessment,
                    location_attempts=location_attempts,
                    location_decision=location_decision,
                    recovery_candidate_urls=recovery_candidate_urls,
                    location_recovery_attempted=location_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                )
                reject_counts[location_decision] += 1
                candidate_reject_reason = location_decision
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(
                    reason=candidate_reject_reason,
                    kept=False,
                    domain=candidate_domain,
                    location_recovery_attempted=location_recovery_attempted,
                    listing_recovery_attempted=listing_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                    listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                )
                continue

        title_pages: List[Tuple[str, str, BeautifulSoup]] = []
        seen_title_page_urls = set()
        for page_url, page_text, page_soup in evidence_plus:
            normalized_page_url = normalize_url(page_url)
            if not normalized_page_url or normalized_page_url in seen_title_page_urls:
                continue
            seen_title_page_urls.add(normalized_page_url)
            title_pages.append((normalized_page_url, page_text, page_soup))

        prelisting_title_resolution = resolve_primary_book_title(
            candidate_url=candidate_url,
            title_pages=title_pages,
            source_title=candidate_source_title,
            source_snippet=candidate_source_snippet,
            source_url=candidate_source_url or candidate_url,
            listing_url="",
            listing_status="missing",
            listing_page=None,
            recency_url="",
            recency_proof="",
            author_name=author_name,
        )
        prelisting_title_status = str(prelisting_title_resolution.get("status", "missing_or_weak") or "missing_or_weak")
        prelisting_title_confidence = str(prelisting_title_resolution.get("confidence", "weak") or "weak")
        strong_prelisting_title = prelisting_title_status == "ok" and prelisting_title_confidence == "strong"
        strict_bn_fallback_viable = not (
            strict_verified_mode
            and (
                (effective_require_email and not preliminary_author_email)
                or (
                    effective_require_email
                    and preliminary_author_email
                    and not email_quality_is_fully_verified(preliminary_email_quality)
                )
                or (require_us_location and preliminary_location_decision == "non_us_location")
                or (require_us_location and preliminary_publisher_location_dead_end)
            )
        )

        listing_title_hints = collect_listing_title_hints(
            candidate_url,
            evidence_plus,
            candidate_source_title,
            candidate_source_snippet,
            author_name=author_name,
        )
        valid_listing_url = ""
        row_listing_url = listing_candidates[0] if listing_candidates else ""
        listing_soup = None
        listing_text = ""
        listing_status = "missing"
        listing_fail_reason = "listing_not_found"
        listing_fetch_attempted = False
        listing_fetch_failed = 0
        listing_seen_duplicate = False
        listing_best_candidate_url = row_listing_url
        listing_best_page_kind = classify_listing_page_kind(row_listing_url) if row_listing_url else ""
        listing_best_evidence_snippet = ""
        listing_best_title = ""
        listing_attempts: List[Dict[str, str]] = []
        amazon_interstitial_confirmed = False
        amazon_interstitial_evidence_snippet = ""
        amazon_interstitial_listing_title = ""
        bn_interstitial_fallback_attempted = False
        terminal_interstitial_reason = ""

        if listing_candidates and listing_has_required_signals(" ".join(text for _, text in proof_evidence), strict=effective_listing_strict):
            valid_listing_url = listing_candidates[0]
            row_listing_url = valid_listing_url
            listing_text = " ".join(text for _, text in proof_evidence)
            listing_status = "verified"
            listing_fail_reason = ""
            listing_best_candidate_url = valid_listing_url
            listing_best_page_kind = classify_listing_page_kind(valid_listing_url)
            listing_best_evidence_snippet = proof_snippet_for_terms(
                listing_text,
                [term for term in listing_signal_terms(listing_text).values() if term],
            )

        for listing_index, listing_url in enumerate(listing_candidates):
            if valid_listing_url or terminal_interstitial_reason:
                break
            listing_page_kind = classify_listing_page_kind(listing_url)
            if amazon_interstitial_confirmed and listing_page_kind == "amazon_product":
                terminal_interstitial_reason = terminal_amazon_interstitial_reason(
                    amazon_interstitial_confirmed=amazon_interstitial_confirmed,
                    listing_candidates=listing_candidates,
                    listing_attempts=listing_attempts,
                    next_index=listing_index + 1,
                )
                if terminal_interstitial_reason:
                    listing_best_evidence_snippet = listing_best_evidence_snippet or amazon_interstitial_evidence_snippet
                    listing_best_title = listing_best_title or amazon_interstitial_listing_title
                    listing_best_candidate_url = listing_best_candidate_url or listing_url
                    listing_best_page_kind = listing_best_page_kind or listing_page_kind
                    break
                listing_attempts.append(
                    {
                        "url": listing_url,
                        "page_kind": listing_page_kind,
                        "reason": "listing_interstitial_or_redirect_state",
                        "evidence_snippet": amazon_interstitial_evidence_snippet,
                        "fetch_failure_reason": "interstitial_skip",
                        "status_code": "",
                        "listing_title": amazon_interstitial_listing_title,
                    }
                )
                continue
            if listing_url in seen_listing:
                listing_seen_duplicate = True
                listing_attempts.append(
                    {
                        "url": listing_url,
                        "page_kind": listing_page_kind,
                        "reason": "listing_already_seen",
                        "evidence_snippet": "",
                        "fetch_failure_reason": "",
                        "status_code": "",
                        "listing_title": "",
                    }
                )
                continue
            listing_fetch_attempted = True
            listing_page = fetch_resilient_html(listing_url, allow_root_fallback=False)
            time.sleep(max(0.0, args.delay))
            if listing_page.failure_reason in {"time_budget_exceeded", "fetch_budget_exceeded", "batch_runtime_exceeded"}:
                candidate_budget_failure = listing_page.failure_reason
                break
            if not listing_page.ok or listing_page.soup is None:
                listing_fetch_failed += 1
                failure_reason, evidence_snippet = classify_listing_fetch_failure(listing_page)
                listing_attempts.append(
                    {
                        "url": listing_url,
                        "page_kind": listing_page_kind,
                        "reason": failure_reason,
                        "evidence_snippet": evidence_snippet,
                        "fetch_failure_reason": listing_page.failure_reason,
                        "status_code": str(int(listing_page.status_code or 0)),
                        "listing_title": "",
                    }
                )
                if amazon_interstitial_confirmed:
                    terminal_interstitial_reason = terminal_amazon_interstitial_reason(
                        amazon_interstitial_confirmed=amazon_interstitial_confirmed,
                        listing_candidates=listing_candidates,
                        listing_attempts=listing_attempts,
                        next_index=listing_index + 1,
                    )
                    if terminal_interstitial_reason:
                        listing_best_candidate_url = listing_url or listing_best_candidate_url
                        listing_best_page_kind = listing_page_kind or listing_best_page_kind
                        listing_best_evidence_snippet = evidence_snippet or listing_best_evidence_snippet
                        break
                continue
            lsoup = listing_page.soup
            ltext = get_text(lsoup)
            if ltext and not listing_text:
                listing_text = ltext
                listing_soup = lsoup
            if listing_has_required_signals(ltext, strict=effective_listing_strict):
                valid_listing_url = listing_url
                listing_soup = lsoup
                listing_text = ltext
                row_listing_url = listing_url
                listing_status = "verified"
                listing_fail_reason = ""
                listing_best_candidate_url = listing_url
                listing_best_page_kind = classify_listing_page_kind(listing_url)
                listing_best_evidence_snippet = proof_snippet_for_terms(
                    ltext,
                    [term for term in listing_signal_terms(ltext).values() if term],
                )
                break
            listing_failure = classify_listing_failure(
                listing_url=listing_url,
                listing_text=ltext,
                listing_soup=lsoup,
                strict=effective_listing_strict,
                expected_titles=listing_title_hints,
                author_name=author_name,
            )
            if (
                listing_failure.get("reason") == "listing_interstitial_or_redirect_state"
                and listing_failure.get("page_kind") == "amazon_product"
            ):
                amazon_interstitial_confirmed = True
                amazon_interstitial_evidence_snippet = listing_failure.get("evidence_snippet", "")
                amazon_interstitial_listing_title = listing_failure.get("listing_title", "")
                if (
                    not bn_interstitial_fallback_attempted
                    and strong_prelisting_title
                    and strict_bn_fallback_viable
                    and not any(
                        classify_listing_page_kind(existing_url).startswith("barnesandnoble_")
                        for existing_url in listing_candidates
                    )
                    and not any(
                        str(attempt.get("page_kind", "")).startswith("barnesandnoble_")
                        for attempt in listing_attempts
                    )
                ):
                    bn_interstitial_fallback_attempted = True
                    listing_recovery_attempted = True
                    bn_hint_urls: List[str] = []
                    for page_url, _, page_soup in evidence_plus:
                        bn_hint_urls.extend(
                            discover_same_domain_listing_support_urls(
                                page_url,
                                page_soup,
                                limit=1,
                                author_name=author_name,
                            )
                        )
                    bn_recovery_urls = build_listing_recovery_urls(
                        contact_origin,
                        all_links,
                        sitemap_urls,
                        bn_hint_urls,
                        limit=1,
                    )
                    for recovery_url in bn_recovery_urls:
                        if recovery_url not in listing_recovery_urls:
                            listing_recovery_urls.append(recovery_url)
                        if recovery_url in evidence_urls:
                            continue
                        appended = append_evidence_page(recovery_url, allow_root_fallback=True)
                        if not appended:
                            continue
                        all_links.extend(extract_links(appended[0], appended[2]))
                        for fallback_listing_url, source_url, method in extract_listing_candidates_from_page(
                            appended[0],
                            appended[2],
                            shortlink_expander=expand_listing_shortlink_candidate,
                        ):
                            if not classify_listing_page_kind(fallback_listing_url).startswith("barnesandnoble_"):
                                continue
                            if fallback_listing_url not in listing_candidates:
                                listing_candidates.append(fallback_listing_url)
                            listing_sources.setdefault(fallback_listing_url, (source_url, method))
            listing_attempts.append(
                {
                    "url": listing_url,
                    "page_kind": listing_failure.get("page_kind", ""),
                    "reason": listing_failure.get("reason", "listing_wrong_format_or_incomplete"),
                    "evidence_snippet": listing_failure.get("evidence_snippet", ""),
                    "fetch_failure_reason": "",
                    "status_code": str(int(listing_page.status_code or 0)),
                    "listing_title": listing_failure.get("listing_title", ""),
                }
            )
            if amazon_interstitial_confirmed:
                terminal_interstitial_reason = terminal_amazon_interstitial_reason(
                    amazon_interstitial_confirmed=amazon_interstitial_confirmed,
                    listing_candidates=listing_candidates,
                    listing_attempts=listing_attempts,
                    next_index=listing_index + 1,
                )
                if terminal_interstitial_reason:
                    listing_best_candidate_url = listing_url or listing_best_candidate_url
                    listing_best_page_kind = listing_failure.get("page_kind", "") or listing_best_page_kind
                    listing_best_evidence_snippet = listing_failure.get("evidence_snippet", "") or listing_best_evidence_snippet
                    listing_best_title = listing_failure.get("listing_title", "") or listing_best_title
                    break

        if listing_status != "verified":
            if not listing_candidates:
                listing_status = "missing"
                listing_fail_reason = "listing_not_found"
            elif terminal_interstitial_reason:
                listing_status = "unverified"
                listing_fail_reason = terminal_interstitial_reason
            elif listing_fetch_attempted and listing_fetch_failed == len(listing_candidates) and listing_attempts:
                listing_status = "fetch_failed"
                best_listing_attempt = choose_best_listing_failure(listing_attempts)
                listing_fail_reason = best_listing_attempt.get("reason", "listing_fetch_failed") or "listing_fetch_failed"
                listing_best_candidate_url = best_listing_attempt.get("url", listing_best_candidate_url) or listing_best_candidate_url
                listing_best_page_kind = best_listing_attempt.get("page_kind", listing_best_page_kind) or listing_best_page_kind
                listing_best_evidence_snippet = best_listing_attempt.get("evidence_snippet", "")
                listing_best_title = best_listing_attempt.get("listing_title", "")
            else:
                listing_status = "unverified"
                if listing_seen_duplicate and not listing_fetch_attempted:
                    listing_fail_reason = "listing_already_seen"
                elif listing_fetch_attempted and listing_attempts:
                    best_listing_attempt = choose_best_listing_failure(listing_attempts)
                    listing_fail_reason = best_listing_attempt.get("reason", "listing_wrong_format_or_incomplete") or "listing_wrong_format_or_incomplete"
                    listing_best_candidate_url = best_listing_attempt.get("url", listing_best_candidate_url) or listing_best_candidate_url
                    listing_best_page_kind = best_listing_attempt.get("page_kind", listing_best_page_kind) or listing_best_page_kind
                    listing_best_evidence_snippet = best_listing_attempt.get("evidence_snippet", "")
                    listing_best_title = best_listing_attempt.get("listing_title", "")
                else:
                    listing_fail_reason = "listing_not_fetched"
            listing_fail_reason = normalize_listing_fail_reason(
                listing_status=listing_status,
                listing_fail_reason=listing_fail_reason,
                listing_title=listing_best_title,
            )
            if should_track_listing_reject_reason(
                strict_verified_mode=strict_verified_mode,
                listing_status=listing_status,
                listing_fail_reason=listing_fail_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                listing_candidates=listing_candidates,
                listing_attempts=listing_attempts,
            ):
                listing_reject_reason_counts[listing_fail_reason] += 1
        if row_listing_url in listing_sources:
            listing_enriched_from_url, listing_enrichment_method = listing_sources[row_listing_url]
        current_candidate_listing_snippet = (listing_best_evidence_snippet or "").strip()
        current_candidate_book_url = (valid_listing_url or row_listing_url or "").strip()
        if listing_status == "verified" and row_listing_url and listing_soup is None:
            listing_page = fetch_resilient_html(row_listing_url, allow_root_fallback=False)
            time.sleep(max(0.0, args.delay))
            if listing_page.failure_reason in {"time_budget_exceeded", "fetch_budget_exceeded", "batch_runtime_exceeded"}:
                candidate_budget_failure = listing_page.failure_reason
            if listing_page.ok and listing_page.soup is not None:
                listing_soup = listing_page.soup
                listing_text = get_text(listing_soup) or listing_text
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_listing_snippet = (
                listing_best_evidence_snippet or current_candidate_ledger.best_listing_snippet
            )
            current_candidate_ledger.best_listing_source_url = (
                valid_listing_url
                or row_listing_url
                or listing_best_candidate_url
                or current_candidate_ledger.best_listing_source_url
            )
            append_candidate_action(
                current_candidate_ledger,
                "try_listing_proof",
                detail=listing_status or "missing",
            )
        if candidate_budget_failure:
            reject_counts[candidate_budget_failure] += 1
            candidate_reject_reason = candidate_budget_failure
            mark_strict_location_skip("strict_non_location_failure")
            record_location_debug(
                candidate_url=candidate_url,
                candidate_domain=candidate_domain,
                location_assessment=location_assessment,
                location_attempts=location_attempts,
                location_decision=location_decision,
                recovery_candidate_urls=recovery_candidate_urls,
                location_recovery_attempted=location_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
            )
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(
                reason=candidate_reject_reason,
                kept=False,
                domain=candidate_domain,
                location_recovery_attempted=location_recovery_attempted,
                listing_recovery_attempted=listing_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
            )
            continue
        skip_strict_listing_gate = (
            strict_verified_mode
            and listing_recovery_skipped_reason
            in {
                "missing_strict_email",
                "definitive_non_us_location",
                "weak_strict_email_quality",
                "publisher_location_only_precheck",
            }
        )
        if effective_listing_strict and listing_status != "verified" and not skip_strict_listing_gate:
            mark_strict_location_skip("strict_non_location_failure")
            listing_debug_records.append(
                {
                    "candidate_url": normalize_url(candidate_url),
                    "candidate_domain": candidate_domain,
                    "validation_profile": validation_profile,
                    "listing_status": listing_status,
                    "reject_reason": listing_fail_reason,
                    "best_listing_candidate_url": listing_best_candidate_url or row_listing_url,
                    "best_listing_page_kind": listing_best_page_kind or classify_listing_page_kind(row_listing_url),
                    "best_listing_evidence_snippet": listing_best_evidence_snippet,
                    "best_listing_title": listing_best_title,
                    "listing_title_hints": listing_title_hints,
                    "listing_recovery_candidate_urls": listing_recovery_urls,
                    "listing_attempts": listing_attempts,
                }
            )
            reject_counts[
                normalize_listing_fail_reason(
                    listing_status=listing_status,
                    listing_fail_reason=listing_fail_reason,
                    listing_title=listing_best_title,
                )
            ] += 1
            candidate_reject_reason = normalize_listing_fail_reason(
                listing_status=listing_status,
                listing_fail_reason=listing_fail_reason,
                listing_title=listing_best_title,
            )
            record_location_debug(
                candidate_url=candidate_url,
                candidate_domain=candidate_domain,
                location_assessment=location_assessment,
                location_attempts=location_attempts,
                location_decision=location_decision,
                recovery_candidate_urls=recovery_candidate_urls,
                location_recovery_attempted=location_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
            )
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(
                reason=candidate_reject_reason,
                kept=False,
                domain=candidate_domain,
                location_recovery_attempted=location_recovery_attempted,
                listing_recovery_attempted=listing_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
            )
            continue

        directory_indie_url, directory_indie_snippet, directory_strength = directory_indie_proof(
            candidate_source_query,
            candidate.get("SourceType", ""),
            candidate_source_url,
            candidate_source_title,
            candidate_source_snippet,
        )
        onsite_indie_url = ""
        onsite_indie_snippet = ""
        for source_url, source_text, _ in evidence_plus:
            if find_indie_keyword(source_text):
                onsite_indie_url = source_url
                onsite_indie_snippet = proof_snippet_for_terms(source_text, INDIE_KEYWORDS)
                break
        if not onsite_indie_url and listing_text and find_indie_keyword(listing_text):
            onsite_indie_url = valid_listing_url or row_listing_url
            onsite_indie_snippet = proof_snippet_for_terms(listing_text, INDIE_KEYWORDS)

        indie_proof_url = ""
        indie_proof_snippet = ""
        indie_proof_strength = ""
        if directory_strength and onsite_indie_url:
            indie_proof_url = directory_indie_url
            indie_proof_snippet = directory_indie_snippet
            indie_proof_strength = "both"
        elif directory_strength:
            indie_proof_url = directory_indie_url
            indie_proof_snippet = directory_indie_snippet
            indie_proof_strength = "directory"
        elif onsite_indie_url:
            indie_proof_url = onsite_indie_url
            indie_proof_snippet = onsite_indie_snippet
            indie_proof_strength = "onsite"
        elif not email_only_mode:
            reject_counts["no_indie_proof"] += 1
            candidate_reject_reason = "no_indie_proof"
            mark_strict_location_skip("strict_non_location_failure")
            record_location_debug(
                candidate_url=candidate_url,
                candidate_domain=candidate_domain,
                location_assessment=location_assessment,
                location_attempts=location_attempts,
                location_decision=location_decision,
                recovery_candidate_urls=recovery_candidate_urls,
                location_recovery_attempted=location_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
            )
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(
                reason=candidate_reject_reason,
                kept=False,
                domain=candidate_domain,
                location_recovery_attempted=location_recovery_attempted,
                listing_recovery_attempted=listing_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
            )
            continue
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_indie_snippet = indie_proof_snippet or current_candidate_ledger.best_indie_snippet
            current_candidate_ledger.best_indie_source_url = indie_proof_url or current_candidate_ledger.best_indie_source_url
            _ = candidate_planner_decision()
        current_candidate_indie_snippet = (indie_proof_snippet or "").strip()

        recency_url = ""
        recency_proof = ""
        recency_status = "missing"
        recency_fail_reason = "no_on_page_date"
        recency_evidence = [(source_url, source_text) for source_url, source_text, _ in evidence_plus]
        if not strict_verified_mode:
            recency_evidence = proof_evidence
        for source_url, source_text in recency_evidence:
            if detect_recency(source_text, args.min_year, args.max_year, now=now):
                recency_url = source_url
                recency_proof = recency_proof_snippet(source_text)
                recency_status = "verified"
                recency_fail_reason = ""
                break
        if not recency_url and listing_text and detect_recency(listing_text, args.min_year, args.max_year, now=now):
            recency_url = valid_listing_url or row_listing_url
            recency_proof = recency_proof_snippet(listing_text)
            recency_status = "verified"
            recency_fail_reason = ""
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_recency_snippet = recency_proof or current_candidate_ledger.best_recency_snippet
            current_candidate_ledger.best_recency_source_url = recency_url or current_candidate_ledger.best_recency_source_url
            append_candidate_action(
                current_candidate_ledger,
                "try_recency_proof",
                detail=recency_status or "missing",
            )
            _ = candidate_planner_decision(
                reject_reason=recency_fail_reason if strict_verified_mode and recency_status != "verified" else ""
            )

        best_email = collect_best_email_record()
        author_email = (best_email.get("email", "") or "").strip()
        author_email_source_url = (best_email.get("source_url", "") or "").strip()
        author_email_proof_snippet = (best_email.get("proof_snippet", "") or "").strip()
        email_quality = (best_email.get("quality", "") or "").strip()
        current_candidate_email = author_email
        current_candidate_email_snippet = author_email_proof_snippet
        if current_candidate_ledger is not None:
            current_candidate_ledger.best_email_snippet = author_email_proof_snippet or current_candidate_ledger.best_email_snippet
            current_candidate_ledger.best_email_source_url = author_email_source_url or current_candidate_ledger.best_email_source_url
            append_candidate_action(
                current_candidate_ledger,
                "try_email_proof",
                detail="verified" if author_email else "missing",
            )
            _ = candidate_planner_decision(
                reject_reason="no_visible_author_email" if strict_verified_mode and effective_require_email and not author_email else ""
            )
        contact_url, contact_url_method = choose_contact_url(
            author_email=author_email,
            author_email_source_url=author_email_source_url,
            contact_page=contact_page,
            subscribe_page=subscribe_page,
            press_kit_url=press_kit_url,
            media_url=media_url,
            author_website=author_website,
            candidate_url=candidate_url,
        )
        if effective_require_email and not author_email:
            missing_email_reason = "no_visible_author_email" if (strict_verified_mode or email_only_mode) else "no_author_email"
            reject_counts[missing_email_reason] += 1
            candidate_reject_reason = missing_email_reason
            location_recovery_skipped_reason = location_recovery_skipped_reason or (
                "missing_strict_email" if strict_verified_mode else location_recovery_skipped_reason
            )
            record_location_debug(
                candidate_url=candidate_url,
                candidate_domain=candidate_domain,
                location_assessment=location_assessment,
                location_attempts=location_attempts,
                location_decision=location_decision,
                recovery_candidate_urls=recovery_candidate_urls,
                location_recovery_attempted=location_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
            )
            add_stage_seconds(candidate_domain, "verify", verify_started_at)
            finalize_candidate_budget(
                reason=candidate_reject_reason,
                kept=False,
                domain=candidate_domain,
                location_recovery_attempted=location_recovery_attempted,
                listing_recovery_attempted=listing_recovery_attempted,
                location_recovery_skipped_reason=location_recovery_skipped_reason,
                listing_recovery_skipped_reason=listing_recovery_skipped_reason,
            )
            continue
        if getattr(args, "require_contact_path", False):
            has_contact_path = is_actionable_contact_path(
                contact_page,
                author_website,
                candidate_url,
                strict=bool(getattr(args, "contact_path_strict", False)),
            )
            has_subscribe_path = bool(subscribe_page)
            if not (has_contact_path or has_subscribe_path):
                reject_counts["no_contact_or_subscribe_path"] += 1
                candidate_reject_reason = "no_contact_or_subscribe_path"
                mark_strict_location_skip("strict_non_location_failure")
                record_location_debug(
                    candidate_url=candidate_url,
                    candidate_domain=candidate_domain,
                    location_assessment=location_assessment,
                    location_attempts=location_attempts,
                    location_decision=location_decision,
                    recovery_candidate_urls=recovery_candidate_urls,
                    location_recovery_attempted=location_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                )
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(
                    reason=candidate_reject_reason,
                    kept=False,
                    domain=candidate_domain,
                    location_recovery_attempted=location_recovery_attempted,
                    listing_recovery_attempted=listing_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                    listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                )
                continue

        listing_page = None
        if listing_soup is not None and (valid_listing_url or row_listing_url):
            listing_page = (valid_listing_url or row_listing_url, listing_text, listing_soup)

        title_resolution = resolve_primary_book_title(
            candidate_url=candidate_url,
            title_pages=title_pages,
            source_title=candidate_source_title,
            source_snippet=candidate_source_snippet,
            source_url=candidate_source_url or candidate_url,
            listing_url=valid_listing_url or row_listing_url,
            listing_status=listing_status,
            listing_page=listing_page,
            recency_url=recency_url,
            recency_proof=recency_proof,
            author_name=author_name,
        )
        book_title = str(title_resolution.get("title", "") or "")
        book_title_source_url = str(title_resolution.get("source_url", "") or "")
        book_title_method = str(title_resolution.get("method", "fallback") or "fallback")
        book_title_score = int(title_resolution.get("score", 0) or 0)
        book_title_confidence = str(title_resolution.get("confidence", "weak") or "weak")
        book_title_status = str(title_resolution.get("status", "missing_or_weak") or "missing_or_weak")
        book_title_reject_reason = str(title_resolution.get("reject_reason", "") or "")
        book_title_top_candidates = serialize_title_candidates(title_resolution.get("top_candidates", []), limit=3)
        if book_title_status != "ok":
            book_title = ""
        elif not is_plausible_book_title(
            book_title,
            author_name=author_name,
            context={"source_method": book_title_method, "strong_book_evidence": book_title_method in STRICT_MASTER_TITLE_METHODS},
        ):
            book_title = ""
            book_title_confidence = "weak"
            book_title_status = "missing_or_weak"
            book_title_reject_reason = "post_selection_plausibility"
        if current_candidate_metrics is not None:
            current_candidate_metrics.book_title = book_title

        listing_title = ""
        if listing_page is not None:
            listing_title_resolution = resolve_listing_title_oracle(
                valid_listing_url or row_listing_url,
                listing_text,
                listing_soup,
                author_name=author_name,
            )
            listing_title = compact_text(str(listing_title_resolution.get("title", "") or ""))

        if strict_verified_mode:
            strict_best_source_url = choose_best_astra_source_url(
                author_email_source_url,
                contact_page,
                author_website,
                candidate_source_url,
                location_proof_url,
                indie_proof_url,
                recency_url,
                valid_listing_url or row_listing_url,
            )
            if astra_outbound_mode:
                fully_verified_ok, fully_verified_reason = row_meets_astra_outbound_profile(
                    author_name=author_name,
                    book_title=book_title,
                    book_title_status=book_title_status,
                    book_title_method=book_title_method,
                    author_email=author_email,
                    author_email_quality=email_quality,
                    author_email_record=best_email,
                    author_email_source_url=author_email_source_url,
                    location=location,
                    location_proof_url=location_proof_url,
                    indie_proof_strength=indie_proof_strength,
                    indie_proof_url=indie_proof_url,
                    listing_url=valid_listing_url or row_listing_url,
                    listing_status=listing_status,
                    listing_fail_reason=listing_fail_reason,
                    recency_status=recency_status,
                    recency_url=recency_url,
                    listing_title=listing_title,
                    location_decision=location_decision,
                    best_source_url=strict_best_source_url,
                    require_us_location=require_us_location,
                )
            else:
                fully_verified_ok, fully_verified_reason = row_meets_fully_verified_profile(
                    author_name=author_name,
                    book_title=book_title,
                    book_title_status=book_title_status,
                    book_title_method=book_title_method,
                    author_email=author_email,
                    author_email_quality=email_quality,
                    author_email_record=best_email,
                    location=location,
                    indie_proof_strength=indie_proof_strength,
                    listing_status=listing_status,
                    listing_fail_reason=listing_fail_reason,
                    recency_status=recency_status,
                    listing_title=listing_title,
                    require_us_location=require_us_location,
                    location_decision=location_decision,
                    best_source_url=strict_best_source_url,
                    require_best_source_url=False,
                )
            location_exempt_ok = False
            if require_us_location and fully_verified_reason in LOCATION_REJECT_REASONS:
                if astra_outbound_mode:
                    location_exempt_ok, _ = row_meets_astra_outbound_profile(
                        author_name=author_name,
                        book_title=book_title,
                        book_title_status=book_title_status,
                        book_title_method=book_title_method,
                        author_email=author_email,
                        author_email_quality=email_quality,
                        author_email_record=best_email,
                        author_email_source_url=author_email_source_url,
                        location=location or "Unknown",
                        location_proof_url=location_proof_url,
                        indie_proof_strength=indie_proof_strength,
                        indie_proof_url=indie_proof_url,
                        listing_url=valid_listing_url or row_listing_url,
                        listing_status=listing_status,
                        listing_fail_reason=listing_fail_reason,
                        recency_status=recency_status,
                        recency_url=recency_url,
                        listing_title=listing_title,
                        location_decision="",
                        best_source_url=strict_best_source_url,
                        require_us_location=False,
                    )
                else:
                    location_exempt_ok, _ = row_meets_fully_verified_profile(
                        author_name=author_name,
                        book_title=book_title,
                        book_title_status=book_title_status,
                        book_title_method=book_title_method,
                        author_email=author_email,
                        author_email_quality=email_quality,
                        author_email_record=best_email,
                        location=location or "Unknown",
                        indie_proof_strength=indie_proof_strength,
                        listing_status=listing_status,
                        listing_fail_reason=listing_fail_reason,
                        recency_status=recency_status,
                        listing_title=listing_title,
                        require_us_location=False,
                        location_decision="",
                        best_source_url=strict_best_source_url,
                        require_best_source_url=False,
                    )
                location_planner = candidate_planner_decision(reject_reason=fully_verified_reason)
                if (
                    location_exempt_ok
                    and location_recovery_mode == "same_domain"
                    and location_recovery_pages > 0
                    and location_planner.get("action") == "try_location_proof"
                ):
                    location_recovery_attempted = True
                    if current_candidate_ledger is not None:
                        append_candidate_action(current_candidate_ledger, "try_location_proof", detail="recovery")
                    recovery_candidate_urls = build_location_recovery_urls(
                        contact_origin,
                        list(all_links),
                        sitemap_urls,
                        limit=max(location_recovery_pages, len(recovery_candidate_urls)),
                    )
                    for recovery_url in recovery_candidate_urls:
                        if recovery_url in evidence_urls:
                            continue
                        appended = append_evidence_page(recovery_url, allow_root_fallback=True)
                        if appended:
                            all_links.extend(extract_links(appended[0], appended[2]))
                    all_links = dedupe_urls(all_links)
                    location_attempts = collect_location_attempts()
                    location_assessment = choose_location_assessment(location_attempts)
                    location_decision = location_assessment.get("decision", "no_location_signal") or "no_location_signal"
                    location_gate_reason = apply_location_assessment_state()
                    current_candidate_us_snippet = (
                        (location_assessment.get("snippet", "") or "").strip()
                        or (location_proof_snippet or "").strip()
                    )
                    strict_best_source_url = choose_best_astra_source_url(
                        author_email_source_url,
                        contact_page,
                        author_website,
                        candidate_source_url,
                        location_proof_url,
                        indie_proof_url,
                        recency_url,
                        valid_listing_url or row_listing_url,
                    )
                    if astra_outbound_mode:
                        fully_verified_ok, fully_verified_reason = row_meets_astra_outbound_profile(
                            author_name=author_name,
                            book_title=book_title,
                            book_title_status=book_title_status,
                            book_title_method=book_title_method,
                            author_email=author_email,
                            author_email_quality=email_quality,
                            author_email_record=best_email,
                            author_email_source_url=author_email_source_url,
                            location=location,
                            location_proof_url=location_proof_url,
                            indie_proof_strength=indie_proof_strength,
                            indie_proof_url=indie_proof_url,
                            listing_url=valid_listing_url or row_listing_url,
                            listing_status=listing_status,
                            listing_fail_reason=listing_fail_reason,
                            recency_status=recency_status,
                            recency_url=recency_url,
                            listing_title=listing_title,
                            location_decision=location_decision,
                            best_source_url=strict_best_source_url,
                            require_us_location=require_us_location,
                        )
                    else:
                        fully_verified_ok, fully_verified_reason = row_meets_fully_verified_profile(
                            author_name=author_name,
                            book_title=book_title,
                            book_title_status=book_title_status,
                            book_title_method=book_title_method,
                            author_email=author_email,
                            author_email_quality=email_quality,
                            author_email_record=best_email,
                            location=location,
                            indie_proof_strength=indie_proof_strength,
                            listing_status=listing_status,
                            listing_fail_reason=listing_fail_reason,
                            recency_status=recency_status,
                            listing_title=listing_title,
                            require_us_location=require_us_location,
                            location_decision=location_decision,
                            best_source_url=strict_best_source_url,
                            require_best_source_url=False,
                        )
                elif location_exempt_ok and location_planner.get("action") != "try_location_proof":
                    location_recovery_skipped_reason = (
                        location_recovery_skipped_reason
                        or location_planner.get("reason", "")
                        or "planner_skip"
                    )
                elif location_exempt_ok:
                    location_recovery_skipped_reason = location_recovery_skipped_reason or "recovery_disabled"
                else:
                    location_recovery_skipped_reason = location_recovery_skipped_reason or "multiple_strict_failures"
            elif require_us_location and not fully_verified_ok:
                mark_strict_location_skip("strict_non_location_failure")
            if current_candidate_ledger is not None:
                current_candidate_ledger.best_location_snippet = (
                    location_assessment.get("snippet", "") or location_proof_snippet or current_candidate_ledger.best_location_snippet
                )
                current_candidate_ledger.best_location_source_url = (
                    location_assessment.get("source_url", "") or location_proof_url or current_candidate_ledger.best_location_source_url
                )
                current_candidate_ledger.best_listing_snippet = (
                    listing_best_evidence_snippet or current_candidate_ledger.best_listing_snippet
                )
                current_candidate_ledger.best_listing_source_url = (
                    valid_listing_url
                    or row_listing_url
                    or listing_best_candidate_url
                    or current_candidate_ledger.best_listing_source_url
                )
            if not fully_verified_ok:
                if require_us_location and fully_verified_reason in LOCATION_REJECT_REASONS and location_exempt_ok:
                    near_miss_location_rows.append(
                        {
                            "AuthorName": author_name,
                            "BookTitle": book_title,
                            "AuthorEmail": author_email,
                            "SourceURL": candidate_source_url,
                            "RejectReason": fully_verified_reason,
                            "BestLocationSnippet": location_assessment.get("snippet", "") or location_proof_snippet,
                            "CandidateRecoveryURLs": "|".join(recovery_candidate_urls),
                        }
                    )
                reject_counts[fully_verified_reason] += 1
                candidate_reject_reason = fully_verified_reason
                record_location_debug(
                    candidate_url=candidate_url,
                    candidate_domain=candidate_domain,
                    location_assessment=location_assessment,
                    location_attempts=location_attempts,
                    location_decision=location_decision,
                    recovery_candidate_urls=recovery_candidate_urls,
                    location_recovery_attempted=location_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                )
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(
                    reason=candidate_reject_reason,
                    kept=False,
                    domain=candidate_domain,
                    location_recovery_attempted=location_recovery_attempted,
                    listing_recovery_attempted=listing_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                    listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                )
                continue

        if email_only_mode:
            email_only_ok, email_only_reason = row_meets_email_only_profile(
                author_name=author_name,
                author_email=author_email,
                author_email_record=best_email,
                source_url=candidate_source_url,
            )
            if not email_only_ok:
                reject_counts[email_only_reason] += 1
                candidate_reject_reason = email_only_reason
                record_location_debug(
                    candidate_url=candidate_url,
                    candidate_domain=candidate_domain,
                    location_assessment=location_assessment,
                    location_attempts=location_attempts,
                    location_decision=location_decision,
                    recovery_candidate_urls=recovery_candidate_urls,
                    location_recovery_attempted=location_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                )
                add_stage_seconds(candidate_domain, "verify", verify_started_at)
                finalize_candidate_budget(
                    reason=candidate_reject_reason,
                    kept=False,
                    domain=candidate_domain,
                    location_recovery_attempted=location_recovery_attempted,
                    listing_recovery_attempted=listing_recovery_attempted,
                    location_recovery_skipped_reason=location_recovery_skipped_reason,
                    listing_recovery_skipped_reason=listing_recovery_skipped_reason,
                )
                continue

        row = {
            "AuthorName": author_name,
            "BookTitle": book_title,
            "BookTitleMethod": book_title_method,
            "BookTitleScore": str(book_title_score),
            "BookTitleConfidence": book_title_confidence,
            "BookTitleStatus": book_title_status,
            "BookTitleRejectReason": book_title_reject_reason,
            "BookTitleTopCandidates": book_title_top_candidates,
            "AuthorEmail": author_email,
            "EmailQuality": email_quality,
            "AuthorNameSourceURL": author_name_source_url,
            "BookTitleSourceURL": book_title_source_url,
            "AuthorEmailSourceURL": author_email_source_url,
            "AuthorEmailProofSnippet": author_email_proof_snippet,
            "AuthorWebsite": author_website,
            "ContactPageURL": contact_page,
            "SubscribeURL": subscribe_page,
            "PressKitURL": press_kit_url,
            "MediaURL": media_url,
            "ContactURL": contact_url,
            "ContactURLMethod": contact_url_method,
            "Location": location,
            "LocationProofURL": location_proof_url,
            "LocationProofSnippet": location_proof_snippet,
            "LocationMethod": location_method,
            "SourceURL": candidate_source_url,
            "SourceTitle": candidate_source_title,
            "SourceSnippet": candidate_source_snippet,
            "IndieProofURL": indie_proof_url,
            "IndieProofSnippet": indie_proof_snippet,
            "IndieProofStrength": indie_proof_strength,
            "ListingURL": valid_listing_url or row_listing_url,
            "ListingStatus": listing_status,
            "ListingFailReason": listing_fail_reason,
            "ListingEnrichedFromURL": listing_enriched_from_url,
            "ListingEnrichmentMethod": listing_enrichment_method,
            "RecencyProofURL": recency_url,
            "RecencyProofSnippet": recency_proof,
            "RecencyStatus": recency_status,
            "RecencyFailReason": recency_fail_reason,
        }
        if current_candidate_metrics is not None:
            current_candidate_metrics.email_source_url = author_email_source_url
            current_candidate_metrics.location_source_url = location_proof_url
            current_candidate_metrics.location_value = location
            current_candidate_metrics.indie_proof_url = indie_proof_url
            current_candidate_metrics.indie_proof_value = indie_proof_snippet
            current_candidate_metrics.retailer_listing_url = valid_listing_url or row_listing_url
            current_candidate_metrics.retailer_listing_status = listing_status
            current_candidate_metrics.recency_source_url = recency_url
            current_candidate_metrics.recency_value = recency_proof
        results.append(row)
        candidate_kept = True
        if row["ListingURL"]:
            seen_listing.add(row["ListingURL"])
        record_location_debug(
            candidate_url=candidate_url,
            candidate_domain=candidate_domain,
            location_assessment=location_assessment,
            location_attempts=location_attempts,
            location_decision=location_decision,
            recovery_candidate_urls=recovery_candidate_urls,
            location_recovery_attempted=location_recovery_attempted,
            location_recovery_skipped_reason=location_recovery_skipped_reason,
        )
        add_stage_seconds(candidate_domain, "verify", verify_started_at)
        finalize_candidate_budget(
            reason="kept",
            kept=True,
            domain=candidate_domain,
            location_recovery_attempted=location_recovery_attempted,
            listing_recovery_attempted=listing_recovery_attempted,
            location_recovery_skipped_reason=location_recovery_skipped_reason,
            listing_recovery_skipped_reason=listing_recovery_skipped_reason,
        )

        if idx % 10 == 0:
            print(f"[INFO] processed {idx}/{len(candidates)} candidates, kept {len(results)}")
        time.sleep(max(0.0, args.delay))

    write_domain_cache(domain_cache_path, robots_cache, robots_text_cache, sitemap_cache, domain_metrics)
    write_location_debug_records(location_debug_path, location_debug_records)
    write_listing_debug_records(listing_debug_path, listing_debug_records)
    write_near_miss_location_rows(near_miss_location_path, near_miss_location_rows)
    per_domain_stats = {
        domain: {
            "fetch_count": metrics.fetch_count,
            "total_seconds": round(metrics.total_seconds, 4),
            "avg_latency": round((metrics.total_seconds / metrics.fetch_count), 4) if metrics.fetch_count else 0.0,
            "timeout_count": metrics.timeout_count,
            "prefilter_seconds": round(metrics.prefilter_seconds, 4),
            "verify_seconds": round(metrics.verify_seconds, 4),
            "dominant_phase": dominant_phase_name(metrics.prefilter_seconds, metrics.verify_seconds),
        }
        for domain, metrics in sorted(domain_metrics.items())
    }
    top_domain_budget_burn = summarize_domain_budget_burn(domain_metrics)
    top_candidate_budget_burn = summarize_candidate_budget_burn(candidate_budget_records)
    candidate_state_counts = Counter((record.get("current_state", "") or "harvested") for record in candidate_ledger_records)
    planned_action_counts = Counter(
        (
            plan_candidate_next_action(
                CandidateProofLedger(
                    candidate_url=str(record.get("candidate_url", "") or ""),
                    candidate_domain=str(record.get("candidate_domain", "") or ""),
                    current_state=str(record.get("current_state", "") or "harvested"),
                    last_attempted_action=str(record.get("last_attempted_action", "") or ""),
                    best_email_snippet=str(record.get("best_email_snippet", "") or ""),
                    best_email_source_url=str(record.get("best_email_source_url", "") or ""),
                    best_location_snippet=str(record.get("best_location_snippet", "") or ""),
                    best_location_source_url=str(record.get("best_location_source_url", "") or ""),
                    best_listing_snippet=str(record.get("best_listing_snippet", "") or ""),
                    best_listing_source_url=str(record.get("best_listing_source_url", "") or ""),
                    best_recency_snippet=str(record.get("best_recency_snippet", "") or ""),
                    best_recency_source_url=str(record.get("best_recency_source_url", "") or ""),
                    best_indie_snippet=str(record.get("best_indie_snippet", "") or ""),
                    best_indie_source_url=str(record.get("best_indie_source_url", "") or ""),
                    reject_reason_if_any=str(record.get("reject_reason_if_any", "") or ""),
                    action_history=list(record.get("action_history", []) or []),
                ),
                strict_verified_mode=strict_verified_mode,
                require_us_location=require_us_location,
                effective_require_email=effective_require_email,
                effective_listing_strict=effective_listing_strict,
            ).get("action", "")
            or "none"
        )
        for record in candidate_ledger_records
    )
    setattr(
        args,
        "_validation_stats",
        {
            "validation_profile": validation_profile,
            "stage_timings": {
                "prefilter_seconds": round(prefilter_seconds_total, 4),
                "verify_seconds": round(verify_seconds_total, 4),
            },
            "prefilter_candidates": prefilter_candidates,
            "verify_candidates": verify_candidates,
            "batch_runtime_exceeded": batch_runtime_exceeded,
            "cache_hits": dict(cache_hits),
            "domain_cache_path": str(domain_cache_path) if domain_cache_path else "",
            "location_debug_path": str(location_debug_path) if location_debug_path else "",
            "listing_debug_path": str(listing_debug_path) if listing_debug_path else "",
            "near_miss_location_path": str(near_miss_location_path) if near_miss_location_path else "",
            "near_miss_location_rows": len(near_miss_location_rows),
            "location_decision_counts": dict(location_decision_counts),
            "location_method_counts": dict(location_method_counts),
            "location_debug_samples": location_debug_records[:10],
            "listing_reject_reason_counts": dict(listing_reject_reason_counts),
            "listing_debug_samples": listing_debug_records[:10],
            "candidate_state_counts": dict(candidate_state_counts),
            "planned_action_counts": dict(planned_action_counts),
            "proof_ledger_samples": candidate_ledger_records[:10],
            "candidate_outcome_records": candidate_outcome_records,
            "per_domain": per_domain_stats,
            "candidate_budget_burn_count": len(candidate_budget_records),
            "top_domain_budget_burn": top_domain_budget_burn,
            "top_candidate_budget_burn": top_candidate_budget_burn,
            "max_fetches_per_domain": max_fetches_per_domain,
            "max_seconds_per_domain": max_seconds_per_domain,
            "max_timeouts_per_domain": max_timeouts_per_domain,
            "max_total_runtime": max_total_runtime,
            "max_concurrency": max_concurrency,
        },
    )

    if reject_counts:
        summary = ", ".join(f"{k}={v}" for k, v in reject_counts.most_common())
        print(f"[INFO] reject counts: {summary}")

    return results, reject_counts, len(candidates)


def main() -> int:
    args = parse_args()
    rows, reject_counts, total_candidates = validate_candidates(args)
    write_rows(args.output, rows)
    if args.stats_output:
        ensure_parent(Path(args.stats_output))
        validation_profile = str(getattr(args, "validation_profile", "default") or "default")
        runtime_stats = dict(getattr(args, "_validation_stats", {}) or {})
        stats = {
            "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "validation_profile": validation_profile,
            "total_candidates": total_candidates,
            "kept_rows": len(rows),
            "reject_reasons": dict(reject_counts),
            "listing_status_counts": dict(Counter((row.get("ListingStatus", "") or "missing") for row in rows)),
            "book_title_method_counts": dict(Counter((row.get("BookTitleMethod", "") or "fallback") for row in rows)),
            "book_title_confidence_counts": dict(Counter((row.get("BookTitleConfidence", "") or "weak") for row in rows)),
            "book_title_status_counts": dict(Counter((row.get("BookTitleStatus", "") or "ok") for row in rows)),
            "book_title_downgraded": sum(
                1 for row in rows if (row.get("BookTitleStatus", "") or "ok").strip().lower() != "ok"
            ),
            "book_title_downgrade_reasons": dict(
                Counter(
                    (row.get("BookTitleRejectReason", "") or "unspecified")
                    for row in rows
                    if (row.get("BookTitleStatus", "") or "ok").strip().lower() != "ok"
                )
            ),
            "indie_proof_strength_counts": dict(Counter((row.get("IndieProofStrength", "") or "missing") for row in rows)),
            "recency_status_counts": dict(Counter((row.get("RecencyStatus", "") or "missing") for row in rows)),
            "email_quality_counts": dict(Counter((row.get("EmailQuality", "") or "missing") for row in rows if row.get("AuthorEmail", ""))),
            "leads_with_author_email": sum(1 for row in rows if (row.get("AuthorEmail", "") or "").strip()),
            "fully_verified_rows": sum(
                1
                for row in rows
                if (
                    row_meets_astra_outbound_profile(
                        author_name=(row.get("AuthorName", "") or "").strip(),
                        book_title=(row.get("BookTitle", "") or "").strip(),
                        book_title_status=(row.get("BookTitleStatus", "") or "").strip(),
                        book_title_method=(row.get("BookTitleMethod", "") or "").strip(),
                        author_email=(row.get("AuthorEmail", "") or "").strip(),
                        author_email_quality=(row.get("EmailQuality", "") or "").strip(),
                        author_email_record={"visible_text": "true" if (row.get("AuthorEmailSourceURL", "") or "").strip() else "false"},
                        author_email_source_url=(row.get("AuthorEmailSourceURL", "") or "").strip(),
                        location=(row.get("Location", "") or "").strip(),
                        location_proof_url=(row.get("LocationProofURL", "") or "").strip(),
                        indie_proof_strength=(row.get("IndieProofStrength", "") or "").strip(),
                        indie_proof_url=(row.get("IndieProofURL", "") or "").strip(),
                        listing_url=(row.get("ListingURL", "") or "").strip(),
                        listing_status=(row.get("ListingStatus", "") or "").strip(),
                        listing_fail_reason=(row.get("ListingFailReason", "") or "").strip(),
                        recency_status=(row.get("RecencyStatus", "") or "").strip(),
                        recency_url=(row.get("RecencyProofURL", "") or "").strip(),
                        listing_title="",
                        location_decision="",
                        best_source_url=choose_best_astra_source_url(
                            (row.get("AuthorEmailSourceURL", "") or "").strip(),
                            (row.get("ContactPageURL", "") or "").strip(),
                            (row.get("AuthorWebsite", "") or "").strip(),
                            (row.get("SourceURL", "") or "").strip(),
                            (row.get("LocationProofURL", "") or "").strip(),
                            (row.get("IndieProofURL", "") or "").strip(),
                            (row.get("RecencyProofURL", "") or "").strip(),
                            (row.get("ListingURL", "") or "").strip(),
                        ),
                    )[0]
                    if is_astra_outbound_profile(validation_profile)
                    else row_meets_fully_verified_profile(
                        author_name=(row.get("AuthorName", "") or "").strip(),
                        book_title=(row.get("BookTitle", "") or "").strip(),
                        book_title_status=(row.get("BookTitleStatus", "") or "").strip(),
                        book_title_method=(row.get("BookTitleMethod", "") or "").strip(),
                        author_email=(row.get("AuthorEmail", "") or "").strip(),
                        author_email_quality=(row.get("EmailQuality", "") or "").strip(),
                        author_email_record={"visible_text": "true" if (row.get("AuthorEmailSourceURL", "") or "").strip() else "false"},
                        location=(row.get("Location", "") or "").strip(),
                        indie_proof_strength=(row.get("IndieProofStrength", "") or "").strip(),
                        listing_status=(row.get("ListingStatus", "") or "").strip(),
                        listing_fail_reason=(row.get("ListingFailReason", "") or "").strip(),
                        recency_status=(row.get("RecencyStatus", "") or "").strip(),
                        listing_title="",
                        require_us_location=validation_profile in US_STRICT_VERIFIED_PROFILES,
                    )[0]
                )
            ),
        }
        stats.update(runtime_stats)
        with open(args.stats_output, "w", encoding="utf-8") as fh:
            json.dump(stats, fh, indent=2, ensure_ascii=True)
    print(f"[OK] wrote {len(rows)} validated prospects to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
