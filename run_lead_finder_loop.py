#!/usr/bin/env python3
"""
Batch loop runner:
repeats the pipeline in small runs, accumulates unique leads, and stops at a target total.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Callable, Dict, Iterable, List, Tuple
from urllib.parse import urljoin, urlparse

from lead_utils import canonical_listing_key, normalize_person_name, registrable_domain
from persistent_dedupe import PersistentDedupeStore
from pipeline_paths import csv_output, ensure_parent, ensure_runtime_dirs, json_output, state_output
from prospect_dedupe import OUTPUT_COLUMNS, dedupe
from prospect_validate import (
    NEAR_MISS_LOCATION_COLUMNS,
    STRICT_MASTER_TITLE_METHODS,
    US_STATE_CODES,
    US_STATE_NAMES,
    build_session,
    choose_author_email_record,
    dedupe_urls,
    extract_links,
    extract_page_email_records,
    fetch_with_meta,
    find_contact_link_from_links,
    find_subscribe_link_from_links,
    is_plausible_author_name,
    is_us_location,
    normalize_url,
    select_supporting_pages,
)
from runtime_config import DEFAULT_CONFIG_PATH, allowed_year_bounds, config_arg_default, load_runtime_config

ASTRA_OUTBOUND_PROFILE = "astra_outbound"
VERIFIED_NO_US_PROFILE = "verified_no_us"
STRICT_INTERACTIVE_PROFILE = "strict_interactive"
STRICT_FULL_PROFILE = "strict_full"
AGENT_HUNT_PROFILE = "agent_hunt"
STRICT_VERIFIED_PROFILES = {
    "fully_verified",
    ASTRA_OUTBOUND_PROFILE,
    VERIFIED_NO_US_PROFILE,
    STRICT_INTERACTIVE_PROFILE,
    STRICT_FULL_PROFILE,
}
US_STRICT_PROFILES = {"fully_verified", ASTRA_OUTBOUND_PROFILE, STRICT_INTERACTIVE_PROFILE, STRICT_FULL_PROFILE}

MINIMAL_COLUMNS = ["AuthorName", "AuthorEmail", "SourceURL"]
VERIFIED_OUTPUT_COLUMNS = ["AuthorName", "AuthorEmail", "SourceURL"]
SCOUTED_OUTPUT_COLUMNS = ["AuthorName", "AuthorEmail", "SourceURL"]
CONTACT_QUEUE_COLUMNS = [
    "AuthorName",
    "BookTitle",
    "BookTitleMethod",
    "BookTitleScore",
    "BookTitleConfidence",
    "BookTitleStatus",
    "BookTitleRejectReason",
    "BookTitleTopCandidates",
    "AuthorEmail",
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
    "AuthorEmailSourceURL",
    "AuthorEmailProofSnippet",
    "EmailQuality",
    "AuthorNameSourceURL",
    "BookTitleSourceURL",
]
REJECTED_COLUMNS = [
    "RunID",
    "AuthorName",
    "BookTitle",
    "CandidateURL",
    "CandidateDomain",
    "SourceURL",
    "BookURL",
    "Email",
    "Confidence",
    "PrimaryFailReason",
    "FailReasons",
    "RejectReason",
    "CurrentState",
    "NextAction",
    "NextActionReason",
    "EmailSnippet",
    "USSnippet",
    "IndieSnippet",
    "ListingSnippet",
]
LIVE_STATUS_PATH = Path(state_output("live_status.json"))

QUERY_GENRES = [
    "fantasy",
    "science fiction",
    "romance",
    "thriller",
    "mystery",
    "horror",
    "historical fiction",
    "young adult",
    "urban fantasy",
    "cozy mystery",
    "paranormal romance",
    "space opera",
    "epic fantasy",
    "dark fantasy",
    "cyberpunk",
    "dystopian",
]

QUERY_TEMPLATES = [
    '"{genre}" "indie author" "official website"',
    '"{genre}" "self-published author" "contact"',
    '"{genre}" "independent author" "newsletter"',
    '"{genre}" "official author website" "contact"',
    '"{genre}" "author newsletter"',
    '"{year}" "{genre}" "indie author" "official website"',
]
BASE_QUERY_VARIANTS = [
    [
        '"indie author" "official website" "contact"',
        '"self-published author" "official website"',
        '"independently published author" "newsletter"',
        '"author website" "new release" "indie author"',
    ],
    [
        '"indie novelist" "official website"',
        '"self-published writer" "contact"',
        '"author website" "mailing list"',
        '"independent author" "new release" "contact"',
    ],
    [
        '"author website" "books" "contact"',
        '"indie fantasy author" "official website"',
        '"self-published author" "about the author"',
        '"independent author" "media kit"',
    ],
]

CANDIDATE_COLUMNS = [
    "CandidateURL",
    "SourceType",
    "SourceQuery",
    "SourceURL",
    "SourceTitle",
    "SourceSnippet",
    "DiscoveredAtUTC",
]
AUTHOR_PATH_RE = re.compile(r"/(?:author|authors|about|bio)/([^/?#]+)", flags=re.IGNORECASE)
SECRET_FLAGS = {"--google-api-key", "--brave-api-key"}
ROLE_EMAIL_LOCALS = {"admin", "contact", "hello", "help", "hi", "info", "mail", "office", "sales", "support", "team"}
SCOUT_ACCEPTABLE_EMAIL_QUALITIES = {"same_domain", "labeled_off_domain", "risky_role"}
AGENT_HUNT_TIER_1 = "tier_1_safest"
AGENT_HUNT_TIER_2 = "tier_2_usable_with_caution"
AGENT_HUNT_TIER_3 = "tier_3_weak_but_usable"
AGENT_HUNT_TIER_PRIORITY = {
    AGENT_HUNT_TIER_1: 0,
    AGENT_HUNT_TIER_2: 1,
    AGENT_HUNT_TIER_3: 2,
}
AGENT_HUNT_TIER_TO_DECISION = {
    AGENT_HUNT_TIER_1: "KEEP",
    AGENT_HUNT_TIER_2: "RECHECK",
    AGENT_HUNT_TIER_3: "RECHECK",
}
AGENT_HUNT_REP_EMAIL_TOKENS = {
    "agent",
    "agency",
    "assistant",
    "booking",
    "bookings",
    "manager",
    "media",
    "press",
    "pr",
    "publicist",
    "publicity",
    "rights",
}
AGENT_HUNT_REP_EMAIL_DOMAIN_HINTS = ("agency", "assist", "media", "press", "publicity", "rights")
NON_US_HINTS = (
    "australia",
    "canada",
    "england",
    "ireland",
    "new zealand",
    "ontario",
    "scotland",
    "united kingdom",
    "uk",
    "victoria",
    "wales",
)
NON_US_TLDS = (".au", ".ca", ".co.nz", ".co.uk", ".de", ".ie", ".in", ".net.au", ".org.au", ".uk")
INDIE_HINTS = (
    "indie author",
    "independent author",
    "self-published",
    "independently published",
    "kdp",
    "kindle direct publishing",
    "draft2digital",
    "ingramspark",
    "barnes & noble press",
    "barnes and noble press",
    "bookbaby",
    "lulu",
)
TITLE_HINTS = (
    "author of",
    "series",
    "book one",
    "book 1",
    "novel",
    "novels",
    "saga",
    "trilogy",
    "chronicles",
)
LISTING_HINTS = ("amazon", "barnes", "noble", "buy", "books", "titles", "works", "bibliography", "series")
SCOUT_GENERIC_AUTHOR_SNIPPETS = (
    "creative writing",
    "new releases",
    "official website",
    "author website",
    "military fantasy",
    "comics & manga",
    "comics and manga",
    "welcome to",
    "home of",
)
SCOUT_GENERIC_AUTHOR_TOKENS = {
    "book",
    "books",
    "comics",
    "creative",
    "fantasy",
    "fiction",
    "history",
    "manga",
    "official",
    "releases",
    "review",
    "reviews",
    "website",
    "writer",
    "writing",
}
SCOUT_LISTING_HARD_DEAD_END_REASONS = {"listing_not_found"}
SCOUT_LISTING_RETAILER_FRICTION_REASONS = {
    "listing_amazon_interstitial_no_bn_candidate",
    "listing_amazon_interstitial_bn_blocked",
    "listing_amazon_interstitial_bn_failed_other",
    "listing_interstitial_or_redirect_state",
}
SCOUT_LISTING_FAILURE_PREFIX = "listing_"
SCOUT_LISTING_HOT_PATH_SOURCE_TYPES = {"epic_directory"}
SCOUT_LISTING_HOT_PATH_SOURCE_QUERIES = {"epic:author-directory"}
SCOUT_LISTING_HOT_PATH_SOURCE_DOMAINS = {"epicindie.net"}
SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_TYPES = {"epic_directory"}
SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_QUERIES = {"epic:author-directory"}
SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_DOMAINS = {"epicindie.net"}
SCOUT_SOURCE_QUALITY_FEEDBACK_REASONS = {"enterprise_or_famous"}
AGENT_HUNT_EPIC_DIRECTORY_MAX_CANDIDATES_PER_BATCH = 24
SCOUT_ENTERPRISE_OR_FAMOUS_HINTS = (
    "new york times bestseller",
    "nyt bestseller",
    "usa today bestseller",
    "wall street journal bestseller",
    "wsj bestseller",
    "wikipedia",
    "penguin random house",
    "harpercollins",
    "simon & schuster",
    "hachette",
    "macmillan",
    "big five publisher",
    "imprint of",
)
SCOUT_EMAIL_RECOVERY_TIMEOUT_SECONDS = 4.0
SCOUT_EMAIL_RECOVERY_RETRY_SECONDS = 45.0
SCOUT_EMAIL_RECOVERY_MAX_PAGES = 6
SCOUT_EMAIL_RECOVERY_SUPPORT_PAGE_LIMIT = 4
SCOUT_EMAIL_RECOVERY_PATH_HINTS = (
    "/contact",
    "/about",
    "/about-author",
    "/about-the-author",
    "/bio",
    "/newsletter",
    "/subscribe",
    "/press",
    "/media",
)
SCOUT_SOURCE_TYPE_BY_DOMAIN = {
    "epicindie.net": "epic_directory",
    "iabx.org": "iabx_directory",
    "independentauthornetwork.com": "ian_directory",
}
INTAKE_SCORE_HIGH_THRESHOLD = 40
INTAKE_SCORE_LOW_THRESHOLD = 15
INTAKE_SCORE_BANDS: List[Tuple[str, int | None]] = [
    ("80_plus", 80),
    ("60_79", 60),
    ("40_59", 40),
    ("20_39", 20),
    ("0_19", 0),
    ("lt_0", None),
]
VISIBLE_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
VISIBLE_OBFUSCATED_EMAIL_RE = re.compile(
    r"\b([A-Z0-9._%+-]+)\s*(?:\(|\[)?at(?:\)|\])?\s*([A-Z0-9.-]+)\s*(?:\(|\[)?dot(?:\)|\])?\s*([A-Z]{2,})\b",
    re.IGNORECASE,
)
US_STATE_NAME_PATTERN = "|".join(re.escape(name) for name in sorted(US_STATE_NAMES, key=len, reverse=True))
US_STATE_CODE_PATTERN = "|".join(sorted(US_STATE_CODES))
AUTHOR_US_LOCATION_PATTERNS = [
    re.compile(
        rf"\b(?:based in|author from|lives in|resides in|living in|novelist in|writer in)\s+([A-Z][A-Za-z.\-']+(?:\s+[A-Z][A-Za-z.\-']+){{0,3}}(?:,\s*(?:{US_STATE_NAME_PATTERN}|{US_STATE_CODE_PATTERN}))?)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b[A-Z][A-Za-z.\-']+(?:\s+[A-Z][A-Za-z.\-']+){{0,3}},\s*(?:{US_STATE_NAME_PATTERN}|{US_STATE_CODE_PATTERN})\b"
    ),
    re.compile(rf"\b(?:{US_STATE_NAME_PATTERN}),\s*USA\b", re.IGNORECASE),
]


def normalize_validation_profile(value: str) -> str:
    return str(value or "default").strip().lower()


def is_fully_verified_profile(value: str) -> bool:
    return normalize_validation_profile(value) in STRICT_VERIFIED_PROFILES


def profile_requires_us_location(value: str) -> bool:
    return normalize_validation_profile(value) in US_STRICT_PROFILES


def is_agent_hunt_profile(value: str) -> bool:
    return normalize_validation_profile(value) == AGENT_HUNT_PROFILE


def candidate_identity_key(
    candidate_url: str,
    source_type: str = "",
    source_query: str = "",
    source_url: str = "",
) -> str:
    return "||".join(
        [
            normalize_url(candidate_url) or (candidate_url or "").strip(),
            (source_type or "").strip().lower(),
            (source_query or "").strip().lower(),
            normalize_url(source_url) or (source_url or "").strip(),
        ]
    )


def candidate_identity_from_row(row: Dict[str, str]) -> str:
    return candidate_identity_key(
        row.get("CandidateURL", ""),
        row.get("SourceType", ""),
        row.get("SourceQuery", ""),
        row.get("SourceURL", ""),
    )


def candidate_identity_from_outcome(record: Dict[str, object]) -> str:
    return candidate_identity_key(
        str(record.get("candidate_url", "") or ""),
        str(record.get("source_type", "") or ""),
        str(record.get("source_query", "") or ""),
        str(record.get("source_url", "") or ""),
    )


def candidate_score_band(score: int) -> str:
    for label, floor in INTAKE_SCORE_BANDS:
        if floor is None or score >= floor:
            return label
    return "lt_0"


def looks_like_non_us_domain(candidate_url: str) -> bool:
    domain = registrable_domain(candidate_url).lower()
    return any(domain.endswith(tld) for tld in NON_US_TLDS)


def extract_visible_email_hints(text: str) -> Tuple[int, int]:
    personal = 0
    generic = 0
    seen: set[str] = set()
    for match in VISIBLE_EMAIL_RE.findall(text or ""):
        email = str(match).strip().lower()
        if email in seen:
            continue
        seen.add(email)
        local = email.split("@", 1)[0]
        if local in ROLE_EMAIL_LOCALS:
            generic += 1
        else:
            personal += 1
    for local, domain, tld in VISIBLE_OBFUSCATED_EMAIL_RE.findall(text or ""):
        email = f"{local}@{domain}.{tld}".strip().lower()
        if email in seen:
            continue
        seen.add(email)
        if local.lower() in ROLE_EMAIL_LOCALS:
            generic += 1
        else:
            personal += 1
    return personal, generic


def has_author_tied_us_location_hint(text: str, *, source_title: str = "") -> str:
    combined = " ".join(part for part in (source_title, text) if part).strip()
    if not combined:
        return ""
    lowered = combined.lower()
    if not any(token in lowered for token in ("author", "writer", "novelist", "lives in", "based in", "resides in", "living in")):
        if not is_plausible_author_name(re.sub(r"^\s*author\s+", "", source_title or "", flags=re.IGNORECASE).strip()):
            return ""
    for pattern in AUTHOR_US_LOCATION_PATTERNS:
        match = pattern.search(combined)
        if not match:
            continue
        snippet = match.group(0).strip()
        if is_us_location(snippet):
            return snippet
    return ""


def has_non_us_hint(text: str, candidate_url: str, *, require_us_location: bool) -> bool:
    if not require_us_location:
        return False
    lowered = (text or "").lower()
    if any(hint in lowered for hint in NON_US_HINTS):
        return True
    return looks_like_non_us_domain(candidate_url)


def looks_like_plausible_author_identity(source_title: str, candidate_url: str) -> bool:
    cleaned = re.sub(r"^\s*author\s+", "", (source_title or "").strip(), flags=re.IGNORECASE)
    if cleaned and is_plausible_author_name(cleaned):
        return True
    domain = registrable_domain(candidate_url)
    domain_slug = re.sub(r"\.(?:com|net|org|info|co)$", "", domain, flags=re.IGNORECASE).replace("-", " ")
    return bool(domain_slug and is_plausible_author_name(domain_slug))


def normalize_source_author_label(value: str) -> str:
    return re.sub(r"^\s*author\s+", "", (value or "").strip(), flags=re.IGNORECASE)


def text_has_enterprise_or_famous_hint(text: str) -> bool:
    lowered = (text or "").strip().lower()
    return any(hint in lowered for hint in SCOUT_ENTERPRISE_OR_FAMOUS_HINTS)


def is_source_quality_hot_path(source_type: str, source_query: str, source_domain: str) -> bool:
    return bool(
        source_type in SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_TYPES
        or source_query in SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_QUERIES
        or source_domain in SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_DOMAINS
    )


def classify_agent_hunt_source_quality_feedback_reason(reject_reason: str) -> str:
    normalized = (reject_reason or "").strip().lower()
    if normalized.startswith("bad_author_name"):
        return "bad_author_name"
    if normalized in SCOUT_SOURCE_QUALITY_FEEDBACK_REASONS:
        return normalized
    return ""


def score_candidate_intake(
    row: Dict[str, str],
    *,
    validation_profile: str,
    agent_hunt_listing_feedback: Dict[str, object] | None = None,
    agent_hunt_source_quality_feedback: Dict[str, object] | None = None,
) -> Dict[str, object]:
    require_us_location = profile_requires_us_location(validation_profile)
    strict_verified = is_fully_verified_profile(validation_profile)
    scout_mode = is_agent_hunt_profile(validation_profile)
    candidate_url = row.get("CandidateURL", "") or ""
    source_type = (row.get("SourceType", "") or "").strip().lower()
    source_query = (row.get("SourceQuery", "") or "").strip().lower()
    source_title = (row.get("SourceTitle", "") or "").strip()
    source_snippet = (row.get("SourceSnippet", "") or "").strip()
    source_url = row.get("SourceURL", "") or ""
    source_domain = registrable_domain(source_url)
    combined = " ".join(part for part in (source_title, source_snippet) if part).strip()
    lowered = combined.lower()
    normalized_source_title = normalize_source_author_label(source_title)
    parsed = urlparse(candidate_url)
    path = (parsed.path or "/").lower()
    score = 0
    components: Dict[str, int] = {}

    def add_component(name: str, value: int) -> None:
        nonlocal score
        if value == 0:
            return
        components[name] = components.get(name, 0) + value
        score += value

    if path in {"", "/"}:
        add_component("root_author_site", 4)
    elif any(path.startswith(prefix) for prefix in ("/about", "/about-author", "/author", "/bio", "/contact")):
        add_component("author_page_entry", 6)
    elif any(token in path for token in ("/shop", "/store", "/product", "/cart")):
        add_component("storefront_entry", -10)

    if source_type == "epic_directory":
        add_component("rich_directory_source", 2)
        if text_has_enterprise_or_famous_hint(combined):
            add_component("epic_enterprise_or_famous_hint", -20)
        elif normalized_source_title:
            if looks_like_generic_scout_author_name(normalized_source_title):
                add_component("epic_generic_author_title", -18)
            elif not is_plausible_author_name(normalized_source_title):
                add_component("epic_implausible_author_title", -12)
            else:
                add_component("epic_named_author_title", 4)
    elif source_type == "iabx_directory":
        add_component("thin_directory_source", -4)

    if "author directory | independent authors book experience" in lowered:
        add_component("generic_directory_snippet", -12)

    personal_email_count, generic_email_count = extract_visible_email_hints(combined)
    if personal_email_count:
        add_component("visible_personal_email_hint", 55)
    elif generic_email_count:
        add_component("visible_role_email_hint", 10)
    elif strict_verified or scout_mode:
        add_component("no_visible_email_hint", -8)

    location_hint = has_author_tied_us_location_hint(source_snippet, source_title=source_title)
    if location_hint:
        add_component("author_us_location_hint", 36)
    elif require_us_location and "usa" in lowered:
        add_component("generic_usa_hint", 2)
    elif scout_mode and "usa" in lowered:
        add_component("generic_usa_hint", 1)
    elif require_us_location:
        add_component("missing_us_location_hint", -10)
    elif scout_mode:
        add_component("missing_us_location_hint", -4)

    if has_non_us_hint(combined, candidate_url, require_us_location=require_us_location or scout_mode):
        add_component("non_us_hint", -45)

    if any(hint in lowered for hint in INDIE_HINTS):
        add_component("indie_hint", 8)

    if any(hint in lowered for hint in TITLE_HINTS):
        add_component("title_hint", 10)

    if any(hint in lowered for hint in LISTING_HINTS) or any(hint in path for hint in ("/books", "/titles", "/works", "/series", "/bibliography", "/buy")):
        add_component("listing_path_hint", 8)

    if looks_like_plausible_author_identity(source_title, candidate_url):
        add_component("plausible_author_identity", 5)
    else:
        add_component("weak_author_identity", -18)

    if any(hint in lowered for hint in ("wikipedia", "new york times bestseller", "usa today bestseller", "wall street journal bestseller")):
        add_component("obvious_dead_end_signal", -20)

    if scout_mode and agent_hunt_listing_feedback:
        query_penalty = int((agent_hunt_listing_feedback.get("query_penalties", {}) or {}).get(source_query, 0) or 0)
        source_type_penalty = int((agent_hunt_listing_feedback.get("source_type_penalties", {}) or {}).get(source_type, 0) or 0)
        source_domain_penalty = int(
            (agent_hunt_listing_feedback.get("source_domain_penalties", {}) or {}).get(source_domain, 0) or 0
        )
        total_penalty = min(12, query_penalty + source_type_penalty + source_domain_penalty)
        if total_penalty:
            add_component("prior_listing_friction_penalty", -total_penalty)
    if scout_mode and agent_hunt_source_quality_feedback:
        query_penalty = int((agent_hunt_source_quality_feedback.get("query_penalties", {}) or {}).get(source_query, 0) or 0)
        source_type_penalty = int(
            (agent_hunt_source_quality_feedback.get("source_type_penalties", {}) or {}).get(source_type, 0) or 0
        )
        source_domain_penalty = int(
            (agent_hunt_source_quality_feedback.get("source_domain_penalties", {}) or {}).get(source_domain, 0) or 0
        )
        total_penalty = min(12, query_penalty + source_type_penalty + source_domain_penalty)
        if total_penalty:
            add_component("prior_source_quality_penalty", -total_penalty)

    positive_features = [
        {"feature": name, "contribution": value}
        for name, value in sorted(((name, value) for name, value in components.items() if value > 0), key=lambda item: item[1], reverse=True)
    ]
    negative_features = [
        {"feature": name, "contribution": value}
        for name, value in sorted(((name, value) for name, value in components.items() if value < 0), key=lambda item: item[1])
    ]

    return {
        "identity": candidate_identity_from_row(row),
        "candidate_url": normalize_url(candidate_url) or candidate_url,
        "source_type": source_type,
        "source_query": source_query,
        "source_url": normalize_url(source_url) or source_url,
        "score": int(score),
        "band": candidate_score_band(int(score)),
        "components": dict(sorted(components.items(), key=lambda item: item[0])),
        "reasons": list(components.keys()),
        "top_positive_features": positive_features[:5],
        "top_negative_features": negative_features[:5],
    }


def order_candidates_for_strict_validation(
    candidate_rows: List[Dict[str, str]],
    *,
    validation_profile: str,
    agent_hunt_listing_feedback: Dict[str, object] | None = None,
    agent_hunt_source_quality_feedback: Dict[str, object] | None = None,
) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    scored_rows: List[Tuple[int, int, Dict[str, str], Dict[str, object]]] = []
    score_map: Dict[str, Dict[str, object]] = {}
    score_map_by_url: Dict[str, Dict[str, object]] = {}
    distribution: Counter[str] = Counter()
    raw_scores: List[int] = []
    enabled = is_fully_verified_profile(validation_profile) or is_agent_hunt_profile(validation_profile)

    for index, row in enumerate(candidate_rows):
        scored = score_candidate_intake(
            row,
            validation_profile=validation_profile,
            agent_hunt_listing_feedback=agent_hunt_listing_feedback,
            agent_hunt_source_quality_feedback=agent_hunt_source_quality_feedback,
        )
        score = int(scored["score"])
        scored_rows.append((score, index, row, scored))
        score_map[str(scored["identity"])] = scored
        candidate_url = str(scored.get("candidate_url", "") or "")
        existing = score_map_by_url.get(candidate_url)
        if existing is None or int(existing.get("score", -10_000) or -10_000) < score:
            score_map_by_url[candidate_url] = scored
        distribution[str(scored["band"])] += 1
        raw_scores.append(score)

    if enabled:
        scored_rows.sort(key=lambda item: (-item[0], item[1]))

    ordered_rows = [item[2] for item in scored_rows]
    ordered_scores = [item[3] for item in scored_rows]
    return ordered_rows, {
        "enabled": enabled,
        "distribution": dict(distribution),
        "avg_score": round(mean(raw_scores), 2) if raw_scores else 0.0,
        "max_score": max(raw_scores) if raw_scores else 0,
        "min_score": min(raw_scores) if raw_scores else 0,
        "score_records": ordered_scores,
        "score_map": score_map,
        "score_map_by_url": score_map_by_url,
        "high_score_threshold": INTAKE_SCORE_HIGH_THRESHOLD,
        "low_score_threshold": INTAKE_SCORE_LOW_THRESHOLD,
        "top_candidates": [
            {
                "candidate_url": record["candidate_url"],
                "score": record["score"],
                "band": record["band"],
                "top_positive_features": list(record["top_positive_features"])[:3],
                "top_negative_features": list(record["top_negative_features"])[:3],
            }
            for record in ordered_scores[:10]
        ],
        "agent_hunt_listing_feedback_applied": {
            "query_penalties": dict((agent_hunt_listing_feedback or {}).get("query_penalties", {}) or {}),
            "source_type_penalties": dict((agent_hunt_listing_feedback or {}).get("source_type_penalties", {}) or {}),
            "source_domain_penalties": dict((agent_hunt_listing_feedback or {}).get("source_domain_penalties", {}) or {}),
        },
        "agent_hunt_source_quality_feedback_applied": {
            "query_penalties": dict((agent_hunt_source_quality_feedback or {}).get("query_penalties", {}) or {}),
            "source_type_penalties": dict((agent_hunt_source_quality_feedback or {}).get("source_type_penalties", {}) or {}),
            "source_domain_penalties": dict((agent_hunt_source_quality_feedback or {}).get("source_domain_penalties", {}) or {}),
        },
    }


def summarize_candidate_intake_scores(
    *,
    ordered_candidate_rows: List[Dict[str, str]],
    scoring_context: Dict[str, object],
    candidate_outcome_records: List[Dict[str, object]],
    orchestration_stats: Dict[str, object],
) -> Dict[str, object]:
    score_map = dict(scoring_context.get("score_map", {}) or {})
    score_map_by_url = dict(scoring_context.get("score_map_by_url", {}) or {})
    high_threshold = int(scoring_context.get("high_score_threshold", INTAKE_SCORE_HIGH_THRESHOLD) or INTAKE_SCORE_HIGH_THRESHOLD)
    low_threshold = int(scoring_context.get("low_score_threshold", INTAKE_SCORE_LOW_THRESHOLD) or INTAKE_SCORE_LOW_THRESHOLD)
    first_slice_candidate_count = 0
    slice_summaries = list(orchestration_stats.get("slice_summaries", []) or [])
    if slice_summaries:
        first_slice_candidate_count = int(slice_summaries[0].get("candidate_count", 0) or 0)
    processed_candidates = int(orchestration_stats.get("processed_candidates", 0) or 0)

    scores_by_outcome: Dict[str, List[int]] = {"verified": [], "dead_end": [], "failed": []}
    high_score_failures: Counter[str] = Counter()
    feature_contrib_sums: Dict[str, Counter[str]] = {
        "verified": Counter(),
        "dead_end": Counter(),
        "failed": Counter(),
    }
    feature_contrib_counts: Dict[str, Counter[str]] = {
        "verified": Counter(),
        "dead_end": Counter(),
        "failed": Counter(),
    }
    false_positive_feature_sums: Counter[str] = Counter()
    false_positive_feature_counts: Counter[str] = Counter()
    dead_end_negative_feature_sums: Counter[str] = Counter()
    dead_end_negative_feature_counts: Counter[str] = Counter()
    score_buckets_by_outcome: Dict[str, Counter[str]] = defaultdict(Counter)
    outcome_records_by_identity: Dict[str, Dict[str, object]] = {}
    for record in candidate_outcome_records:
        identity = candidate_identity_from_outcome(record)
        scored = score_map.get(identity)
        if scored is None:
            scored = score_map_by_url.get(normalize_url(str(record.get("candidate_url", "") or "")) or str(record.get("candidate_url", "") or ""))
        if not scored:
            continue
        outcome_records_by_identity[identity] = record
        score = int(scored.get("score", 0) or 0)
        kept = bool(record.get("kept", False))
        current_state = str(record.get("current_state", "") or "")
        reject_reason = str(record.get("reject_reason", "") or "")
        if kept or (current_state == "verified" and reject_reason in {"", "kept"}):
            outcome = "verified"
        elif current_state == "dead_end":
            outcome = "dead_end"
        else:
            outcome = "failed"
        scores_by_outcome[outcome].append(score)
        score_buckets_by_outcome[candidate_score_band(score)][outcome] += 1
        for feature_name, contribution in dict(scored.get("components", {}) or {}).items():
            feature_contrib_sums[outcome][feature_name] += int(contribution or 0)
            feature_contrib_counts[outcome][feature_name] += 1
            if outcome != "verified" and int(contribution or 0) > 0:
                false_positive_feature_sums[feature_name] += int(contribution or 0)
                false_positive_feature_counts[feature_name] += 1
            if outcome == "dead_end" and int(contribution or 0) < 0:
                dead_end_negative_feature_sums[feature_name] += abs(int(contribution or 0))
                dead_end_negative_feature_counts[feature_name] += 1
        if score >= high_threshold and reject_reason and outcome != "verified":
            high_score_failures[reject_reason] += 1

    low_score_total = 0
    low_score_skipped = 0
    low_score_delayed = 0
    listing_penalized_total = 0
    source_quality_penalized_total = 0
    listing_penalized_source_types: Counter[str] = Counter()
    listing_penalized_source_queries: Counter[str] = Counter()
    listing_penalized_source_domains: Counter[str] = Counter()
    source_quality_penalized_source_types: Counter[str] = Counter()
    source_quality_penalized_source_queries: Counter[str] = Counter()
    source_quality_penalized_source_domains: Counter[str] = Counter()
    candidate_score_records: List[Dict[str, object]] = []
    for index, row in enumerate(ordered_candidate_rows):
        scored = score_map.get(candidate_identity_from_row(row))
        if not scored:
            continue
        score = int(scored.get("score", 0) or 0)
        outcome_record = outcome_records_by_identity.get(str(scored.get("identity", "") or ""))
        candidate_score_records.append(
            {
                "candidate_url": scored.get("candidate_url", ""),
                "source_type": scored.get("source_type", ""),
                "source_query": scored.get("source_query", ""),
                "source_url": scored.get("source_url", ""),
                "score": score,
                "band": scored.get("band", ""),
                "components": dict(scored.get("components", {}) or {}),
                "top_positive_features": list(scored.get("top_positive_features", []) or []),
                "top_negative_features": list(scored.get("top_negative_features", []) or []),
                "processed": index < processed_candidates,
                "outcome_state": str((outcome_record or {}).get("current_state", "") or ""),
                "reject_reason": str((outcome_record or {}).get("reject_reason", "") or ""),
                "kept": bool((outcome_record or {}).get("kept", False)),
            }
        )
        if int(dict(scored.get("components", {}) or {}).get("prior_listing_friction_penalty", 0) or 0) < 0:
            listing_penalized_total += 1
            source_type = str(scored.get("source_type", "") or "").strip()
            source_query = str(scored.get("source_query", "") or "").strip()
            source_domain = registrable_domain(str(scored.get("source_url", "") or "").strip())
            if source_type:
                listing_penalized_source_types[source_type] += 1
            if source_query:
                listing_penalized_source_queries[source_query] += 1
            if source_domain:
                listing_penalized_source_domains[source_domain] += 1
        if int(dict(scored.get("components", {}) or {}).get("prior_source_quality_penalty", 0) or 0) < 0:
            source_quality_penalized_total += 1
            source_type = str(scored.get("source_type", "") or "").strip()
            source_query = str(scored.get("source_query", "") or "").strip()
            source_domain = registrable_domain(str(scored.get("source_url", "") or "").strip())
            if source_type:
                source_quality_penalized_source_types[source_type] += 1
            if source_query:
                source_quality_penalized_source_queries[source_query] += 1
            if source_domain:
                source_quality_penalized_source_domains[source_domain] += 1
        if score >= low_threshold:
            continue
        low_score_total += 1
        if index >= processed_candidates:
            low_score_skipped += 1
        elif first_slice_candidate_count and index >= first_slice_candidate_count:
            low_score_delayed += 1

    avg_feature_contributions_by_outcome = {
        outcome: {
            feature: round(
                feature_contrib_sums[outcome][feature] / max(1, feature_contrib_counts[outcome][feature]),
                2,
            )
            for feature in sorted(feature_contrib_sums[outcome])
        }
        for outcome in ("verified", "dead_end", "failed")
    }
    top_false_positive_features = [
        {
            "feature": feature,
            "avg_contribution": round(false_positive_feature_sums[feature] / max(1, false_positive_feature_counts[feature]), 2),
            "count": int(false_positive_feature_counts[feature]),
        }
        for feature, _ in sorted(
            false_positive_feature_sums.items(),
            key=lambda item: (item[1], false_positive_feature_counts[item[0]]),
            reverse=True,
        )[:10]
    ]
    strongest_negative_predictors = [
        {
            "feature": feature,
            "avg_contribution": round(
                -dead_end_negative_feature_sums[feature] / max(1, dead_end_negative_feature_counts[feature]),
                2,
            ),
            "count": int(dead_end_negative_feature_counts[feature]),
        }
        for feature, _ in sorted(
            dead_end_negative_feature_sums.items(),
            key=lambda item: (item[1], dead_end_negative_feature_counts[item[0]]),
            reverse=True,
        )[:10]
    ]

    return {
        "enabled": bool(scoring_context.get("enabled", False)),
        "prevalidate_candidate_score_distribution": dict(scoring_context.get("distribution", {}) or {}),
        "prevalidate_candidate_score_summary": {
            "avg_score": float(scoring_context.get("avg_score", 0.0) or 0.0),
            "max_score": int(scoring_context.get("max_score", 0) or 0),
            "min_score": int(scoring_context.get("min_score", 0) or 0),
            "high_score_threshold": high_threshold,
            "low_score_threshold": low_threshold,
        },
        "average_candidate_score_by_outcome": {
            key: round(mean(values), 2) if values else 0.0 for key, values in scores_by_outcome.items()
        },
        "average_feature_contributions_by_outcome": avg_feature_contributions_by_outcome,
        "top_false_positive_score_features": top_false_positive_features,
        "strongest_negative_predictors_dead_end": strongest_negative_predictors,
        "top_high_score_failure_reasons": dict(high_score_failures.most_common(10)),
        "score_buckets_by_outcome": {
            bucket: dict(score_buckets_by_outcome.get(bucket, {})) for bucket, _ in INTAKE_SCORE_BANDS
        },
        "low_score_candidates_total": low_score_total,
        "low_score_candidates_skipped": low_score_skipped,
        "low_score_candidates_delayed": low_score_delayed,
        "listing_feedback_penalized_candidates_total": listing_penalized_total,
        "listing_feedback_penalized_source_types": summarize_counter(
            listing_penalized_source_types,
            key_name="source",
        ),
        "listing_feedback_penalized_source_queries": summarize_counter(
            listing_penalized_source_queries,
            key_name="query",
        ),
        "listing_feedback_penalized_source_domains": summarize_counter(
            listing_penalized_source_domains,
            key_name="domain",
        ),
        "source_quality_feedback_penalized_candidates_total": source_quality_penalized_total,
        "source_quality_feedback_penalized_source_types": summarize_counter(
            source_quality_penalized_source_types,
            key_name="source",
        ),
        "source_quality_feedback_penalized_source_queries": summarize_counter(
            source_quality_penalized_source_queries,
            key_name="query",
        ),
        "source_quality_feedback_penalized_source_domains": summarize_counter(
            source_quality_penalized_source_domains,
            key_name="domain",
        ),
        "top_prevalidate_candidates": list(scoring_context.get("top_candidates", []) or []),
        "candidate_intake_score_records": candidate_score_records,
    }


def apply_validation_profile_defaults(args: argparse.Namespace) -> None:
    profile = normalize_validation_profile(getattr(args, "validation_profile", "default"))
    args.validation_profile = profile
    if profile == AGENT_HUNT_PROFILE:
        if int(getattr(args, "goal_final", 0) or 0) == 0 and int(getattr(args, "goal_total", 100) or 100) == 100:
            args.goal_final = 20
        return
    if profile == STRICT_INTERACTIVE_PROFILE:
        if int(getattr(args, "max_runs", 20) or 20) == 20:
            args.max_runs = 20
        if int(getattr(args, "max_stale_runs", 5) or 5) == 5:
            args.max_stale_runs = 5
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

    if int(getattr(args, "goal_final", 0) or 0) == 0 and int(getattr(args, "goal_total", 100) or 100) == 100:
        args.goal_final = 20
    if int(getattr(args, "max_runs", 20) or 20) == 20:
        args.max_runs = 50
    if int(getattr(args, "max_stale_runs", 5) or 5) == 5:
        args.max_stale_runs = max(int(getattr(args, "max_runs", 50) or 50), 50)
    if int(getattr(args, "batch_min", 10) or 10) == 10:
        args.batch_min = 20
    if int(getattr(args, "batch_max", 20) or 20) == 20:
        args.batch_max = 40
    if int(getattr(args, "target", 40) or 40) == 40:
        args.target = 80
    args.min_candidates = max(80, int(getattr(args, "min_candidates", 80) or 80))
    args.require_location_proof = True
    args.us_only = True
    args.listing_strict = True
    args.merge_policy = "strict"


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    argv_list = list(argv) if argv is not None else sys.argv[1:]
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help=argparse.SUPPRESS)
    pre_args, _ = pre_parser.parse_known_args(argv_list)
    runtime_config = load_runtime_config(Path(pre_args.config))
    min_year_default, max_year_default = allowed_year_bounds(runtime_config)
    parser = argparse.ArgumentParser(description="Run lead finder in repeated small batches.")
    parser.add_argument("--config", default=str(pre_args.config), help="YAML defaults file.")
    parser.add_argument("--goal-total", type=int, default=100, help="Stop when total unique leads reaches this number.")
    parser.add_argument(
        "--goal-final",
        type=int,
        default=int(config_arg_default(runtime_config, "goal_final", 0, aliases=("target_final",)) or 0),
        help="Optional final kept-row target. When set, this overrides --goal-total for stop conditions.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=int(config_arg_default(runtime_config, "max_runs", 20) or 20),
        help="Maximum number of batch runs.",
    )
    parser.add_argument(
        "--max-stale-runs",
        type=int,
        default=int(config_arg_default(runtime_config, "max_stale_runs", 5) or 5),
        help="Stop early if this many runs in a row add zero new leads.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=3,
        help="Stop early after this many failed runs in a row.",
    )
    parser.add_argument(
        "--max-empty-candidate-runs",
        type=int,
        default=2,
        help="Stop early after this many successful runs in a row produce zero filtered candidates.",
    )
    parser.add_argument(
        "--max-zero-validated-runs",
        type=int,
        default=3,
        help="Stop early after this many successful runs in a row produce zero validated rows.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Pause between runs.")
    parser.add_argument(
        "--batch-min",
        type=int,
        default=int(config_arg_default(runtime_config, "batch_min", 10) or 10),
        help="Per-run minimum final leads (warning threshold).",
    )
    parser.add_argument(
        "--batch-max",
        type=int,
        default=int(config_arg_default(runtime_config, "batch_max", 20) or 20),
        help="Per-run maximum final leads.",
    )
    parser.add_argument("--master-output", default=csv_output("all_prospects.csv"), help="Accumulated deduped output CSV.")
    parser.add_argument(
        "--minimal-output",
        default=csv_output("author_email_source.csv"),
        help="3-column lead export CSV (AuthorName, AuthorEmail, SourceURL).",
    )
    parser.add_argument(
        "--no-minimal-output",
        action="store_true",
        help="Disable writing the minimal outreach export file.",
    )
    parser.add_argument(
        "--minimal-with-header",
        action="store_true",
        help="Write header row in minimal export (default is no header).",
    )
    parser.add_argument(
        "--verified-output",
        default=csv_output("fully_verified_leads.csv"),
        help="3-column no-header export for fully_verified mode.",
    )
    parser.add_argument(
        "--scouted-output",
        default=csv_output("scouted_leads.csv"),
        help="3-column no-header export for agent_hunt mode.",
    )
    parser.add_argument(
        "--no-scouted-output",
        action="store_true",
        help="Disable writing the agent_hunt 3-column export.",
    )
    parser.add_argument(
        "--no-verified-output",
        action="store_true",
        help="Disable writing the fully verified 3-column export.",
    )
    parser.add_argument(
        "--contact-queue-output",
        default=csv_output("contact_queue.csv"),
        help="Contact-path queue CSV for leads without public emails.",
    )
    parser.add_argument(
        "--no-contact-queue-output",
        action="store_true",
        help="Disable writing contact queue output.",
    )
    parser.add_argument(
        "--near-miss-location-output",
        default=csv_output("near_miss_location.csv"),
        help="Aggregated CSV for strict rows that fail only on U.S. author location.",
    )
    parser.add_argument(
        "--no-near-miss-location-output",
        action="store_true",
        help="Disable writing the aggregated near-miss location export.",
    )
    parser.add_argument("--runs-dir", default="outputs/runs", help="Directory for per-run output files.")

    # Forwarded pipeline knobs.
    parser.add_argument(
        "--target",
        type=int,
        default=int(config_arg_default(runtime_config, "target", 40) or 40),
        help="Harvest target candidates per run.",
    )
    parser.add_argument(
        "--min-candidates",
        type=int,
        default=int(config_arg_default(runtime_config, "min_candidates", 80, aliases=("harvest_minimum",)) or 80),
        help="Minimum harvested candidates to attempt before accepting a shortfall.",
    )
    parser.add_argument("--per-query", type=int, default=30, help="Harvest results per query.")
    parser.add_argument("--goodreads-pages", type=int, default=3, help="Goodreads pages per seed.")
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
    parser.add_argument("--max-per-domain", type=int, default=0, help="Harvest domain cap (0 disables).")
    parser.add_argument("--pause-seconds", type=float, default=1.2, help="Harvest request pause.")
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=int(config_arg_default(runtime_config, "max_candidates", 80) or 80),
        help="Validator candidate cap per run.",
    )
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
    parser.add_argument("--delay", type=float, default=0.3, help="Validator delay between requests.")
    parser.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout.")
    parser.add_argument("--min-year", type=int, default=min_year_default, help="Recency minimum year.")
    parser.add_argument("--max-year", type=int, default=max_year_default, help="Recency maximum year.")
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
    parser.add_argument(
        "--disable-auto-queries",
        action="store_true",
        help="Disable per-run rotating queries when --queries-file is not provided.",
    )
    parser.add_argument("--require-email", action="store_true", help="Only keep leads with AuthorEmail.")
    parser.add_argument(
        "--require-location-proof",
        action="store_true",
        help="Only keep leads with explicit location text found during validation.",
    )
    parser.add_argument(
        "--us-only",
        action="store_true",
        help="Only keep US-based leads (from detected location text).",
    )
    parser.add_argument(
        "--require-contact-path",
        action="store_true",
        help="Only keep leads with non-homepage contact/subscribe paths.",
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
        help="Require stronger contact path hints when using --require-contact-path.",
    )
    parser.add_argument(
        "--listing-strict",
        action="store_true",
        help="Require format + price + purchase control on listing pages.",
    )
    parser.add_argument(
        "--skip-email-verify",
        action="store_true",
        help="Skip verify_emails.py filtering before merge (not recommended).",
    )
    parser.add_argument(
        "--email-gate",
        choices=("strict", "balanced"),
        default="balanced",
        help="Email verification merge gate when --require-email is enabled.",
    )
    parser.add_argument(
        "--require-location",
        action="store_true",
        help="Exclude rows where Location is empty or 'Unknown' when accumulating.",
    )
    parser.add_argument(
        "--merge-policy",
        choices=("strict", "balanced", "open"),
        default="strict",
        help="Master merge policy: strict=only proof-strong rows, balanced=allow title-strong rows, open=merge all.",
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
        default=str(config_arg_default(runtime_config, "validation_profile", "default") or "default"),
        help="Validation profile: default keeps staged rows; fully_verified only keeps outbound-ready rows; agent_hunt writes scout-qualified rows with a separate 3-column export; astra_outbound applies the Astra strict outbound preset; verified_no_us keeps the strict outbound gates but does not require U.S. location; strict_interactive and strict_full keep fully_verified acceptance rules with smaller or larger runtime budgets.",
    )
    parser.add_argument(
        "--dedupe-db",
        default=state_output("dedupe.sqlite"),
        help="SQLite path for persistent cross-run dedupe fingerprints.",
    )
    parser.add_argument(
        "--disable-persistent-dedupe",
        action="store_true",
        help="Disable persistent cross-run dedupe checks.",
    )
    args = parser.parse_args(argv_list)
    args._runtime_config = runtime_config
    if bool(config_arg_default(runtime_config, "listing_strict", False)) and "--listing-strict" not in argv_list:
        args.listing_strict = True
    if "--merge-policy" not in argv_list:
        args.merge_policy = str(config_arg_default(runtime_config, "merge_policy", args.merge_policy) or args.merge_policy)
    return args


def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows([{col: row.get(col, "") for col in OUTPUT_COLUMNS} for row in rows])


def read_candidate_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_candidate_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANDIDATE_COLUMNS)
        writer.writeheader()
        writer.writerows([{col: row.get(col, "") for col in CANDIDATE_COLUMNS} for row in rows])


def build_projected_lead_export_rows(
    rows: List[Dict[str, str]],
    *,
    source_url_getter: Callable[[Dict[str, str]], str],
) -> List[Dict[str, str]]:
    unique = set()
    projected: List[Dict[str, str]] = []
    for row in rows:
        author = (row.get("AuthorName", "") or "").strip()
        email = (row.get("AuthorEmail", "") or "").strip().lower()
        source_url = source_url_getter(row).strip()
        if not (author and email and source_url):
            continue
        key = (normalize_person_name(author), email)
        if not all(key):
            continue
        if key in unique:
            continue
        unique.add(key)
        projected.append(
            {
                "AuthorName": author,
                "AuthorEmail": email,
                "SourceURL": source_url,
            }
        )
    return projected


def build_new_lead_export_rows(
    current_rows: List[Dict[str, str]],
    existing_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    existing_keys = {
        (
            normalize_person_name((row.get("AuthorName", "") or "").strip()),
            (row.get("AuthorEmail", "") or "").strip().lower(),
        )
        for row in existing_rows
        if (row.get("AuthorName", "") or "").strip() and (row.get("AuthorEmail", "") or "").strip()
    }
    return [
        row
        for row in current_rows
        if (
            normalize_person_name((row.get("AuthorName", "") or "").strip()),
            (row.get("AuthorEmail", "") or "").strip().lower(),
        )
        not in existing_keys
    ]


def write_projected_lead_export_rows(path: Path, rows: List[Dict[str, str]], *, with_header: bool = False) -> int:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        if with_header:
            writer.writerow(MINIMAL_COLUMNS)
        for row in rows:
            writer.writerow(
                [
                    (row.get("AuthorName", "") or "").strip(),
                    (row.get("AuthorEmail", "") or "").strip().lower(),
                    (row.get("SourceURL", "") or "").strip(),
                ]
            )
    return len(rows)


def write_minimal_rows(path: Path, rows: List[Dict[str, str]], with_header: bool) -> int:
    projected_rows = build_projected_lead_export_rows(rows, source_url_getter=best_verified_source_url)
    return write_projected_lead_export_rows(path, projected_rows, with_header=with_header)


def best_verified_source_url(row: Dict[str, str]) -> str:
    for field in ("AuthorEmailSourceURL", "ContactPageURL", "AuthorWebsite", "SourceURL"):
        value = (row.get(field, "") or "").strip()
        if value:
            return value
    return ""


def best_scout_source_url(row: Dict[str, str]) -> str:
    for field in ("AuthorEmailSourceURL", "SourceURL", "ContactPageURL", "AuthorWebsite"):
        value = (row.get(field, "") or "").strip()
        if value:
            return value
    return ""


def write_verified_rows(path: Path, rows: List[Dict[str, str]]) -> int:
    projected_rows = build_projected_lead_export_rows(rows, source_url_getter=best_verified_source_url)
    return write_projected_lead_export_rows(path, projected_rows)


def write_scouted_rows(path: Path, rows: List[Dict[str, str]]) -> int:
    projected_rows = build_projected_lead_export_rows(rows, source_url_getter=best_scout_source_url)
    return write_projected_lead_export_rows(path, projected_rows)


def write_contact_queue_rows(path: Path, rows: List[Dict[str, str]], include_email_rows: bool = False) -> int:
    unique = set()
    out_rows: List[Dict[str, str]] = []
    for row in rows:
        email = (row.get("AuthorEmail", "") or "").strip()
        if email and not include_email_rows:
            continue
        contact_url = (row.get("ContactURL", "") or "").strip()
        contact = (row.get("ContactPageURL", "") or "").strip()
        subscribe = (row.get("SubscribeURL", "") or "").strip()
        website = (row.get("AuthorWebsite", "") or "").strip()
        if not (contact_url or contact or subscribe or website):
            continue
        author = (row.get("AuthorName", "") or "").strip()
        book = (row.get("BookTitle", "") or "").strip()
        key = (author.lower(), book.lower(), (contact_url or contact or subscribe or website).lower())
        if key in unique:
            continue
        unique.add(key)
        out_rows.append({col: (row.get(col, "") or "").strip() for col in CONTACT_QUEUE_COLUMNS})

    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CONTACT_QUEUE_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)
    return len(out_rows)


def write_near_miss_location_rows(path: Path, rows: List[Dict[str, str]]) -> int:
    unique = set()
    out_rows: List[Dict[str, str]] = []
    for row in rows:
        author = (row.get("AuthorName", "") or "").strip()
        email = (row.get("AuthorEmail", "") or "").strip().lower()
        source_url = (row.get("SourceURL", "") or "").strip().lower()
        reject_reason = (row.get("RejectReason", "") or "").strip()
        key = (author.lower(), email, source_url, reject_reason)
        if key in unique:
            continue
        unique.add(key)
        out_rows.append({col: (row.get(col, "") or "").strip() for col in NEAR_MISS_LOCATION_COLUMNS})

    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=NEAR_MISS_LOCATION_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)
    return len(out_rows)


def row_is_contactable(row: Dict[str, str]) -> bool:
    return any((row.get(field, "") or "").strip() for field in ("ContactURL", "ContactPageURL", "SubscribeURL", "AuthorWebsite"))


def row_has_clean_author_name(row: Dict[str, str]) -> bool:
    author = (row.get("AuthorName", "") or "").strip()
    lowered = author.lower()
    return bool(author) and "@" not in author and "get in touch" not in lowered


def looks_like_generic_scout_author_name(value: str) -> bool:
    author = re.sub(r"\s+", " ", (value or "").strip(" -|")).strip()
    if not author:
        return True
    lowered = author.lower()
    if any(snippet in lowered for snippet in SCOUT_GENERIC_AUTHOR_SNIPPETS):
        return True
    tokens = normalize_person_name(author).split()
    if len(tokens) < 2:
        return True
    non_generic_tokens = [token for token in tokens if token not in SCOUT_GENERIC_AUTHOR_TOKENS]
    return len(non_generic_tokens) < 2


def row_has_scout_email(row: Dict[str, str]) -> bool:
    email = (row.get("AuthorEmail", "") or "").strip()
    email_quality = (row.get("EmailQuality", "") or "").strip()
    return bool(email and best_scout_source_url(row) and email_quality in SCOUT_ACCEPTABLE_EMAIL_QUALITIES)


def row_has_definitive_non_us_location(row: Dict[str, str]) -> bool:
    location = (row.get("Location", "") or "").strip()
    if not location or location.lower() in {"unknown", "n/a", "na"}:
        return False
    return not is_us_location(location)


def normalized_scout_author_identity(row: Dict[str, str]) -> str:
    return normalize_person_name((row.get("AuthorName", "") or "").strip())


def best_scout_candidate_domain(row: Dict[str, str]) -> str:
    for field in ("AuthorWebsite", "ContactURL", "ContactPageURL", "SubscribeURL", "SourceURL"):
        domain = registrable_domain(row.get(field, ""))
        if domain:
            return domain
    return ""


def infer_source_type_from_source_url(source_url: str) -> str:
    domain = registrable_domain(source_url)
    if not domain:
        return "unknown"
    return SCOUT_SOURCE_TYPE_BY_DOMAIN.get(domain, domain)


def infer_scout_email_quality(email: str, *, candidate_url: str = "", source_url: str = "") -> str:
    normalized = (email or "").strip().lower()
    if "@" not in normalized:
        return ""
    local_part, domain_part = normalized.split("@", 1)
    if local_part in ROLE_EMAIL_LOCALS:
        return "risky_role"
    email_domain = registrable_domain(f"https://{domain_part}")
    candidate_domain = registrable_domain(candidate_url) or registrable_domain(source_url)
    if email_domain and candidate_domain and email_domain == candidate_domain:
        return "same_domain"
    return "labeled_off_domain"


def agent_hunt_email_local_tokens(email: str) -> List[str]:
    normalized = (email or "").strip().lower()
    if "@" not in normalized:
        return []
    local_part = normalized.split("@", 1)[0]
    return [token for token in re.split(r"[^a-z0-9]+", local_part) if token]


def classify_agent_hunt_author_email_match(author_name: str, email: str) -> str:
    author_tokens = normalize_person_name(author_name).split()
    local_tokens = agent_hunt_email_local_tokens(email)
    if not author_tokens or not local_tokens:
        return "none"
    if all(token in ROLE_EMAIL_LOCALS for token in local_tokens):
        return "role"
    author_compact = "".join(author_tokens)
    local_compact = "".join(local_tokens)
    if author_compact and len(author_compact) >= 5 and (
        author_compact in local_compact or (len(local_compact) >= 5 and local_compact in author_compact)
    ):
        return "clear"
    given = author_tokens[0]
    surname = author_tokens[-1] if len(author_tokens) >= 2 else ""
    initials = "".join(token[0] for token in author_tokens if token)
    if surname and surname in local_compact:
        return "partial"
    if given and len(given) >= 3 and given in local_compact:
        return "partial"
    if initials and len(initials) >= 2 and initials in local_compact:
        return "partial"
    return "none"


def row_has_author_aligned_email_domain(row: Dict[str, str]) -> bool:
    email = (row.get("AuthorEmail", "") or "").strip().lower()
    if "@" not in email:
        return False
    email_domain = registrable_domain(f"https://{email.split('@', 1)[1]}")
    if not email_domain:
        return False
    for field in ("AuthorWebsite", "ContactPageURL", "SubscribeURL", "SourceURL", "AuthorEmailSourceURL"):
        if registrable_domain(row.get(field, "")) == email_domain:
            return True
    return False


def row_has_rep_or_agency_only_email(row: Dict[str, str]) -> bool:
    email = (row.get("AuthorEmail", "") or "").strip().lower()
    if "@" not in email:
        return False
    local_tokens = set(agent_hunt_email_local_tokens(email))
    domain_part = email.split("@", 1)[1]
    if not local_tokens:
        return False
    has_rep_signal = bool(local_tokens & AGENT_HUNT_REP_EMAIL_TOKENS) or any(
        hint in domain_part for hint in AGENT_HUNT_REP_EMAIL_DOMAIN_HINTS
    )
    if not has_rep_signal:
        return False
    if row_has_author_aligned_email_domain(row):
        return False
    match_strength = classify_agent_hunt_author_email_match((row.get("AuthorName", "") or "").strip(), email)
    return match_strength in {"none", "role"}


def classify_agent_hunt_outreach_tier(row: Dict[str, str]) -> Dict[str, object]:
    email = (row.get("AuthorEmail", "") or "").strip().lower()
    quality = (row.get("EmailQuality", "") or "").strip()
    if not email:
        return {
            "tier": "",
            "decision": "RECHECK",
            "reason": "missing_public_email",
            "email_match": "none",
            "same_domain_support": False,
        }
    if quality not in SCOUT_ACCEPTABLE_EMAIL_QUALITIES:
        return {
            "tier": "",
            "decision": "REPLACE",
            "reason": "unusable_public_email",
            "email_match": "none",
            "same_domain_support": False,
        }
    if row_has_rep_or_agency_only_email(row):
        return {
            "tier": "",
            "decision": "REPLACE",
            "reason": "rep_or_agency_only_email",
            "email_match": classify_agent_hunt_author_email_match((row.get("AuthorName", "") or "").strip(), email),
            "same_domain_support": row_has_author_aligned_email_domain(row),
        }

    email_match = classify_agent_hunt_author_email_match((row.get("AuthorName", "") or "").strip(), email)
    same_domain_support = row_has_author_aligned_email_domain(row)
    contactable = row_is_contactable(row)

    if quality == "same_domain":
        if email_match in {"clear", "partial"} and contactable:
            return {
                "tier": AGENT_HUNT_TIER_1,
                "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_1],
                "reason": "tier_1_same_domain_clear_match",
                "email_match": email_match,
                "same_domain_support": same_domain_support,
            }
        if email_match in {"clear", "partial"} or contactable or same_domain_support:
            return {
                "tier": AGENT_HUNT_TIER_2,
                "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_2],
                "reason": "tier_2_same_domain_public_email",
                "email_match": email_match,
                "same_domain_support": same_domain_support,
            }
        return {
            "tier": AGENT_HUNT_TIER_3,
            "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_3],
            "reason": "tier_3_same_domain_weak_match",
            "email_match": email_match,
            "same_domain_support": same_domain_support,
        }
    if quality == "labeled_off_domain":
        if email_match in {"clear", "partial"}:
            return {
                "tier": AGENT_HUNT_TIER_2,
                "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_2],
                "reason": "tier_2_off_domain_named_email",
                "email_match": email_match,
                "same_domain_support": same_domain_support,
            }
        if contactable or same_domain_support:
            return {
                "tier": AGENT_HUNT_TIER_3,
                "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_3],
                "reason": "tier_3_off_domain_supported_email",
                "email_match": email_match,
                "same_domain_support": same_domain_support,
            }
        return {
            "tier": "",
            "decision": "REPLACE",
            "reason": "weak_public_email_match",
            "email_match": email_match,
            "same_domain_support": same_domain_support,
        }
    if quality == "risky_role" and (contactable or same_domain_support):
        return {
            "tier": AGENT_HUNT_TIER_3,
            "decision": AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_3],
            "reason": "tier_3_public_role_email",
            "email_match": email_match,
            "same_domain_support": same_domain_support,
        }
    return {
        "tier": "",
        "decision": "REPLACE",
        "reason": "weak_public_email_match",
        "email_match": email_match,
        "same_domain_support": same_domain_support,
    }


def with_agent_hunt_outreach_metadata(
    row: Dict[str, str],
    assessment: Dict[str, object],
) -> Dict[str, str]:
    updated = dict(row)
    outreach_tier = str(assessment.get("outreach_tier", "") or "").strip()
    decision = str(assessment.get("decision", "") or "").strip()
    tier_reason = str(assessment.get("tier_reason", "") or "").strip()
    email_match = str(assessment.get("email_match", "") or "").strip()
    if outreach_tier:
        updated["AgentHuntOutreachTier"] = outreach_tier
    if decision:
        updated["AgentHuntDecision"] = decision
    if tier_reason:
        updated["AgentHuntTierReason"] = tier_reason
    if email_match:
        updated["AgentHuntEmailMatch"] = email_match
    return updated


def order_agent_hunt_rows_by_priority(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def sort_key(row: Dict[str, str]) -> Tuple[int, str, str]:
        tier = str(row.get("AgentHuntOutreachTier", "") or assess_agent_hunt_row(row).get("outreach_tier", "") or "").strip()
        return (
            AGENT_HUNT_TIER_PRIORITY.get(tier, 99),
            (row.get("AuthorName", "") or "").strip().lower(),
            (row.get("AuthorEmail", "") or "").strip().lower(),
        )

    return sorted(rows, key=sort_key)


def partition_agent_hunt_rows_by_outreach_tier(rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    partitions = {
        AGENT_HUNT_TIER_1: [],
        AGENT_HUNT_TIER_2: [],
        AGENT_HUNT_TIER_3: [],
    }
    for row in order_agent_hunt_rows_by_priority(rows):
        tier = str(row.get("AgentHuntOutreachTier", "") or assess_agent_hunt_row(row).get("outreach_tier", "") or "").strip()
        if tier in partitions:
            partitions[tier].append(row)
    return partitions


def scout_export_row_key(row: Dict[str, str]) -> Tuple[str, str]:
    return (
        normalize_person_name((row.get("AuthorName", "") or "").strip()),
        (row.get("AuthorEmail", "") or "").strip().lower(),
    )


def dedupe_scout_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    unique: set[Tuple[str, str]] = set()
    deduped: List[Dict[str, str]] = []
    for row in order_agent_hunt_rows_by_priority(rows):
        key = scout_export_row_key(row)
        if not all(key):
            continue
        if key in unique:
            continue
        unique.add(key)
        deduped.append(row)
    return deduped


def classify_agent_hunt_listing_failure(
    reject_reason: str,
    *,
    record: Dict[str, object] | None = None,
) -> str:
    normalized = (reject_reason or "").strip().lower()
    if not normalized.startswith(SCOUT_LISTING_FAILURE_PREFIX):
        return ""
    if normalized in SCOUT_LISTING_HARD_DEAD_END_REASONS:
        return "listing_absent"
    if normalized in SCOUT_LISTING_RETAILER_FRICTION_REASONS or normalized.startswith("listing_amazon_interstitial_"):
        if record is not None and (
            bool(record.get("listing_recovery_attempted", False))
            or str(record.get("next_action", "") or "").strip() == "try_listing_proof"
            or str(record.get("book_url", "") or "").strip()
            or str(record.get("listing_snippet", "") or "").strip()
        ):
            return "listing_retailer_friction_recoverable"
        return "listing_retailer_friction_blocked"
    return "listing_other_failure"


def build_scout_row_from_candidate_outcome(record: Dict[str, object]) -> Dict[str, str]:
    candidate_url = str(record.get("candidate_url", "") or "").strip()
    source_url = str(record.get("source_url", "") or "").strip()
    email = str(record.get("agent_hunt_email_recovered_email", "") or record.get("email", "") or "").strip().lower()
    email_source_url = str(
        record.get("agent_hunt_email_recovery_source_url", "") or record.get("email_source_url", "") or candidate_url or source_url
    ).strip()
    contact_page_url = str(record.get("agent_hunt_email_recovery_source_url", "") or candidate_url).strip()
    email_quality = str(record.get("agent_hunt_email_recovered_email_quality", "") or "").strip()
    return {
        "AuthorName": str(record.get("author_name", "") or "").strip(),
        "AuthorEmail": email,
        "AuthorEmailSourceURL": email_source_url,
        "AuthorEmailProofSnippet": str(record.get("agent_hunt_email_recovery_proof_snippet", "") or record.get("email_snippet", "") or "").strip(),
        "EmailQuality": email_quality or infer_scout_email_quality(email, candidate_url=candidate_url, source_url=source_url),
        "AuthorWebsite": candidate_url,
        "ContactPageURL": contact_page_url,
        "SourceURL": source_url or candidate_url,
        "SourceTitle": "",
        "SourceSnippet": "",
        "ListingURL": str(record.get("book_url", "") or "").strip(),
        "ListingStatus": "unverified",
        "ListingFailReason": str(record.get("reject_reason", "") or "").strip(),
    }


def build_agent_hunt_email_recovery_fetcher() -> Tuple[Callable[[str], Dict[str, object]], object]:
    session = build_session()
    robots_cache: Dict[str, Tuple[object | None, float]] = {}
    robots_text_cache: Dict[str, str] = {}

    def fetch_page(url: str) -> Dict[str, object]:
        result = fetch_with_meta(
            session=session,
            url=url,
            timeout=SCOUT_EMAIL_RECOVERY_TIMEOUT_SECONDS,
            robots_cache=robots_cache,
            robots_text_cache=robots_text_cache,
            ignore_robots=False,
            retry_seconds=SCOUT_EMAIL_RECOVERY_RETRY_SECONDS,
            expect_html=True,
        )
        return {
            "ok": bool(result.ok and result.soup),
            "url": result.final_url or result.url,
            "text": result.text,
            "soup": result.soup,
            "failure_reason": result.failure_reason,
        }

    return fetch_page, session


def build_agent_hunt_email_recovery_urls(
    candidate_url: str,
    *,
    landing_page: Dict[str, object] | None = None,
) -> List[str]:
    normalized_candidate = normalize_url(candidate_url)
    if not normalized_candidate:
        return []
    candidate_domain = registrable_domain(normalized_candidate)
    parsed = urlparse(normalized_candidate)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    candidates: List[str] = []

    def add(url: str) -> None:
        normalized = normalize_url(url)
        if not normalized:
            return
        if candidate_domain and registrable_domain(normalized) != candidate_domain:
            return
        candidates.append(normalized)

    add(normalized_candidate)

    landing_url = normalize_url(str((landing_page or {}).get("url", "") or "")) or normalized_candidate
    landing_soup = (landing_page or {}).get("soup")
    if landing_url:
        add(landing_url)
    if landing_url and landing_soup is not None:
        links = dedupe_urls(extract_links(landing_url, landing_soup))
        add(find_contact_link_from_links(origin or landing_url, links))
        add(find_subscribe_link_from_links(origin or landing_url, links))
        for url in select_supporting_pages(landing_url, links, limit=SCOUT_EMAIL_RECOVERY_SUPPORT_PAGE_LIMIT):
            add(url)

    base_for_hints = origin or landing_url or normalized_candidate
    for hint in SCOUT_EMAIL_RECOVERY_PATH_HINTS:
        add(urljoin(base_for_hints.rstrip("/") + "/", hint.lstrip("/")))

    return dedupe_urls(candidates)[:SCOUT_EMAIL_RECOVERY_MAX_PAGES]


def recover_agent_hunt_listing_friction_email_record(
    record: Dict[str, object],
    *,
    fetch_page: Callable[[str], Dict[str, object]] | None = None,
) -> Dict[str, object]:
    updated = dict(record)
    listing_assessment = assess_agent_hunt_listing_blocked_record(updated)
    if (
        str(listing_assessment.get("status", "") or "") != "scoutworthy_not_outreach_ready"
        or str(listing_assessment.get("base_scout_reason", "") or "") != "scoutworthy_missing_visible_email"
    ):
        updated["agent_hunt_email_recovery_attempted"] = False
        updated["agent_hunt_email_recovery_skipped_reason"] = "not_listing_friction_caution"
        return updated

    row = dict(listing_assessment.get("row", {}) or {})
    candidate_url = normalize_url(str(updated.get("candidate_url", "") or row.get("AuthorWebsite", "") or ""))
    if not candidate_url:
        updated["agent_hunt_email_recovery_attempted"] = False
        updated["agent_hunt_email_recovery_skipped_reason"] = "missing_candidate_url"
        return updated

    local_fetch_page = fetch_page
    session = None
    if local_fetch_page is None:
        local_fetch_page, session = build_agent_hunt_email_recovery_fetcher()

    try:
        landing_page = local_fetch_page(candidate_url)
        recovery_urls = build_agent_hunt_email_recovery_urls(candidate_url, landing_page=landing_page)
        updated["agent_hunt_email_recovery_attempted"] = True
        updated["agent_hunt_email_recovery_skipped_reason"] = ""
        updated["agent_hunt_email_recovery_attempted_url_count"] = len(recovery_urls)

        if not recovery_urls:
            updated["agent_hunt_email_recovery_success"] = False
            updated["agent_hunt_email_recovery_fail_reason"] = "no_recovery_urls"
            return updated

        attempted_results: Dict[str, Dict[str, object]] = {}
        if normalize_url(str(landing_page.get("url", "") or "")) in recovery_urls:
            attempted_results[normalize_url(str(landing_page.get("url", "") or ""))] = landing_page
        elif candidate_url in recovery_urls:
            attempted_results[candidate_url] = landing_page

        email_records: List[Dict[str, str]] = []
        fetch_failures: Counter[str] = Counter()
        visible_but_unusable = False
        successful_fetches = 0

        for recovery_url in recovery_urls:
            result = attempted_results.get(recovery_url)
            if result is None:
                result = local_fetch_page(recovery_url)
                attempted_results[recovery_url] = result
            if not bool(result.get("ok", False)) or result.get("soup") is None:
                failure_reason = str(result.get("failure_reason", "") or "fetch_failed").strip() or "fetch_failed"
                fetch_failures[failure_reason] += 1
                continue
            successful_fetches += 1
            source_url = normalize_url(str(result.get("url", "") or recovery_url)) or recovery_url
            page_records = extract_page_email_records(
                source_url,
                str(result.get("text", "") or ""),
                result.get("soup"),
                candidate_url,
            )
            if any(str(page_record.get("visible_text", "") or "").strip().lower() != "true" for page_record in page_records):
                visible_but_unusable = True
            email_records.extend(page_records)

        best_record = choose_author_email_record(
            email_records,
            candidate_url,
            author_name=str(updated.get("author_name", "") or ""),
            require_visible_text=True,
        )
        if best_record:
            updated["agent_hunt_email_recovery_success"] = True
            updated["agent_hunt_email_recovery_fail_reason"] = ""
            updated["agent_hunt_email_recovery_source_url"] = str(best_record.get("source_url", "") or "").strip()
            updated["agent_hunt_email_recovered_email"] = str(best_record.get("email", "") or "").strip().lower()
            updated["agent_hunt_email_recovered_email_quality"] = str(best_record.get("quality", "") or "").strip()
            updated["agent_hunt_email_recovery_proof_snippet"] = str(best_record.get("proof_snippet", "") or "").strip()
            updated["agent_hunt_email_recovery_method"] = str(best_record.get("method", "") or "").strip()
            updated["email"] = updated["agent_hunt_email_recovered_email"]
            updated["email_snippet"] = updated["agent_hunt_email_recovery_proof_snippet"]
            return updated

        updated["agent_hunt_email_recovery_success"] = False
        if visible_but_unusable:
            updated["agent_hunt_email_recovery_fail_reason"] = "no_visible_text_email"
        elif successful_fetches:
            updated["agent_hunt_email_recovery_fail_reason"] = "no_visible_email_found"
        elif fetch_failures:
            fail_reason, _ = fetch_failures.most_common(1)[0]
            updated["agent_hunt_email_recovery_fail_reason"] = f"page_fetch_failed:{fail_reason}"
        else:
            updated["agent_hunt_email_recovery_fail_reason"] = "no_visible_email_found"
        return updated
    finally:
        if session is not None:
            session.close()


def recover_agent_hunt_listing_friction_emails(
    candidate_outcome_records: List[Dict[str, object]],
    *,
    fetch_page: Callable[[str], Dict[str, object]] | None = None,
) -> List[Dict[str, object]]:
    if not candidate_outcome_records:
        return []
    local_fetch_page = fetch_page
    session = None
    if local_fetch_page is None:
        local_fetch_page, session = build_agent_hunt_email_recovery_fetcher()
    try:
        return [
            recover_agent_hunt_listing_friction_email_record(record, fetch_page=local_fetch_page)
            for record in candidate_outcome_records
        ]
    finally:
        if session is not None:
            session.close()


def assess_agent_hunt_listing_blocked_record(record: Dict[str, object]) -> Dict[str, object]:
    listing_failure_reason = str(record.get("reject_reason", "") or "").strip()
    listing_failure_category = classify_agent_hunt_listing_failure(listing_failure_reason, record=record)
    row = build_scout_row_from_candidate_outcome(record)
    base_assessment = assess_agent_hunt_row(row)
    if listing_failure_category not in {"listing_retailer_friction_blocked", "listing_retailer_friction_recoverable"}:
        return {
            "qualified": False,
            "status": "rejected",
            "reason": listing_failure_reason or str(base_assessment.get("reason", "") or ""),
            "listing_failure_reason": listing_failure_reason,
            "listing_failure_category": listing_failure_category,
            "promotion_blocker_reason": listing_failure_reason or str(base_assessment.get("reason", "") or ""),
            "row": row,
            "decision": str(base_assessment.get("decision", "") or "REPLACE"),
            "outreach_tier": str(base_assessment.get("outreach_tier", "") or ""),
            "tier_reason": str(base_assessment.get("tier_reason", "") or ""),
            "email_match": str(base_assessment.get("email_match", "") or ""),
        }
    if not bool(base_assessment.get("qualified", False)):
        if str(base_assessment.get("status", "") or "") == "scoutworthy_not_outreach_ready":
            caution_reason = f"listing_blocked:{listing_failure_reason}"
            promotion_blocker_reason = caution_reason
            if bool(record.get("agent_hunt_email_recovery_attempted", False)) and not bool(
                record.get("agent_hunt_email_recovery_success", False)
            ):
                fail_reason = str(record.get("agent_hunt_email_recovery_fail_reason", "") or "").strip()
                if fail_reason:
                    promotion_blocker_reason = f"listing_friction_email_recovery_failed:{fail_reason}"
            return {
                "qualified": False,
                "status": "scoutworthy_not_outreach_ready",
                "reason": caution_reason,
                "listing_failure_reason": listing_failure_reason,
                "listing_failure_category": listing_failure_category,
                "promotion_blocker_reason": promotion_blocker_reason,
                "base_scout_reason": str(base_assessment.get("reason", "") or ""),
                "row": row,
                "decision": str(base_assessment.get("decision", "") or "RECHECK"),
                "outreach_tier": str(base_assessment.get("outreach_tier", "") or ""),
                "tier_reason": str(base_assessment.get("tier_reason", "") or ""),
                "email_match": str(base_assessment.get("email_match", "") or ""),
            }
        return {
            "qualified": False,
            "status": str(base_assessment.get("status", "rejected") or "rejected"),
            "reason": str(base_assessment.get("reason", "") or ""),
            "listing_failure_reason": listing_failure_reason,
            "listing_failure_category": listing_failure_category,
            "promotion_blocker_reason": str(base_assessment.get("reason", "") or ""),
            "row": row,
            "decision": str(base_assessment.get("decision", "") or "REPLACE"),
            "outreach_tier": str(base_assessment.get("outreach_tier", "") or ""),
            "tier_reason": str(base_assessment.get("tier_reason", "") or ""),
            "email_match": str(base_assessment.get("email_match", "") or ""),
        }
    return {
        "qualified": True,
        "status": "qualified_listing_blocked",
        "reason": "",
        "listing_failure_reason": listing_failure_reason,
        "listing_failure_category": listing_failure_category,
        "promotion_blocker_reason": "",
        "row": with_agent_hunt_outreach_metadata(row, base_assessment),
        "decision": str(base_assessment.get("decision", "") or AGENT_HUNT_TIER_TO_DECISION[AGENT_HUNT_TIER_2]),
        "outreach_tier": str(base_assessment.get("outreach_tier", "") or ""),
        "tier_reason": str(base_assessment.get("tier_reason", "") or ""),
        "email_match": str(base_assessment.get("email_match", "") or ""),
    }


def generic_scout_author_name_reason(value: str) -> str:
    author = re.sub(r"\s+", " ", (value or "").strip(" -|")).strip()
    if not author:
        return "bad_author_name_empty"
    tokens = normalize_person_name(author).split()
    if len(tokens) < 2:
        return "bad_author_name_single_token"
    lowered = author.lower()
    if any(snippet in lowered for snippet in SCOUT_GENERIC_AUTHOR_SNIPPETS):
        return "bad_author_name_generic_phrase"
    return "bad_author_name_generic_tokens"


def assess_agent_hunt_row(row: Dict[str, str]) -> Dict[str, str | bool]:
    author_name = (row.get("AuthorName", "") or "").strip()
    if not row_has_clean_author_name(row):
        return {"qualified": False, "status": "rejected", "reason": "bad_author_name_unclean", "decision": "REPLACE"}
    if looks_like_generic_scout_author_name(author_name):
        return {
            "qualified": False,
            "status": "rejected",
            "reason": generic_scout_author_name_reason(author_name),
            "decision": "REPLACE",
        }
    if not is_plausible_author_name(author_name):
        return {"qualified": False, "status": "rejected", "reason": "bad_author_name_implausible", "decision": "REPLACE"}
    if row_has_definitive_non_us_location(row):
        return {"qualified": False, "status": "rejected", "reason": "non_us_location", "decision": "REPLACE"}
    if not (row.get("SourceURL", "") or "").strip():
        return {"qualified": False, "status": "rejected", "reason": "missing_source_url", "decision": "REPLACE"}
    if not row_has_scout_email(row):
        if not row_is_contactable(row):
            return {
                "qualified": False,
                "status": "rejected",
                "reason": "missing_public_email_and_contact_path",
                "decision": "REPLACE",
            }
        return {
            "qualified": False,
            "status": "scoutworthy_not_outreach_ready",
            "reason": "scoutworthy_missing_visible_email",
            "decision": "RECHECK",
        }
    outreach = classify_agent_hunt_outreach_tier(row)
    if not str(outreach.get("tier", "") or "").strip():
        return {
            "qualified": False,
            "status": "rejected",
            "reason": str(outreach.get("reason", "") or "weak_public_email_match"),
            "decision": str(outreach.get("decision", "") or "REPLACE"),
            "email_match": str(outreach.get("email_match", "") or ""),
        }
    return {
        "qualified": True,
        "status": "qualified",
        "reason": "",
        "decision": str(outreach.get("decision", "") or "RECHECK"),
        "outreach_tier": str(outreach.get("tier", "") or ""),
        "tier_reason": str(outreach.get("reason", "") or ""),
        "email_match": str(outreach.get("email_match", "") or ""),
    }


def scout_reject_reason(row: Dict[str, str]) -> str:
    return str(assess_agent_hunt_row(row).get("reason", "") or "")


def row_is_agent_hunt_qualified(row: Dict[str, str]) -> bool:
    return bool(assess_agent_hunt_row(row).get("qualified", False))


def row_is_fully_verified(row: Dict[str, str], require_us_location: bool = True) -> bool:
    if not row_is_contactable(row):
        return False
    if not row_has_clean_author_name(row):
        return False
    if not (row.get("AuthorEmail", "") or "").strip():
        return False
    if (row.get("EmailQuality", "") or "").strip() not in {"same_domain", "labeled_off_domain"}:
        return False
    if not (row.get("AuthorEmailSourceURL", "") or "").strip():
        return False
    if require_us_location:
        location = (row.get("Location", "") or "").strip()
        if not location or location.lower() in {"unknown", "n/a", "na"} or not is_us_location(location):
            return False
    if (row.get("IndieProofStrength", "") or "").strip().lower() not in {"onsite", "both"}:
        return False
    if (row.get("ListingStatus", "") or "").strip().lower() != "verified":
        return False
    if (row.get("RecencyStatus", "") or "").strip().lower() != "verified":
        return False
    return bool(best_verified_source_url(row))


def count_goal_rows(rows: List[Dict[str, str]], validation_profile: str, policy: str) -> int:
    if is_agent_hunt_profile(validation_profile):
        return sum(1 for row in rows if row_is_agent_hunt_qualified(row))
    if is_fully_verified_profile(validation_profile):
        require_us_location = profile_requires_us_location(validation_profile)
        return sum(1 for row in rows if row_is_fully_verified(row, require_us_location=require_us_location))
    return len(rows)


def row_qualifies_for_master(row: Dict[str, str], policy: str, validation_profile: str = "default") -> bool:
    if is_agent_hunt_profile(validation_profile):
        return row_is_agent_hunt_qualified(row)
    if is_fully_verified_profile(validation_profile):
        return row_is_fully_verified(row, require_us_location=profile_requires_us_location(validation_profile))
    if policy == "open":
        return True
    if not row_is_contactable(row):
        return False
    if not row_has_clean_author_name(row):
        return False

    recency_ok = (row.get("RecencyStatus", "verified") or "verified").strip().lower() == "verified"
    email_ready = row_has_scout_email(row)

    if policy == "balanced":
        return email_ready
    return email_ready and recency_ok


def split_rows_by_merge_policy(
    rows: List[Dict[str, str]],
    policy: str,
    validation_profile: str = "default",
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    master_rows: List[Dict[str, str]] = []
    queue_only_rows: List[Dict[str, str]] = []
    for row in rows:
        if row_qualifies_for_master(row, policy=policy, validation_profile=validation_profile):
            master_rows.append(row)
            continue
        if row_is_contactable(row):
            queue_only_rows.append(row)
    return master_rows, queue_only_rows


def build_contact_queue_export_rows(
    master_rows: List[Dict[str, str]],
    queue_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    combined = list(queue_rows)
    combined.extend(row for row in master_rows if not (row.get("AuthorEmail", "") or "").strip())
    return dedupe(combined)


def summarize_row_domains(rows: List[Dict[str, str]], *, source_selector: Callable[[Dict[str, str]], str], limit: int = 10) -> List[Dict[str, object]]:
    counts: Counter[str] = Counter()
    for row in rows:
        domain = registrable_domain(source_selector(row))
        if domain:
            counts[domain] += 1
    return [{"domain": domain, "count": count} for domain, count in counts.most_common(limit)]


def summarize_counter(counter: Counter[str], *, key_name: str, limit: int = 10) -> List[Dict[str, object]]:
    return [{key_name: key, "count": count} for key, count in counter.most_common(limit)]


def build_candidate_outcome_lookup(candidate_outcome_records: List[Dict[str, object]]) -> Dict[Tuple[str, str], Dict[str, str]]:
    lookup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for record in candidate_outcome_records:
        source_url = str(record.get("source_url", "") or "").strip()
        candidate_domain = registrable_domain(str(record.get("candidate_domain", "") or "").strip())
        if not candidate_domain:
            candidate_domain = registrable_domain(str(record.get("candidate_url", "") or "").strip())
        if not source_url or not candidate_domain:
            continue
        lookup.setdefault(
            (source_url, candidate_domain),
            {
                "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
                "source_type": str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(source_url),
                "source_url": source_url,
            },
        )
    return lookup


def build_route_source_context(
    *,
    row: Dict[str, str],
    outcome_lookup: Dict[Tuple[str, str], Dict[str, str]],
) -> Dict[str, str]:
    source_url = (row.get("SourceURL", "") or "").strip()
    candidate_domain = best_scout_candidate_domain(row)
    context = outcome_lookup.get((source_url, candidate_domain), {}) if source_url and candidate_domain else {}
    source_type = str(context.get("source_type", "") or infer_source_type_from_source_url(source_url))
    source_query = str(context.get("source_query", "") or "<unknown>")
    return {
        "source_url": source_url,
        "source_domain": registrable_domain(source_url),
        "source_type": source_type or "unknown",
        "source_query": source_query or "<unknown>",
    }


def summarize_route_source_contexts(
    route_records: List[Dict[str, str]],
    *,
    route: str,
    key_name: str,
    limit: int = 10,
) -> List[Dict[str, object]]:
    counter: Counter[str] = Counter()
    for record in route_records:
        if str(record.get("route", "") or "") != route:
            continue
        key = str(record.get(key_name, "") or "").strip()
        if not key:
            continue
        counter[key] += 1
    return summarize_counter(counter, key_name=key_name.replace("source_", ""), limit=limit)


def build_agent_hunt_convergence_stats(
    *,
    validated_rows: List[Dict[str, str]],
    row_assessments: List[Dict[str, str | bool]],
    existing_master_rows: List[Dict[str, str]],
    candidate_outcome_records: List[Dict[str, object]],
    rescued_candidate_identities: set[str] | None = None,
) -> Dict[str, object]:
    existing_author_domains: set[str] = set()
    existing_author_names: set[str] = set()
    for row in existing_master_rows:
        identity = normalized_scout_author_identity(row)
        if identity:
            existing_author_names.add(identity)
        for field in ("AuthorWebsite", "ContactURL", "ContactPageURL", "SubscribeURL", "SourceURL"):
            domain = registrable_domain(row.get(field, ""))
            if domain:
                existing_author_domains.add(domain)
    identity_counts: Counter[str] = Counter()
    qualified_identity_counts: Counter[str] = Counter()
    novelty_counts: Counter[str] = Counter()
    novel_url_fail_reasons: Counter[str] = Counter()
    novel_author_fail_reasons: Counter[str] = Counter()
    repeated_author_counts: Counter[str] = Counter()
    converged_source_domains: Counter[str] = Counter()
    converged_source_types: Counter[str] = Counter()
    converged_source_queries: Counter[str] = Counter()
    scoutworthy_rows = 0
    outcome_lookup = build_candidate_outcome_lookup(candidate_outcome_records)
    rescued_candidate_identities = set(rescued_candidate_identities or set())

    prepared_rows: List[Dict[str, object]] = []
    for record in candidate_outcome_records:
        if candidate_identity_from_outcome(record) in rescued_candidate_identities:
            continue
        reason = str(record.get("reject_reason", "") or "").strip()
        if not reason or reason == "kept":
            continue
        novel_url_fail_reasons[reason] += 1

    for row, assessment in zip(validated_rows, row_assessments):
        identity = normalized_scout_author_identity(row)
        candidate_domain = best_scout_candidate_domain(row)
        author_known = bool(identity and identity in existing_author_names)
        url_known = bool(candidate_domain and candidate_domain in existing_author_domains)
        if identity:
            identity_counts[identity] += 1
        if bool(assessment.get("qualified", False)) and identity and not author_known:
            qualified_identity_counts[identity] += 1
        novelty_key = (
            f"{'known' if url_known else 'novel'}_candidate_url_"
            f"{'existing' if author_known else 'novel'}_author"
        )
        novelty_counts[novelty_key] += 1
        if assessment.get("status") == "scoutworthy_not_outreach_ready":
            scoutworthy_rows += 1
        prepared_rows.append(
            {
                "row": row,
                "assessment": assessment,
                "identity": identity,
                "candidate_domain": candidate_domain,
                "author_known": author_known,
                "url_known": url_known,
            }
        )

    for item in prepared_rows:
        identity = str(item.get("identity", "") or "")
        author_known = bool(item.get("author_known", False))
        if identity and (author_known or int(identity_counts.get(identity, 0) or 0) > 1):
            repeated_author_counts[identity] += 1

        if bool(item.get("url_known", False)):
            continue

        row = item["row"]
        assessment = item["assessment"]
        reason = ""
        if author_known:
            reason = "existing_author_identity"
            source_url = (row.get("SourceURL", "") or "").strip()
            source_domain = registrable_domain(source_url)
            if source_domain:
                converged_source_domains[source_domain] += 1
            candidate_domain = str(item.get("candidate_domain", "") or "")
            context = outcome_lookup.get((source_url, candidate_domain), {})
            source_type = str(context.get("source_type", "") or infer_source_type_from_source_url(source_url))
            source_query = str(context.get("source_query", "") or "<unknown>")
            converged_source_types[source_type or "unknown"] += 1
            converged_source_queries[source_query] += 1
        elif not bool(assessment.get("qualified", False)):
            reason = str(assessment.get("reason", "") or "")
            if reason:
                novel_author_fail_reasons[reason] += 1
        if reason:
            novel_url_fail_reasons[reason] += 1

    for identity, count in qualified_identity_counts.items():
        extra_rows = max(0, int(count or 0) - 1)
        if extra_rows <= 0:
            continue
        novel_url_fail_reasons["duplicate_author_identity_in_batch"] += extra_rows

    return {
        "repeated_author_identity_count": sum(int(count) for count in repeated_author_counts.values()),
        "repeated_author_identities": [
            {
                "author_identity": identity,
                "count": count,
                "already_known": identity in existing_author_names,
            }
            for identity, count in repeated_author_counts.most_common(10)
        ],
        "candidate_url_vs_author_novelty": dict(novelty_counts),
        "novel_candidate_url_existing_author_count": int(novelty_counts.get("novel_candidate_url_existing_author", 0) or 0),
        "top_fail_reasons_for_novel_candidate_urls": dict(novel_url_fail_reasons.most_common(10)),
        "top_fail_reasons_for_novel_authors": dict(novel_author_fail_reasons.most_common(10)),
        "top_converged_source_domains": summarize_counter(converged_source_domains, key_name="domain"),
        "top_converged_source_types": summarize_counter(converged_source_types, key_name="source"),
        "top_converged_source_queries": summarize_counter(converged_source_queries, key_name="query"),
        "scoutworthy_not_outreach_ready_rows": scoutworthy_rows,
    }


def build_agent_hunt_listing_friction_stats(
    *,
    candidate_outcome_records: List[Dict[str, object]],
    qualified_listing_records: List[Dict[str, object]],
    caution_listing_records: List[Dict[str, object]],
) -> Dict[str, object]:
    top_listing_failure_reasons: Counter[str] = Counter()
    listing_dead_end_source_domains: Counter[str] = Counter()
    listing_dead_end_source_types: Counter[str] = Counter()
    listing_dead_end_source_queries: Counter[str] = Counter()
    qualified_reason_counts: Counter[str] = Counter()
    qualified_source_domains: Counter[str] = Counter()
    qualified_source_types: Counter[str] = Counter()
    qualified_source_queries: Counter[str] = Counter()
    caution_reason_counts: Counter[str] = Counter()
    caution_source_domains: Counter[str] = Counter()
    caution_source_types: Counter[str] = Counter()
    caution_source_queries: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()

    for record in candidate_outcome_records:
        reason = str(record.get("reject_reason", "") or "").strip()
        category = classify_agent_hunt_listing_failure(reason, record=record)
        if not category:
            continue
        category_counts[category] += 1
        top_listing_failure_reasons[reason] += 1
        source_url = str(record.get("source_url", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(source_url)
        source_query = str(record.get("source_query", "") or "").strip() or "<unknown>"
        if source_domain:
            listing_dead_end_source_domains[source_domain] += 1
        if source_type:
            listing_dead_end_source_types[source_type] += 1
        listing_dead_end_source_queries[source_query] += 1

    for item in qualified_listing_records:
        reason = str(item.get("listing_failure_reason", "") or "").strip()
        row = dict(item.get("row", {}) or {})
        if not reason or not row:
            continue
        qualified_reason_counts[reason] += 1
        source_url = (row.get("SourceURL", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = infer_source_type_from_source_url(str(row.get("SourceURL", "") or source_url))
        source_query = str(item.get("source_query", "") or "<unknown>")
        if source_domain:
            qualified_source_domains[source_domain] += 1
        if source_type:
            qualified_source_types[source_type] += 1
        qualified_source_queries[source_query] += 1

    for item in caution_listing_records:
        reason = str(item.get("listing_failure_reason", "") or "").strip()
        row = dict(item.get("row", {}) or {})
        if not reason or not row:
            continue
        caution_reason_counts[reason] += 1
        source_url = (row.get("SourceURL", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = infer_source_type_from_source_url(str(row.get("SourceURL", "") or source_url))
        source_query = str(item.get("source_query", "") or "<unknown>")
        if source_domain:
            caution_source_domains[source_domain] += 1
        if source_type:
            caution_source_types[source_type] += 1
        caution_source_queries[source_query] += 1

    return {
        "top_listing_failure_reasons": dict(top_listing_failure_reasons.most_common(10)),
        "listing_failure_categories": dict(category_counts),
        "qualified_listing_blocked_count": len(qualified_listing_records),
        "qualified_listing_blocked_reasons": dict(qualified_reason_counts),
        "qualified_listing_blocked_source_domains": summarize_counter(qualified_source_domains, key_name="domain"),
        "qualified_listing_blocked_source_types": summarize_counter(qualified_source_types, key_name="source"),
        "qualified_listing_blocked_source_queries": summarize_counter(qualified_source_queries, key_name="query"),
        "scoutworthy_listing_blocked_count": len(caution_listing_records),
        "scoutworthy_listing_blocked_reasons": dict(caution_reason_counts),
        "scoutworthy_listing_blocked_source_domains": summarize_counter(caution_source_domains, key_name="domain"),
        "scoutworthy_listing_blocked_source_types": summarize_counter(caution_source_types, key_name="source"),
        "scoutworthy_listing_blocked_source_queries": summarize_counter(caution_source_queries, key_name="query"),
        "top_listing_dead_end_source_domains": summarize_counter(listing_dead_end_source_domains, key_name="domain"),
        "top_listing_dead_end_source_types": summarize_counter(listing_dead_end_source_types, key_name="source"),
        "top_listing_dead_end_source_queries": summarize_counter(listing_dead_end_source_queries, key_name="query"),
    }


def build_agent_hunt_email_recovery_stats(candidate_outcome_records: List[Dict[str, object]]) -> Dict[str, object]:
    fail_reasons: Counter[str] = Counter()
    recovered_source_domains: Counter[str] = Counter()
    recovered_source_types: Counter[str] = Counter()
    recovered_source_queries: Counter[str] = Counter()
    failed_source_domains: Counter[str] = Counter()
    failed_source_types: Counter[str] = Counter()
    failed_source_queries: Counter[str] = Counter()
    attempted = 0
    success = 0

    for record in candidate_outcome_records:
        if not bool(record.get("agent_hunt_email_recovery_attempted", False)):
            continue
        attempted += 1
        source_url = str(record.get("source_url", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(source_url)
        source_query = str(record.get("source_query", "") or "").strip() or "<unknown>"
        if bool(record.get("agent_hunt_email_recovery_success", False)):
            success += 1
            if source_domain:
                recovered_source_domains[source_domain] += 1
            if source_type:
                recovered_source_types[source_type] += 1
            recovered_source_queries[source_query] += 1
            continue
        fail_reason = str(record.get("agent_hunt_email_recovery_fail_reason", "") or "").strip() or "unknown"
        fail_reasons[fail_reason] += 1
        if source_domain:
            failed_source_domains[source_domain] += 1
        if source_type:
            failed_source_types[source_type] += 1
        failed_source_queries[source_query] += 1

    fail_count = max(0, attempted - success)
    return {
        "listing_friction_email_recovery_attempted_count": attempted,
        "listing_friction_email_recovery_success_count": success,
        "listing_friction_email_recovery_fail_count": fail_count,
        "top_listing_friction_email_recovery_fail_reasons": dict(fail_reasons.most_common(10)),
        "listing_friction_email_recovered_source_domains": summarize_counter(recovered_source_domains, key_name="domain"),
        "listing_friction_email_recovered_source_types": summarize_counter(recovered_source_types, key_name="source"),
        "listing_friction_email_recovered_source_queries": summarize_counter(recovered_source_queries, key_name="query"),
        "listing_friction_email_recovery_failed_source_domains": summarize_counter(failed_source_domains, key_name="domain"),
        "listing_friction_email_recovery_failed_source_types": summarize_counter(failed_source_types, key_name="source"),
        "listing_friction_email_recovery_failed_source_queries": summarize_counter(failed_source_queries, key_name="query"),
    }


def build_agent_hunt_stats(
    *,
    validated_rows: List[Dict[str, str]],
    validator_reject_reasons: Dict[str, object],
    target: int,
    existing_master_rows: List[Dict[str, str]] | None = None,
    candidate_outcome_records: List[Dict[str, object]] | None = None,
) -> Dict[str, object]:
    row_assessments = [assess_agent_hunt_row(row) for row in validated_rows]
    directly_qualified_rows = [
        with_agent_hunt_outreach_metadata(row, assessment)
        for row, assessment in zip(validated_rows, row_assessments)
        if bool(assessment.get("qualified", False))
    ]
    outcome_lookup = build_candidate_outcome_lookup(list(candidate_outcome_records or []))
    known_author_identities = {
        normalized_scout_author_identity(row)
        for row in list(existing_master_rows or []) + directly_qualified_rows
        if normalized_scout_author_identity(row)
    }
    known_emails = {
        (row.get("AuthorEmail", "") or "").strip().lower()
        for row in list(existing_master_rows or []) + directly_qualified_rows
        if (row.get("AuthorEmail", "") or "").strip()
    }
    rescued_listing_records: List[Dict[str, object]] = []
    rescued_listing_rows: List[Dict[str, str]] = []
    rescued_candidate_identities: set[str] = set()
    rescued_listing_reasons: Counter[str] = Counter()
    caution_candidate_identities: set[str] = set()
    caution_listing_records: List[Dict[str, object]] = []
    caution_listing_rows: List[Dict[str, str]] = []
    caution_listing_reasons: Counter[str] = Counter()
    caution_promotion_blockers: Counter[str] = Counter()
    scout_reject_counts: Counter[str] = Counter()
    scoutworthy_counts: Counter[str] = Counter()
    scoutworthy_rows: List[Dict[str, str]] = []
    route_counts: Counter[str] = Counter()
    route_reason_counts: Dict[str, Counter[str]] = defaultdict(Counter)
    route_source_records: List[Dict[str, str]] = []
    outreach_tier_counts: Counter[str] = Counter()
    outreach_tier_reason_counts: Dict[str, Counter[str]] = defaultdict(Counter)
    outreach_decision_counts: Counter[str] = Counter()

    for row, assessment in zip(validated_rows, row_assessments):
        route = str(assessment.get("status", "rejected") or "rejected")
        reason = str(assessment.get("reason", "") or "")
        route_counts[route] += 1
        if reason:
            route_reason_counts[route][reason] += 1
            if route == "scoutworthy_not_outreach_ready":
                caution_promotion_blockers[reason] += 1
        route_context = build_route_source_context(row=row, outcome_lookup=outcome_lookup)
        route_source_records.append(
            {
                "route": route,
                "reason": reason,
                **route_context,
            }
        )
        if route == "qualified":
            tier = str(assessment.get("outreach_tier", "") or "").strip()
            decision = str(assessment.get("decision", "") or "").strip()
            tier_reason = str(assessment.get("tier_reason", "") or "").strip()
            if tier:
                outreach_tier_counts[tier] += 1
            if decision:
                outreach_decision_counts[decision] += 1
            if tier and tier_reason:
                outreach_tier_reason_counts[tier][tier_reason] += 1

    for record in list(candidate_outcome_records or []):
        listing_assessment = assess_agent_hunt_listing_blocked_record(record)
        assessment_status = str(listing_assessment.get("status", "") or "rejected")
        if assessment_status == "scoutworthy_not_outreach_ready":
            row = dict(listing_assessment.get("row", {}) or {})
            if not row:
                continue
            identity = normalized_scout_author_identity(row)
            email = (row.get("AuthorEmail", "") or "").strip().lower()
            if (identity and identity in known_author_identities) or (email and email in known_emails):
                continue
            reason = str(listing_assessment.get("reason", "") or "")
            caution_candidate_identities.add(candidate_identity_from_outcome(record))
            caution_listing_reasons[str(listing_assessment.get("listing_failure_reason", "") or "")] += 1
            if reason:
                route_reason_counts["scoutworthy_not_outreach_ready"][reason] += 1
                caution_promotion_blockers[str(listing_assessment.get("promotion_blocker_reason", "") or reason)] += 1
                scoutworthy_counts[reason] += 1
            route_counts["scoutworthy_not_outreach_ready"] += 1
            route_source_records.append(
                {
                    "route": "scoutworthy_not_outreach_ready",
                    "reason": reason,
                    "source_url": str(record.get("source_url", "") or "").strip(),
                    "source_domain": registrable_domain(str(record.get("source_url", "") or "").strip()),
                    "source_type": str(record.get("source_type", "") or "").strip()
                    or infer_source_type_from_source_url(str(record.get("source_url", "") or "").strip()),
                    "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
                }
            )
            scoutworthy_rows.append(row)
            caution_listing_rows.append(row)
            caution_listing_records.append(
                {
                    **listing_assessment,
                    "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
                    "source_type": str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(
                        str(record.get("source_url", "") or "")
                    ),
                }
            )
            continue
        if not bool(listing_assessment.get("qualified", False)):
            continue
        row = dict(listing_assessment.get("row", {}) or {})
        if not row:
            continue
        identity = normalized_scout_author_identity(row)
        email = (row.get("AuthorEmail", "") or "").strip().lower()
        if (identity and identity in known_author_identities) or (email and email in known_emails):
            continue
        rescued_candidate_identities.add(candidate_identity_from_outcome(record))
        rescued_listing_rows.append(row)
        rescued_listing_reasons[str(listing_assessment.get("listing_failure_reason", "") or "")] += 1
        tier = str(listing_assessment.get("outreach_tier", "") or "").strip()
        decision = str(listing_assessment.get("decision", "") or "").strip()
        tier_reason = str(listing_assessment.get("tier_reason", "") or "").strip()
        if tier:
            outreach_tier_counts[tier] += 1
        if decision:
            outreach_decision_counts[decision] += 1
        if tier and tier_reason:
            outreach_tier_reason_counts[tier][tier_reason] += 1
        rescued_listing_records.append(
            {
                **listing_assessment,
                "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
                "source_type": str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(
                    str(record.get("source_url", "") or "")
                ),
            }
        )
        route_counts["qualified"] += 1
        route_reason_counts["qualified"][f"listing_blocked:{str(listing_assessment.get('listing_failure_reason', '') or '')}"] += 1
        route_source_records.append(
            {
                "route": "qualified",
                "reason": f"listing_blocked:{str(listing_assessment.get('listing_failure_reason', '') or '')}",
                "source_url": str(record.get("source_url", "") or "").strip(),
                "source_domain": registrable_domain(str(record.get("source_url", "") or "").strip()),
                "source_type": str(record.get("source_type", "") or "").strip()
                or infer_source_type_from_source_url(str(record.get("source_url", "") or "").strip()),
                "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
            }
        )
        if identity:
            known_author_identities.add(identity)
        if email:
            known_emails.add(email)
    scouted_rows = dedupe_scout_rows(directly_qualified_rows + rescued_listing_rows)
    scouted_rows_by_tier = partition_agent_hunt_rows_by_outreach_tier(scouted_rows)
    strict_rows = [row for row in scouted_rows if row_is_fully_verified(row, require_us_location=True)]
    for row, assessment in zip(validated_rows, row_assessments):
        status = str(assessment.get("status", "") or "")
        reason = str(assessment.get("reason", "") or "")
        if not reason:
            continue
        if status == "scoutworthy_not_outreach_ready":
            scoutworthy_counts[reason] += 1
            scoutworthy_rows.append(row)
        else:
            scout_reject_counts[reason] += 1
    combined_rejects: Counter[str] = Counter()
    merge_counter_dict(combined_rejects, validator_reject_reasons)
    for reason, count in rescued_listing_reasons.items():
        if not reason:
            continue
        remaining = max(0, int(combined_rejects.get(reason, 0) or 0) - int(count))
        if remaining:
            combined_rejects[reason] = remaining
        elif reason in combined_rejects:
            del combined_rejects[reason]
    for reason, count in caution_listing_reasons.items():
        if not reason:
            continue
        remaining = max(0, int(combined_rejects.get(reason, 0) or 0) - int(count))
        if remaining:
            combined_rejects[reason] = remaining
        elif reason in combined_rejects:
            del combined_rejects[reason]
    merge_counter_dict(combined_rejects, dict(scout_reject_counts))
    for record in list(candidate_outcome_records or []):
        identity_key = candidate_identity_from_outcome(record)
        if identity_key in rescued_candidate_identities or identity_key in caution_candidate_identities:
            continue
        reason = str(record.get("reject_reason", "") or "").strip()
        if not reason or reason == "kept":
            continue
        route_counts["rejected"] += 1
        route_reason_counts["rejected"][reason] += 1
        outreach_decision_counts["REPLACE"] += 1
        source_url = str(record.get("source_url", "") or "").strip()
        route_source_records.append(
            {
                "route": "rejected",
                "reason": reason,
                "source_url": source_url,
                "source_domain": registrable_domain(source_url),
                "source_type": str(record.get("source_type", "") or "").strip()
                or infer_source_type_from_source_url(source_url),
                "source_query": str(record.get("source_query", "") or "").strip() or "<unknown>",
            }
        )
    convergence_stats = build_agent_hunt_convergence_stats(
        validated_rows=validated_rows,
        row_assessments=row_assessments,
        existing_master_rows=list(existing_master_rows or []),
        candidate_outcome_records=list(candidate_outcome_records or []),
        rescued_candidate_identities=rescued_candidate_identities,
    )
    listing_friction_stats = build_agent_hunt_listing_friction_stats(
        candidate_outcome_records=list(candidate_outcome_records or []),
        qualified_listing_records=rescued_listing_records,
        caution_listing_records=caution_listing_records,
    )
    email_recovery_stats = build_agent_hunt_email_recovery_stats(list(candidate_outcome_records or []))
    return {
        "scouted_rows": scouted_rows,
        "scouted_rows_by_tier": scouted_rows_by_tier,
        "listing_blocked_scout_rows": rescued_listing_rows,
        "listing_blocked_caution_rows": caution_listing_rows,
        "rescued_candidate_identities": sorted(rescued_candidate_identities),
        "strict_rows": strict_rows,
        "scouted_target": int(target),
        "scouted_progress": len(scouted_rows),
        "scouted_rows_written": len(scouted_rows),
        "strict_rows_written": len(strict_rows),
        "top_reject_reasons": dict(combined_rejects.most_common(10)),
        "scout_gate_reject_reasons": dict(scout_reject_counts),
        "routing_counts": {
            "qualified": int(route_counts.get("qualified", 0) or 0),
            "scoutworthy_not_outreach_ready": int(route_counts.get("scoutworthy_not_outreach_ready", 0) or 0),
            "rejected": int(route_counts.get("rejected", 0) or 0),
        },
        "outreach_tier_counts": {
            AGENT_HUNT_TIER_1: int(outreach_tier_counts.get(AGENT_HUNT_TIER_1, 0) or 0),
            AGENT_HUNT_TIER_2: int(outreach_tier_counts.get(AGENT_HUNT_TIER_2, 0) or 0),
            AGENT_HUNT_TIER_3: int(outreach_tier_counts.get(AGENT_HUNT_TIER_3, 0) or 0),
        },
        "outreach_decision_counts": dict(outreach_decision_counts),
        "top_tier_1_reasons": dict(outreach_tier_reason_counts[AGENT_HUNT_TIER_1].most_common(10)),
        "top_tier_2_reasons": dict(outreach_tier_reason_counts[AGENT_HUNT_TIER_2].most_common(10)),
        "top_tier_3_reasons": dict(outreach_tier_reason_counts[AGENT_HUNT_TIER_3].most_common(10)),
        "top_caution_reasons": dict(route_reason_counts["scoutworthy_not_outreach_ready"].most_common(10)),
        "top_rejected_reasons": dict(route_reason_counts["rejected"].most_common(10)),
        "top_reasons_caution_fail_promotion_to_qualified": dict(
            caution_promotion_blockers.most_common(10)
        ),
        "scoutworthy_not_outreach_ready_count": len(scoutworthy_rows),
        "scoutworthy_not_outreach_ready_reasons": dict(scoutworthy_counts),
        "scoutworthy_not_outreach_ready_source_domains": summarize_row_domains(
            scoutworthy_rows,
            source_selector=lambda row: row.get("SourceURL", ""),
        ),
        "caution_source_types": summarize_route_source_contexts(route_source_records, route="scoutworthy_not_outreach_ready", key_name="source_type"),
        "caution_source_queries": summarize_route_source_contexts(route_source_records, route="scoutworthy_not_outreach_ready", key_name="source_query"),
        "rejected_source_domains": summarize_route_source_contexts(route_source_records, route="rejected", key_name="source_domain"),
        "rejected_source_types": summarize_route_source_contexts(route_source_records, route="rejected", key_name="source_type"),
        "rejected_source_queries": summarize_route_source_contexts(route_source_records, route="rejected", key_name="source_query"),
        "listing_friction": listing_friction_stats,
        **email_recovery_stats,
        "scouted_source_domains": summarize_row_domains(scouted_rows, source_selector=best_scout_source_url),
        "strict_source_domains": summarize_row_domains(strict_rows, source_selector=best_verified_source_url),
        "author_convergence": convergence_stats,
    }


def row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        return max(0, sum(1 for _ in fh) - 1)


def analyze_candidates(path: Path) -> Dict[str, object]:
    query_counts: Dict[str, int] = {}
    domain_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    total = 0
    if not path.exists():
        return {"total": 0, "per_query": {}, "per_source": {}, "top_domains": []}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            total += 1
            query = (row.get("SourceQuery", "") or "").strip()
            source = (row.get("SourceType", "") or "").strip()
            if query:
                query_counts[query] = query_counts.get(query, 0) + 1
            if not source:
                lowered = query.lower()
                if lowered.startswith("goodreads:"):
                    source = "goodreads"
                elif lowered.startswith("openlibrary:"):
                    source = "openlibrary"
                else:
                    source = "web_search"
            source_counts[source] = source_counts.get(source, 0) + 1
            url = (row.get("CandidateURL", "") or "").strip()
            domain = ""
            if "://" in url:
                domain = url.split("://", 1)[1].split("/", 1)[0].lower()
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
    top_domains = sorted(domain_counts.items(), key=lambda kv: kv[1], reverse=True)[:15]
    return {
        "total": total,
        "per_query": dict(sorted(query_counts.items(), key=lambda kv: kv[1], reverse=True)),
        "per_source": dict(sorted(source_counts.items(), key=lambda kv: kv[1], reverse=True)),
        "top_domains": [{"domain": domain, "count": count} for domain, count in top_domains],
    }


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def write_run_stats(path: Path, stats: Dict[str, object]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(stats, indent=2, ensure_ascii=True), encoding="utf-8")


def write_rejected_rows(path: Path, rows: List[Dict[str, str]]) -> int:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REJECTED_COLUMNS)
        writer.writeheader()
        writer.writerows([{col: (row.get(col, "") or "").strip() for col in REJECTED_COLUMNS} for row in rows])
    return len(rows)


def write_run_manifest(path: Path, payload: Dict[str, object]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def write_live_status(
    *,
    status: str,
    stage: str,
    validation_profile: str,
    goal_target: int,
    active: bool,
    run_tag: str = "",
    batch_index: int = 0,
    max_runs: int = 0,
    counts: Dict[str, object] | None = None,
    pipeline_exit_code: int | None = None,
    stop_reason: str = "",
    message: str = "",
    runs_dir: Path | None = None,
    log_path: Path | None = None,
    artifact_paths: Dict[str, Path] | None = None,
) -> None:
    payload = load_json(LIVE_STATUS_PATH)
    payload.update(
        {
            "active": bool(active),
            "status": str(status or "").strip().lower() or "idle",
            "stage": str(stage or "").strip().lower() or "idle",
            "validation_profile": validation_profile,
            "goal_target": int(goal_target),
            "run_tag": run_tag,
            "batch_index": int(batch_index or 0),
            "max_runs": int(max_runs or 0),
            "pipeline_exit_code": None if pipeline_exit_code is None else int(pipeline_exit_code),
            "stop_reason": str(stop_reason or "").strip(),
            "message": str(message or "").strip(),
            "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
    )
    if counts is not None:
        payload["counts"] = {
            key: int(value) if isinstance(value, bool) is False and isinstance(value, int) else value
            for key, value in counts.items()
        }
    if runs_dir is not None:
        payload["runs_dir"] = str(runs_dir)
    if log_path is not None:
        payload["log_path"] = str(log_path)
    if artifact_paths is not None:
        payload["artifacts"] = {key: str(path) for key, path in artifact_paths.items()}
    ensure_parent(LIVE_STATUS_PATH)
    LIVE_STATUS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def build_rejected_rows(candidate_outcomes: List[Dict[str, object]], run_id: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for record in candidate_outcomes:
        if bool(record.get("passed", False)):
            continue
        fail_reasons_raw = list(record.get("fail_reasons", []) or [])
        fail_reasons = [str(reason).strip() for reason in fail_reasons_raw if str(reason).strip()]
        rows.append(
            {
                "RunID": run_id,
                "AuthorName": str(record.get("author_name", "") or ""),
                "BookTitle": str(record.get("book_title", "") or ""),
                "CandidateURL": str(record.get("candidate_url", "") or ""),
                "CandidateDomain": str(record.get("candidate_domain", "") or ""),
                "SourceURL": str(record.get("source_url", "") or ""),
                "BookURL": str(record.get("book_url", "") or ""),
                "Email": str(record.get("email", "") or "").strip().lower(),
                "Confidence": str(record.get("confidence", "") or ""),
                "PrimaryFailReason": str(record.get("primary_fail_reason", "") or ""),
                "FailReasons": "|".join(fail_reasons),
                "RejectReason": str(record.get("reject_reason", "") or ""),
                "CurrentState": str(record.get("current_state", "") or ""),
                "NextAction": str(record.get("next_action", "") or ""),
                "NextActionReason": str(record.get("next_action_reason", "") or ""),
                "EmailSnippet": str(record.get("email_snippet", "") or ""),
                "USSnippet": str(record.get("us_snippet", "") or ""),
                "IndieSnippet": str(record.get("indie_snippet", "") or ""),
                "ListingSnippet": str(record.get("listing_snippet", "") or ""),
            }
        )
    return rows


def build_duplicate_rejected_rows(rows: List[Dict[str, str]], run_id: str) -> List[Dict[str, str]]:
    rejected: List[Dict[str, str]] = []
    for row in rows:
        rejected.append(
            {
                "RunID": run_id,
                "AuthorName": str(row.get("AuthorName", "") or ""),
                "BookTitle": str(row.get("BookTitle", "") or ""),
                "CandidateURL": str(row.get("SourceURL", "") or row.get("AuthorWebsite", "") or row.get("ContactURL", "") or ""),
                "CandidateDomain": registrable_domain(
                    str(row.get("SourceURL", "") or row.get("AuthorWebsite", "") or row.get("ContactURL", "") or "")
                ),
                "SourceURL": str(row.get("SourceURL", "") or ""),
                "BookURL": str(row.get("ListingURL", "") or row.get("BookTitleSourceURL", "") or ""),
                "Email": str(row.get("AuthorEmail", "") or "").strip().lower(),
                "Confidence": "weak",
                "PrimaryFailReason": "duplicate_lead",
                "FailReasons": "duplicate_lead",
                "RejectReason": "duplicate_lead",
                "CurrentState": "dead_end",
                "NextAction": "stop_dead_end",
                "NextActionReason": "duplicate_lead",
                "EmailSnippet": str(row.get("AuthorEmailProofSnippet", "") or ""),
                "USSnippet": str(row.get("LocationProofSnippet", "") or ""),
                "IndieSnippet": str(row.get("IndieProofSnippet", "") or ""),
                "ListingSnippet": "",
            }
        )
    return rejected


def concatenate_jsonl_files(paths: Iterable[Path], output_path: Path) -> None:
    ensure_parent(output_path)
    wrote = False
    with output_path.open("w", encoding="utf-8") as out_fh:
        for path in paths:
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            if not text.strip():
                continue
            out_fh.write(text.rstrip() + "\n")
            wrote = True
    if not wrote and output_path.exists():
        output_path.write_text("", encoding="utf-8")


def normalize_location(value: str) -> str:
    return (value or "").strip().lower()


def append_log_line(path: Path, line: str) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line.rstrip() + "\n")


def summarize_rejected_reason_counts(rows: List[Dict[str, str]]) -> Dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        reason = (row.get("PrimaryFailReason", "") or "").strip()
        if not reason:
            fail_reasons = [part.strip() for part in str(row.get("FailReasons", "") or "").split("|") if part.strip()]
            reason = fail_reasons[0] if fail_reasons else ""
        if reason:
            counter[reason] += 1
    return dict(counter)


def secret_values_from_cmd(cmd: List[str]) -> List[str]:
    secrets: List[str] = []
    for idx, token in enumerate(cmd[:-1]):
        if token in SECRET_FLAGS:
            value = cmd[idx + 1].strip()
            if value:
                secrets.append(value)
    return secrets


def format_logged_command(cmd: List[str]) -> str:
    redacted: List[str] = []
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


def sanitize_logged_text(text: str, secrets: List[str]) -> str:
    sanitized = text or ""
    for secret in secrets:
        if secret:
            sanitized = sanitized.replace(secret, "<redacted>")
    return sanitized


def infer_candidate_author_name(url: str) -> str:
    path = urlparse(url).path
    match = AUTHOR_PATH_RE.search(path)
    if not match:
        return ""
    slug = match.group(1).replace("-", " ").replace("_", " ")
    return normalize_person_name(slug)


def listing_key_for_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host == "amazon.com" or host.endswith(".amazon.com"):
        if any(token in path for token in ("/dp/", "/gp/product/", "/kindle-dbs/product/")):
            return canonical_listing_key(url)
        return ""
    if host == "barnesandnoble.com" or host.endswith(".barnesandnoble.com"):
        if "/w/" in path or "/s/" in path:
            return canonical_listing_key(url)
        return ""
    return ""


def build_existing_indexes(master_rows: List[Dict[str, str]]) -> Tuple[set[str], set[str], set[str]]:
    author_domains: set[str] = set()
    listing_keys: set[str] = set()
    author_names: set[str] = set()

    for row in master_rows:
        book_title_status = (row.get("BookTitleStatus", "ok") or "ok").strip().lower()
        allow_revisit = book_title_status not in {"", "ok"}
        for url_field in ("AuthorWebsite", "ContactPageURL", "SubscribeURL"):
            domain = registrable_domain(row.get(url_field, ""))
            if domain and not allow_revisit:
                author_domains.add(domain)
        listing_key = listing_key_for_url(row.get("ListingURL", ""))
        if listing_key:
            listing_keys.add(listing_key)
        author_name = normalize_person_name(row.get("AuthorName", ""))
        if author_name and not allow_revisit:
            author_names.add(author_name)

    return author_domains, listing_keys, author_names


def suppress_pre_validate_candidates(
    candidate_rows: List[Dict[str, str]],
    master_rows: List[Dict[str, str]],
) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    author_domains, listing_keys, author_names = build_existing_indexes(master_rows)
    kept: List[Dict[str, str]] = []
    reason_counts: Dict[str, int] = {}
    source_totals: Counter[str] = Counter()
    source_new: Counter[str] = Counter()
    source_duplicates: Counter[str] = Counter()
    query_totals: Counter[str] = Counter()
    query_duplicates: Counter[str] = Counter()

    def infer_source_label(row: Dict[str, str]) -> str:
        source = (row.get("SourceType", "") or "").strip()
        if source:
            return source
        lowered = (row.get("SourceQuery", "") or "").strip().lower()
        if lowered.startswith("goodreads:"):
            return "goodreads"
        if lowered.startswith("openlibrary:"):
            return "openlibrary"
        return "web_search"

    for row in candidate_rows:
        url = (row.get("CandidateURL", "") or "").strip()
        if not url:
            continue
        source = infer_source_label(row)
        query = (row.get("SourceQuery", "") or "").strip() or "<none>"
        source_totals[source] += 1
        query_totals[query] += 1
        listing_key = listing_key_for_url(url)
        if listing_key and listing_key in listing_keys:
            reason_counts["listing_key"] = reason_counts.get("listing_key", 0) + 1
            source_duplicates[source] += 1
            query_duplicates[query] += 1
            continue

        candidate_domain = registrable_domain(url)
        if candidate_domain and candidate_domain in author_domains and not listing_key:
            reason_counts["author_site_domain"] = reason_counts.get("author_site_domain", 0) + 1
            source_duplicates[source] += 1
            query_duplicates[query] += 1
            continue

        inferred_author = infer_candidate_author_name(url)
        if inferred_author and inferred_author in author_names:
            reason_counts["author_name"] = reason_counts.get("author_name", 0) + 1
            source_duplicates[source] += 1
            query_duplicates[query] += 1
            continue

        kept.append(row)
        source_new[source] += 1

    total = sum(reason_counts.values())
    return kept, {
        "suppressed_pre_validate_total": total,
        "suppressed_pre_validate_by_reason": dict(sorted(reason_counts.items(), key=lambda kv: kv[1], reverse=True)),
        "new_unique_candidates_by_source": dict(sorted(source_new.items(), key=lambda kv: (-kv[1], kv[0]))),
        "duplicate_hit_rate_by_source": [
            {
                "source": source,
                "duplicates": int(source_duplicates[source]),
                "new": int(source_new[source]),
                "total": int(total_count),
                "duplicate_hit_rate": round(source_duplicates[source] / total_count, 3) if total_count else 0.0,
            }
            for source, total_count in sorted(source_totals.items(), key=lambda kv: (-kv[1], kv[0]))
        ],
        "duplicate_hit_rate_by_query": [
            {
                "query": query,
                "duplicates": int(query_duplicates[query]),
                "total": int(total_count),
                "duplicate_hit_rate": round(query_duplicates[query] / total_count, 3) if total_count else 0.0,
            }
            for query, total_count in sorted(query_totals.items(), key=lambda kv: (-kv[1], kv[0]))
        ],
        "top_repeat_sources": [
            {"source": source, "duplicates": int(count)}
            for source, count in source_duplicates.most_common(10)
        ],
        "top_new_sources": [
            {"source": source, "new": int(count)}
            for source, count in source_new.most_common(10)
        ],
    }


def build_agent_hunt_listing_feedback(
    candidate_outcome_records: List[Dict[str, object]],
    *,
    rescued_candidate_identities: set[str] | None = None,
) -> Dict[str, object]:
    source_domain_totals: Counter[str] = Counter()
    source_domain_listing_friction: Counter[str] = Counter()
    source_type_totals: Counter[str] = Counter()
    source_type_listing_friction: Counter[str] = Counter()
    query_totals: Counter[str] = Counter()
    query_listing_friction: Counter[str] = Counter()

    rescued_candidate_identities = set(rescued_candidate_identities or set())
    for record in candidate_outcome_records:
        if candidate_identity_from_outcome(record) in rescued_candidate_identities:
            continue
        source_url = str(record.get("source_url", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(source_url)
        source_query = str(record.get("source_query", "") or "").strip() or "<unknown>"
        reason = str(record.get("reject_reason", "") or "").strip()
        listing_failure_category = classify_agent_hunt_listing_failure(reason, record=record)

        if source_domain:
            source_domain_totals[source_domain] += 1
        if source_type:
            source_type_totals[source_type] += 1
        query_totals[source_query] += 1

        if listing_failure_category not in {
            "listing_absent",
            "listing_retailer_friction_blocked",
            "listing_retailer_friction_recoverable",
        }:
            continue

        if source_domain:
            source_domain_listing_friction[source_domain] += 1
        if source_type:
            source_type_listing_friction[source_type] += 1
        query_listing_friction[source_query] += 1

    def build_penalties(
        total_counts: Counter[str],
        friction_counts: Counter[str],
        *,
        hot_paths: set[str] | None = None,
    ) -> Dict[str, int]:
        penalties: Dict[str, int] = {}
        hot_paths = set(hot_paths or set())
        for key, friction_count in friction_counts.items():
            total_count = int(total_counts.get(key, 0) or 0)
            if total_count < 3 or friction_count < 2:
                continue
            ratio = friction_count / total_count if total_count else 0.0
            penalty = 0
            if ratio >= 0.85:
                penalty = 6
            elif ratio >= 0.7:
                penalty = 4
            elif ratio >= 0.5:
                penalty = 2
            if penalty and key in hot_paths and ratio >= 0.7:
                penalty = min(8, penalty + 2)
            if penalty:
                penalties[key] = penalty
        return penalties

    query_penalties = build_penalties(
        query_totals,
        query_listing_friction,
        hot_paths=SCOUT_LISTING_HOT_PATH_SOURCE_QUERIES,
    )
    source_type_penalties = build_penalties(
        source_type_totals,
        source_type_listing_friction,
        hot_paths=SCOUT_LISTING_HOT_PATH_SOURCE_TYPES,
    )
    source_domain_penalties = build_penalties(
        source_domain_totals,
        source_domain_listing_friction,
        hot_paths=SCOUT_LISTING_HOT_PATH_SOURCE_DOMAINS,
    )
    return {
        "query_penalties": query_penalties,
        "source_type_penalties": source_type_penalties,
        "source_domain_penalties": source_domain_penalties,
        "top_listing_friction_queries": summarize_counter(query_listing_friction, key_name="query"),
        "top_listing_friction_source_types": summarize_counter(source_type_listing_friction, key_name="source"),
        "top_listing_friction_source_domains": summarize_counter(source_domain_listing_friction, key_name="domain"),
        "penalized_queries": [{"query": key, "penalty": value} for key, value in sorted(query_penalties.items())],
        "penalized_source_types": [{"source": key, "penalty": value} for key, value in sorted(source_type_penalties.items())],
        "penalized_source_domains": [{"domain": key, "penalty": value} for key, value in sorted(source_domain_penalties.items())],
    }


def build_agent_hunt_source_quality_feedback(
    candidate_outcome_records: List[Dict[str, object]],
) -> Dict[str, object]:
    source_domain_totals: Counter[str] = Counter()
    source_domain_quality_failures: Counter[str] = Counter()
    source_type_totals: Counter[str] = Counter()
    source_type_quality_failures: Counter[str] = Counter()
    query_totals: Counter[str] = Counter()
    query_quality_failures: Counter[str] = Counter()
    failure_reasons: Counter[str] = Counter()

    for record in candidate_outcome_records:
        source_url = str(record.get("source_url", "") or "").strip()
        source_domain = registrable_domain(source_url)
        source_type = str(record.get("source_type", "") or "").strip() or infer_source_type_from_source_url(source_url)
        source_query = str(record.get("source_query", "") or "").strip() or "<unknown>"
        reason = classify_agent_hunt_source_quality_feedback_reason(str(record.get("reject_reason", "") or ""))

        if source_domain:
            source_domain_totals[source_domain] += 1
        if source_type:
            source_type_totals[source_type] += 1
        query_totals[source_query] += 1

        if not reason:
            continue

        failure_reasons[reason] += 1
        if source_domain:
            source_domain_quality_failures[source_domain] += 1
        if source_type:
            source_type_quality_failures[source_type] += 1
        query_quality_failures[source_query] += 1

    def build_penalties(
        total_counts: Counter[str],
        failure_counts: Counter[str],
        *,
        hot_paths: set[str] | None = None,
    ) -> Dict[str, int]:
        penalties: Dict[str, int] = {}
        hot_paths = set(hot_paths or set())
        for key, failure_count in failure_counts.items():
            total_count = int(total_counts.get(key, 0) or 0)
            if total_count < 3 or failure_count < 2:
                continue
            ratio = failure_count / total_count if total_count else 0.0
            penalty = 0
            if ratio >= 0.85:
                penalty = 6
            elif ratio >= 0.7:
                penalty = 4
            elif ratio >= 0.5:
                penalty = 2
            if penalty and key in hot_paths and ratio >= 0.7:
                penalty = min(8, penalty + 2)
            if penalty:
                penalties[key] = penalty
        return penalties

    query_penalties = build_penalties(
        query_totals,
        query_quality_failures,
        hot_paths=SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_QUERIES,
    )
    source_type_penalties = build_penalties(
        source_type_totals,
        source_type_quality_failures,
        hot_paths=SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_TYPES,
    )
    source_domain_penalties = build_penalties(
        source_domain_totals,
        source_domain_quality_failures,
        hot_paths=SCOUT_SOURCE_QUALITY_HOT_PATH_SOURCE_DOMAINS,
    )
    return {
        "query_penalties": query_penalties,
        "source_type_penalties": source_type_penalties,
        "source_domain_penalties": source_domain_penalties,
        "top_source_quality_failure_reasons": dict(failure_reasons.most_common(10)),
        "top_source_quality_failure_queries": summarize_counter(query_quality_failures, key_name="query"),
        "top_source_quality_failure_source_types": summarize_counter(
            source_type_quality_failures,
            key_name="source",
        ),
        "top_source_quality_failure_source_domains": summarize_counter(
            source_domain_quality_failures,
            key_name="domain",
        ),
        "penalized_queries": [{"query": key, "penalty": value} for key, value in sorted(query_penalties.items())],
        "penalized_source_types": [{"source": key, "penalty": value} for key, value in sorted(source_type_penalties.items())],
        "penalized_source_domains": [{"domain": key, "penalty": value} for key, value in sorted(source_domain_penalties.items())],
    }


def classify_agent_hunt_source_quality_suppression_reason(
    row: Dict[str, str],
    scored: Dict[str, object],
) -> str:
    source_type = str(scored.get("source_type", "") or "").strip()
    source_query = str(scored.get("source_query", "") or "").strip()
    source_domain = registrable_domain(str(scored.get("source_url", "") or "").strip())
    if not is_source_quality_hot_path(source_type, source_query, source_domain):
        return ""

    components = dict(scored.get("components", {}) or {})
    score = int(scored.get("score", 0) or 0)
    if int(components.get("epic_enterprise_or_famous_hint", 0) or 0) < 0:
        return "enterprise_or_famous_source_hint"
    if int(components.get("epic_generic_author_title", 0) or 0) < 0:
        return "bad_author_name_generic_source_title"
    if int(components.get("epic_implausible_author_title", 0) or 0) < 0 and score <= INTAKE_SCORE_HIGH_THRESHOLD - 10:
        return "bad_author_name_implausible_source_title"
    if int(components.get("prior_source_quality_penalty", 0) or 0) < 0:
        author_label = normalize_source_author_label(str(row.get("SourceTitle", "") or ""))
        if (not author_label or not looks_like_plausible_author_identity(author_label, str(row.get("CandidateURL", "") or ""))) and score <= (
            INTAKE_SCORE_LOW_THRESHOLD + 8
        ):
            return "repeated_junk_source_low_score"
    return ""


def summarize_before_after_counter(
    before_counts: Counter[str],
    after_counts: Counter[str],
    *,
    key_name: str,
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for key, before in sorted(before_counts.items(), key=lambda item: (-item[1], item[0])):
        after = int(after_counts.get(key, 0))
        out.append(
            {
                key_name: key,
                "before": int(before),
                "after": after,
                "suppressed": int(before) - after,
            }
        )
    return out[:10]


def suppress_agent_hunt_source_quality_candidates(
    candidate_rows: List[Dict[str, str]],
    *,
    scoring_context: Dict[str, object],
) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    score_map = dict(scoring_context.get("score_map", {}) or {})
    score_map_by_url = dict(scoring_context.get("score_map_by_url", {}) or {})
    kept: List[Dict[str, str]] = []
    reason_counts: Counter[str] = Counter()
    suppressed_source_domains: Counter[str] = Counter()
    suppressed_source_types: Counter[str] = Counter()
    suppressed_source_queries: Counter[str] = Counter()
    before_penalized_domains: Counter[str] = Counter()
    before_penalized_source_types: Counter[str] = Counter()
    before_penalized_source_queries: Counter[str] = Counter()
    after_penalized_domains: Counter[str] = Counter()
    after_penalized_source_types: Counter[str] = Counter()
    after_penalized_source_queries: Counter[str] = Counter()
    suppressed_due_to_feedback_count = 0

    for row in candidate_rows:
        scored = score_map.get(candidate_identity_from_row(row))
        if not scored:
            scored = score_map_by_url.get(normalize_url(row.get("CandidateURL", "")) or row.get("CandidateURL", ""))
        if not scored:
            kept.append(row)
            continue
        source_type = str(scored.get("source_type", "") or "").strip()
        source_query = str(scored.get("source_query", "") or "").strip()
        source_domain = registrable_domain(str(scored.get("source_url", "") or "").strip())
        components = dict(scored.get("components", {}) or {})
        is_penalized = int(components.get("prior_source_quality_penalty", 0) or 0) < 0 or is_source_quality_hot_path(
            source_type,
            source_query,
            source_domain,
        )
        if is_penalized:
            if source_domain:
                before_penalized_domains[source_domain] += 1
            if source_type:
                before_penalized_source_types[source_type] += 1
            if source_query:
                before_penalized_source_queries[source_query] += 1
        reason = classify_agent_hunt_source_quality_suppression_reason(row, scored)
        if reason:
            reason_counts[reason] += 1
            if source_domain:
                suppressed_source_domains[source_domain] += 1
            if source_type:
                suppressed_source_types[source_type] += 1
            if source_query:
                suppressed_source_queries[source_query] += 1
            if int(components.get("prior_source_quality_penalty", 0) or 0) < 0:
                suppressed_due_to_feedback_count += 1
            continue
        kept.append(row)
        if is_penalized:
            if source_domain:
                after_penalized_domains[source_domain] += 1
            if source_type:
                after_penalized_source_types[source_type] += 1
            if source_query:
                after_penalized_source_queries[source_query] += 1

    return kept, {
        "suppressed_by_source_quality_count": int(sum(reason_counts.values())),
        "top_source_quality_suppression_reasons": dict(reason_counts.most_common(10)),
        "source_quality_suppressed_source_domains": summarize_counter(suppressed_source_domains, key_name="domain"),
        "source_quality_suppressed_source_types": summarize_counter(suppressed_source_types, key_name="source"),
        "source_quality_suppressed_source_queries": summarize_counter(suppressed_source_queries, key_name="query"),
        "suppressed_due_to_source_quality_feedback_count": suppressed_due_to_feedback_count,
        "source_quality_penalty_before_after_source_domains": summarize_before_after_counter(
            before_penalized_domains,
            after_penalized_domains,
            key_name="domain",
        ),
        "source_quality_penalty_before_after_source_types": summarize_before_after_counter(
            before_penalized_source_types,
            after_penalized_source_types,
            key_name="source",
        ),
        "source_quality_penalty_before_after_source_queries": summarize_before_after_counter(
            before_penalized_source_queries,
            after_penalized_source_queries,
            key_name="query",
        ),
    }


def throttle_agent_hunt_epic_directory_candidates(
    candidate_rows: List[Dict[str, str]],
    *,
    scoring_context: Dict[str, object],
) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    score_map = dict(scoring_context.get("score_map", {}) or {})
    score_map_by_url = dict(scoring_context.get("score_map_by_url", {}) or {})
    epic_rows: List[Tuple[int, int, Dict[str, str], Dict[str, object]]] = []

    for index, row in enumerate(candidate_rows):
        scored = score_map.get(candidate_identity_from_row(row))
        if not scored:
            scored = score_map_by_url.get(normalize_url(row.get("CandidateURL", "")) or row.get("CandidateURL", ""))
        source_type = str((scored or {}).get("source_type", "") or row.get("SourceType", "") or "").strip()
        if source_type == "epic_directory":
            epic_rows.append((int((scored or {}).get("score", 0) or 0), index, row, dict(scored or {})))

    epic_before = len(epic_rows)
    if epic_before <= AGENT_HUNT_EPIC_DIRECTORY_MAX_CANDIDATES_PER_BATCH:
        return candidate_rows, {
            "epic_directory_throttled_count": 0,
            "epic_directory_before_count": epic_before,
            "epic_directory_after_count": epic_before,
            "epic_directory_throttle_cap": AGENT_HUNT_EPIC_DIRECTORY_MAX_CANDIDATES_PER_BATCH,
            "epic_directory_throttle_applied": False,
        }

    epic_rows.sort(key=lambda item: (-item[0], item[1]))
    kept_epic_rows = [row for _, _, row, _ in epic_rows[:AGENT_HUNT_EPIC_DIRECTORY_MAX_CANDIDATES_PER_BATCH]]
    kept_identity_keys = {candidate_identity_from_row(row) for row in kept_epic_rows}
    throttled_rows = [row for _, _, row, _ in epic_rows if candidate_identity_from_row(row) not in kept_identity_keys]
    throttled_source_domains: Counter[str] = Counter()
    throttled_source_queries: Counter[str] = Counter()
    for row in throttled_rows:
        source_domain = registrable_domain(row.get("SourceURL", ""))
        source_query = (row.get("SourceQuery", "") or "").strip()
        if source_domain:
            throttled_source_domains[source_domain] += 1
        if source_query:
            throttled_source_queries[source_query] += 1

    kept_ordered: List[Dict[str, str]] = []
    for row in candidate_rows:
        if (row.get("SourceType", "") or "").strip() == "epic_directory":
            if candidate_identity_from_row(row) in kept_identity_keys:
                kept_ordered.append(row)
        else:
            kept_ordered.append(row)

    return kept_ordered, {
        "epic_directory_throttled_count": len(throttled_rows),
        "epic_directory_before_count": epic_before,
        "epic_directory_after_count": len(kept_epic_rows),
        "epic_directory_throttle_cap": AGENT_HUNT_EPIC_DIRECTORY_MAX_CANDIDATES_PER_BATCH,
        "epic_directory_throttle_applied": True,
        "epic_directory_throttled_source_domains": summarize_counter(throttled_source_domains, key_name="domain"),
        "epic_directory_throttled_source_queries": summarize_counter(throttled_source_queries, key_name="query"),
    }


def run_logged_command(
    cmd: List[str],
    stdout_path: Path,
    stderr_path: Path,
    append: bool = False,
) -> subprocess.CompletedProcess[str]:
    secrets = secret_values_from_cmd(cmd)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    stdout_text = sanitize_logged_text(result.stdout or "", secrets)
    stderr_text = sanitize_logged_text(result.stderr or "", secrets)
    mode = "a" if append else "w"
    with stdout_path.open(mode, encoding="utf-8") as fh:
        fh.write(stdout_text)
    with stderr_path.open(mode, encoding="utf-8") as fh:
        fh.write(stderr_text)
    if stdout_text:
        print(stdout_text, end="")
    if stderr_text:
        print(stderr_text, file=sys.stderr, end="")
    return result


def build_rotating_queries(run_idx: int, *, stale_runs: int = 0) -> List[str]:
    base_queries = list(BASE_QUERY_VARIANTS[(max(0, run_idx - 1 + stale_runs)) % len(BASE_QUERY_VARIANTS)])
    current_year = dt.datetime.now(dt.timezone.utc).year
    years = list(range(current_year - 4, current_year + 1))
    window = min(len(QUERY_GENRES), 4 + min(2, max(0, stale_runs)))
    start = ((run_idx - 1) * window) % len(QUERY_GENRES)
    rotated = [QUERY_GENRES[(start + i) % len(QUERY_GENRES)] for i in range(window)]
    year = years[(run_idx - 1) % len(years)]

    out = list(base_queries)
    for genre in rotated:
        for template in QUERY_TEMPLATES:
            out.append(template.format(genre=genre, year=year))

    # Keep order, remove duplicates.
    seen = set()
    deduped = []
    for query in out:
        if query in seen:
            continue
        seen.add(query)
        deduped.append(query)
    return deduped


def write_queries_file(path: Path, queries: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for query in queries:
            fh.write(query.strip() + "\n")


def keep_verified_rows(rows: List[Dict[str, str]], gate: str) -> List[Dict[str, str]]:
    kept: List[Dict[str, str]] = []
    for row in rows:
        status = (row.get("VerificationStatus", "") or "").strip().lower()
        has_mx = (row.get("HasMX", "") or "").strip().lower() == "true"
        if status == "blocked_role":
            continue
        if gate == "strict":
            if status == "deliverable" and has_mx:
                kept.append(row)
            continue
        # balanced
        if status in {"deliverable", "risky"} and has_mx:
            kept.append(row)
    return kept


def derive_validate_debug_path(stats_path: Path, suffix: str) -> Path:
    return stats_path.with_name(f"{stats_path.stem}{suffix}")


def merge_counter_dict(target: Counter[str], source: Dict[str, object]) -> None:
    for key, value in (source or {}).items():
        try:
            target[str(key)] += int(value or 0)
        except (TypeError, ValueError):
            continue


def compute_batch_target_count(
    *,
    batch_max: int,
    goal_target: int,
    current_total: int,
) -> int:
    remaining_goal = max(0, goal_target - current_total)
    if remaining_goal <= 0:
        return 0
    return min(max(1, batch_max), remaining_goal)


def update_reliability_counters(
    counters: Dict[str, int],
    *,
    pipeline_failed: bool,
    filtered_candidates: int,
    validated_rows: int,
) -> Dict[str, int]:
    updated = {
        "consecutive_failures": int(counters.get("consecutive_failures", 0) or 0),
        "consecutive_empty_candidate_runs": int(counters.get("consecutive_empty_candidate_runs", 0) or 0),
        "consecutive_zero_validated_runs": int(counters.get("consecutive_zero_validated_runs", 0) or 0),
    }
    if pipeline_failed:
        updated["consecutive_failures"] += 1
        return updated

    updated["consecutive_failures"] = 0
    updated["consecutive_empty_candidate_runs"] = (
        updated["consecutive_empty_candidate_runs"] + 1 if filtered_candidates <= 0 else 0
    )
    updated["consecutive_zero_validated_runs"] = (
        updated["consecutive_zero_validated_runs"] + 1 if validated_rows <= 0 else 0
    )
    return updated


def determine_loop_stop_reason(args: argparse.Namespace, *, stale_runs: int, reliability_counters: Dict[str, int]) -> str:
    if stale_runs >= max(1, int(getattr(args, "max_stale_runs", 1) or 1)):
        return "stale_runs"
    if int(reliability_counters.get("consecutive_failures", 0) or 0) >= max(
        1,
        int(getattr(args, "max_consecutive_failures", 1) or 1),
    ):
        return "consecutive_failures"
    if int(reliability_counters.get("consecutive_empty_candidate_runs", 0) or 0) >= max(
        1,
        int(getattr(args, "max_empty_candidate_runs", 1) or 1),
    ):
        return "empty_candidate_runs"
    if int(reliability_counters.get("consecutive_zero_validated_runs", 0) or 0) >= max(
        1,
        int(getattr(args, "max_zero_validated_runs", 1) or 1),
    ):
        return "zero_validated_runs"
    return ""


def orchestrate_candidate_replacements(
    candidate_rows: List[Dict[str, str]],
    *,
    target_verified: int,
    max_candidates_per_slice: int,
    max_total_runtime: float,
    validate_slice: Callable[[List[Dict[str, str]], int, float], Dict[str, object]],
    qualifies_for_progress: Callable[[Dict[str, str]], bool] | None = None,
) -> Dict[str, object]:
    aggregated_validated_rows: List[Dict[str, str]] = []
    slice_summaries: List[Dict[str, object]] = []
    candidate_state_counts: Counter[str] = Counter()
    planned_action_counts: Counter[str] = Counter()
    reject_reasons: Counter[str] = Counter()
    dead_end_count = 0
    runtime_used = 0.0
    cursor = 0
    slice_index = 0
    runtime_exhausted = False
    progress_gate = qualifies_for_progress or (lambda row: True)

    def qualified_progress_rows() -> List[Dict[str, str]]:
        return dedupe([row for row in aggregated_validated_rows if progress_gate(row)])

    while cursor < len(candidate_rows):
        verified_progress = len(qualified_progress_rows()[: max(1, target_verified)])
        if verified_progress >= target_verified:
            break
        if max_total_runtime > 0 and runtime_used >= max_total_runtime:
            runtime_exhausted = True
            break

        remaining_needed = max(1, target_verified - verified_progress)
        slice_limit = max_candidates_per_slice if max_candidates_per_slice > 0 else remaining_needed
        slice_size = min(len(candidate_rows) - cursor, max(1, min(slice_limit, remaining_needed)))
        slice_rows = candidate_rows[cursor : cursor + slice_size]
        slice_index += 1
        remaining_runtime = max(0.0, max_total_runtime - runtime_used) if max_total_runtime > 0 else 0.0
        slice_result = validate_slice(slice_rows, slice_index, remaining_runtime)
        slice_stats = dict(slice_result.get("stats", {}) or {})
        slice_validated_rows = list(slice_result.get("validated_rows", []) or [])

        aggregated_validated_rows.extend(slice_validated_rows)
        slice_summaries.append(
            {
                "slice_index": slice_index,
                "candidate_count": len(slice_rows),
                "kept_rows": len(slice_validated_rows),
                "batch_runtime_exceeded": bool(slice_stats.get("batch_runtime_exceeded", False)),
                "runtime_seconds": round(
                    float((slice_stats.get("stage_timings", {}) or {}).get("prefilter_seconds", 0.0) or 0.0)
                    + float((slice_stats.get("stage_timings", {}) or {}).get("verify_seconds", 0.0) or 0.0),
                    4,
                ),
                "stats_path": str(slice_result.get("stats_path", "") or ""),
            }
        )

        runtime_used += float((slice_stats.get("stage_timings", {}) or {}).get("prefilter_seconds", 0.0) or 0.0)
        runtime_used += float((slice_stats.get("stage_timings", {}) or {}).get("verify_seconds", 0.0) or 0.0)
        merge_counter_dict(candidate_state_counts, slice_stats.get("candidate_state_counts", {}))
        merge_counter_dict(planned_action_counts, slice_stats.get("planned_action_counts", {}))
        merge_counter_dict(reject_reasons, slice_stats.get("reject_reasons", {}))
        dead_end_count += int((slice_stats.get("candidate_state_counts", {}) or {}).get("dead_end", 0) or 0)

        cursor += len(slice_rows)
        if bool(slice_stats.get("batch_runtime_exceeded", False)):
            runtime_exhausted = True
            break

    verified_progress = len(qualified_progress_rows()[: max(1, target_verified)])
    return {
        "validated_rows": aggregated_validated_rows,
        "slice_summaries": slice_summaries,
        "processed_candidates": cursor,
        "candidates_replaced": max(0, cursor - verified_progress),
        "verified_target": target_verified,
        "verified_progress": verified_progress,
        "dead_end_count": dead_end_count,
        "exhausted_before_target": cursor >= len(candidate_rows) and verified_progress < target_verified,
        "runtime_exhausted": runtime_exhausted,
        "candidate_state_counts": dict(candidate_state_counts),
        "planned_action_counts": dict(planned_action_counts),
        "reject_reasons": dict(reject_reasons),
        "runtime_seconds": round(runtime_used, 4),
    }


def merge_per_domain_stats(target: Dict[str, Dict[str, object]], source: Dict[str, object]) -> None:
    for domain, stats in (source or {}).items():
        if not isinstance(stats, dict):
            continue
        merged = target.setdefault(
            str(domain),
            {
                "fetch_count": 0,
                "total_seconds": 0.0,
                "timeout_count": 0,
                "prefilter_seconds": 0.0,
                "verify_seconds": 0.0,
            },
        )
        merged["fetch_count"] = int(merged.get("fetch_count", 0) or 0) + int(stats.get("fetch_count", 0) or 0)
        merged["timeout_count"] = int(merged.get("timeout_count", 0) or 0) + int(stats.get("timeout_count", 0) or 0)
        merged["total_seconds"] = float(merged.get("total_seconds", 0.0) or 0.0) + float(
            stats.get("total_seconds", 0.0) or 0.0
        )
        merged["prefilter_seconds"] = float(merged.get("prefilter_seconds", 0.0) or 0.0) + float(
            stats.get("prefilter_seconds", 0.0) or 0.0
        )
        merged["verify_seconds"] = float(merged.get("verify_seconds", 0.0) or 0.0) + float(
            stats.get("verify_seconds", 0.0) or 0.0
        )


def summarize_merged_domain_budget_burn(per_domain: Dict[str, Dict[str, object]], limit: int = 10) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for domain, stats in per_domain.items():
        prefilter_seconds = float(stats.get("prefilter_seconds", 0.0) or 0.0)
        verify_seconds = float(stats.get("verify_seconds", 0.0) or 0.0)
        rows.append(
            {
                "candidate_domain": domain,
                "fetch_count": int(stats.get("fetch_count", 0) or 0),
                "total_seconds": round(float(stats.get("total_seconds", 0.0) or 0.0), 4),
                "timeout_count": int(stats.get("timeout_count", 0) or 0),
                "prefilter_seconds": round(prefilter_seconds, 4),
                "verify_seconds": round(verify_seconds, 4),
                "dominant_phase": "prefilter" if prefilter_seconds >= verify_seconds else "verify",
            }
        )
    rows.sort(key=lambda row: (float(row.get("total_seconds", 0.0) or 0.0), int(row.get("fetch_count", 0) or 0)), reverse=True)
    return rows[:limit]


def aggregate_validate_stats(
    *,
    slice_stats_list: List[Dict[str, object]],
    validated_rows: List[Dict[str, str]],
    validation_profile: str,
    domain_cache_path: Path,
    location_debug_path: Path,
    listing_debug_path: Path,
    near_miss_location_path: Path,
    orchestration_stats: Dict[str, object],
    ordered_candidate_rows: List[Dict[str, str]],
    scoring_context: Dict[str, object],
) -> Dict[str, object]:
    reject_reasons: Counter[str] = Counter()
    cache_hits: Counter[str] = Counter()
    location_decision_counts: Counter[str] = Counter()
    location_method_counts: Counter[str] = Counter()
    listing_reject_reason_counts: Counter[str] = Counter()
    candidate_state_counts: Counter[str] = Counter()
    planned_action_counts: Counter[str] = Counter()
    book_title_downgrade_reasons: Counter[str] = Counter()
    per_domain: Dict[str, Dict[str, object]] = {}
    top_candidate_budget_burn: List[Dict[str, object]] = []
    location_debug_samples: List[Dict[str, object]] = []
    listing_debug_samples: List[Dict[str, object]] = []
    proof_ledger_samples: List[Dict[str, object]] = []
    candidate_outcome_records: List[Dict[str, object]] = []
    stage_prefilter = 0.0
    stage_verify = 0.0
    total_candidates = 0
    prefilter_candidates = 0
    verify_candidates = 0
    batch_runtime_exceeded = bool(orchestration_stats.get("runtime_exhausted", False))

    for stats in slice_stats_list:
        merge_counter_dict(reject_reasons, stats.get("reject_reasons", {}))
        merge_counter_dict(cache_hits, stats.get("cache_hits", {}))
        merge_counter_dict(location_decision_counts, stats.get("location_decision_counts", {}))
        merge_counter_dict(location_method_counts, stats.get("location_method_counts", {}))
        merge_counter_dict(listing_reject_reason_counts, stats.get("listing_reject_reason_counts", {}))
        merge_counter_dict(candidate_state_counts, stats.get("candidate_state_counts", {}))
        merge_counter_dict(planned_action_counts, stats.get("planned_action_counts", {}))
        merge_counter_dict(book_title_downgrade_reasons, stats.get("book_title_downgrade_reasons", {}))
        merge_per_domain_stats(per_domain, stats.get("per_domain", {}))
        stage_prefilter += float((stats.get("stage_timings", {}) or {}).get("prefilter_seconds", 0.0) or 0.0)
        stage_verify += float((stats.get("stage_timings", {}) or {}).get("verify_seconds", 0.0) or 0.0)
        total_candidates += int(stats.get("total_candidates", 0) or 0)
        prefilter_candidates += int(stats.get("prefilter_candidates", 0) or 0)
        verify_candidates += int(stats.get("verify_candidates", 0) or 0)
        batch_runtime_exceeded = batch_runtime_exceeded or bool(stats.get("batch_runtime_exceeded", False))
        top_candidate_budget_burn.extend(list(stats.get("top_candidate_budget_burn", []) or []))
        if len(location_debug_samples) < 10:
            location_debug_samples.extend(list(stats.get("location_debug_samples", []) or [])[: 10 - len(location_debug_samples)])
        if len(listing_debug_samples) < 10:
            listing_debug_samples.extend(list(stats.get("listing_debug_samples", []) or [])[: 10 - len(listing_debug_samples)])
        if len(proof_ledger_samples) < 10:
            proof_ledger_samples.extend(list(stats.get("proof_ledger_samples", []) or [])[: 10 - len(proof_ledger_samples)])
        candidate_outcome_records.extend(list(stats.get("candidate_outcome_records", []) or []))

    top_candidate_budget_burn.sort(
        key=lambda row: (
            float(row.get("total_seconds", 0.0) or 0.0),
            int(row.get("fetch_count", 0) or 0),
        ),
        reverse=True,
    )

    listing_status_counts = Counter((row.get("ListingStatus", "") or "missing") for row in validated_rows)
    book_title_method_counts = Counter((row.get("BookTitleMethod", "") or "fallback") for row in validated_rows)
    book_title_confidence_counts = Counter((row.get("BookTitleConfidence", "") or "weak") for row in validated_rows)
    book_title_status_counts = Counter((row.get("BookTitleStatus", "") or "ok") for row in validated_rows)
    indie_proof_strength_counts = Counter((row.get("IndieProofStrength", "") or "missing") for row in validated_rows)
    recency_status_counts = Counter((row.get("RecencyStatus", "") or "missing") for row in validated_rows)
    email_quality_counts = Counter(
        (row.get("EmailQuality", "") or "missing") for row in validated_rows if (row.get("AuthorEmail", "") or "").strip()
    )

    fully_verified_rows = sum(
        1
        for row in validated_rows
        if row_is_fully_verified(row, require_us_location=profile_requires_us_location(validation_profile))
    )
    kept_rows = len(validated_rows)
    book_title_downgraded = sum(
        1 for row in validated_rows if (row.get("BookTitleStatus", "") or "ok").strip().lower() != "ok"
    )
    intake_scoring = summarize_candidate_intake_scores(
        ordered_candidate_rows=ordered_candidate_rows,
        scoring_context=scoring_context,
        candidate_outcome_records=candidate_outcome_records,
        orchestration_stats=orchestration_stats,
    )

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "validation_profile": validation_profile,
        "total_candidates": total_candidates,
        "kept_rows": kept_rows,
        "reject_reasons": dict(reject_reasons),
        "listing_status_counts": dict(listing_status_counts),
        "book_title_method_counts": dict(book_title_method_counts),
        "book_title_confidence_counts": dict(book_title_confidence_counts),
        "book_title_status_counts": dict(book_title_status_counts),
        "book_title_downgraded": book_title_downgraded,
        "book_title_downgrade_reasons": dict(book_title_downgrade_reasons),
        "indie_proof_strength_counts": dict(indie_proof_strength_counts),
        "recency_status_counts": dict(recency_status_counts),
        "email_quality_counts": dict(email_quality_counts),
        "leads_with_author_email": sum(1 for row in validated_rows if (row.get("AuthorEmail", "") or "").strip()),
        "fully_verified_rows": fully_verified_rows,
        "stage_timings": {
            "prefilter_seconds": round(stage_prefilter, 4),
            "verify_seconds": round(stage_verify, 4),
        },
        "prefilter_candidates": prefilter_candidates,
        "verify_candidates": verify_candidates,
        "batch_runtime_exceeded": batch_runtime_exceeded,
        "cache_hits": dict(cache_hits),
        "domain_cache_path": str(domain_cache_path),
        "location_debug_path": str(location_debug_path),
        "listing_debug_path": str(listing_debug_path),
        "near_miss_location_path": str(near_miss_location_path),
        "near_miss_location_rows": row_count(near_miss_location_path),
        "location_decision_counts": dict(location_decision_counts),
        "location_method_counts": dict(location_method_counts),
        "location_debug_samples": location_debug_samples[:10],
        "listing_reject_reason_counts": dict(listing_reject_reason_counts),
        "listing_debug_samples": listing_debug_samples[:10],
        "candidate_state_counts": dict(candidate_state_counts),
        "planned_action_counts": dict(planned_action_counts),
        "proof_ledger_samples": proof_ledger_samples[:10],
        "candidate_outcome_records": candidate_outcome_records,
        "candidate_outcome_records_count": len(candidate_outcome_records),
        "per_domain": per_domain,
        "candidate_budget_burn_count": sum(int(stats.get("candidate_budget_burn_count", 0) or 0) for stats in slice_stats_list),
        "top_domain_budget_burn": summarize_merged_domain_budget_burn(per_domain),
        "top_candidate_budget_burn": top_candidate_budget_burn[:10],
        "max_fetches_per_domain": max((int(stats.get("max_fetches_per_domain", 0) or 0) for stats in slice_stats_list), default=0),
        "max_seconds_per_domain": max((float(stats.get("max_seconds_per_domain", 0.0) or 0.0) for stats in slice_stats_list), default=0.0),
        "max_timeouts_per_domain": max((int(stats.get("max_timeouts_per_domain", 0) or 0) for stats in slice_stats_list), default=0),
        "max_total_runtime": max((float(stats.get("max_total_runtime", 0.0) or 0.0) for stats in slice_stats_list), default=0.0),
        "max_concurrency": max((int(stats.get("max_concurrency", 0) or 0) for stats in slice_stats_list), default=0),
        "verified_target": int(orchestration_stats.get("verified_target", 0) or 0),
        "verified_progress": int(orchestration_stats.get("verified_progress", 0) or 0),
        "candidates_replaced": int(orchestration_stats.get("candidates_replaced", 0) or 0),
        "dead_end_count": int(orchestration_stats.get("dead_end_count", 0) or 0),
        "exhausted_before_target": bool(orchestration_stats.get("exhausted_before_target", False)),
        "validator_slices": list(orchestration_stats.get("slice_summaries", []) or []),
        "intake_scoring": intake_scoring,
    }


def build_run_manifest_payload(
    *,
    run_tag: str,
    goal_target: int,
    validation_profile: str,
    pipeline_exit_code: int,
    counts: Dict[str, object],
    rejected_rows: List[Dict[str, str]],
    artifact_paths: Dict[str, Path],
    loop_control: Dict[str, object] | None = None,
) -> Dict[str, object]:
    return {
        "run_id": run_tag,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "validation_profile": validation_profile,
        "status": "failed" if pipeline_exit_code != 0 else "completed",
        "pipeline_exit_code": int(pipeline_exit_code),
        "target_final": int(goal_target),
        "ruleset_version": "leadfinder_rules_v1",
        "counts": {
            key: int(value) if isinstance(value, bool) is False and isinstance(value, int) else value
            for key, value in counts.items()
        },
        "rejected_reason_counts": summarize_rejected_reason_counts(rejected_rows),
        "artifacts": {key: str(path) for key, path in artifact_paths.items()},
        "loop_control": dict(loop_control or {}),
    }


def main() -> int:
    args = parse_args()
    apply_validation_profile_defaults(args)
    ensure_runtime_dirs()
    py = sys.executable
    validation_profile = normalize_validation_profile(getattr(args, "validation_profile", "default"))
    goal_target = max(1, int(args.goal_final or args.goal_total))
    runs_dir = Path(args.runs_dir)
    runs_dir.mkdir(parents=True, exist_ok=True)

    master_path = Path(args.master_output)
    contact_queue_path = Path(args.contact_queue_output)
    near_miss_location_path = Path(args.near_miss_location_output)
    csv_outputs_dir = master_path.parent if str(master_path.parent) != "." else Path(csv_output("all_prospects.csv")).parent
    json_outputs_dir = runs_dir.parent / "json" if str(runs_dir.parent) not in {"", "."} else Path(json_output("run_manifest.json")).parent
    csv_outputs_dir.mkdir(parents=True, exist_ok=True)
    json_outputs_dir.mkdir(parents=True, exist_ok=True)
    persistent_dedupe_store = None if args.disable_persistent_dedupe else PersistentDedupeStore(Path(args.dedupe_db))
    master_rows = dedupe(read_rows(master_path))
    queue_rows = dedupe(read_rows(contact_queue_path)) if (not args.no_contact_queue_output and contact_queue_path.exists()) else []
    if persistent_dedupe_store is not None:
        persistent_dedupe_store.remember_rows(master_rows, run_id="existing_master")
        persistent_dedupe_store.remember_rows(queue_rows, run_id="existing_queue")
    near_miss_location_rows = (
        read_rows(near_miss_location_path)
        if (not args.no_near_miss_location_output and near_miss_location_path.exists())
        else []
    )
    stale_runs = 0
    reliability_counters = {
        "consecutive_failures": 0,
        "consecutive_empty_candidate_runs": 0,
        "consecutive_zero_validated_runs": 0,
    }
    last_run_tag = ""
    last_pipeline_exit_code = 0
    final_stop_reason = ""
    if master_rows:
        starting_total = count_goal_rows(master_rows, validation_profile=validation_profile, policy=args.merge_policy)
        print(f"[INFO] starting with {starting_total} existing goal-qualified leads in {master_path}")
    else:
        starting_total = 0
    agent_hunt_listing_feedback: Dict[str, object] = {}
    agent_hunt_source_quality_feedback: Dict[str, object] = {}
    write_live_status(
        status="running",
        stage="starting",
        validation_profile=validation_profile,
        goal_target=goal_target,
        active=True,
        batch_index=0,
        max_runs=max(1, args.max_runs),
        counts={
            "goal_qualified_total": starting_total,
            "master_total": len(master_rows),
            "queue_total": len(queue_rows),
        },
        message="Lead finder loop starting",
        runs_dir=runs_dir,
    )

    for run_idx in range(1, max(1, args.max_runs) + 1):
        current_total = count_goal_rows(master_rows, validation_profile=validation_profile, policy=args.merge_policy)
        if current_total >= goal_target:
            break

        run_tag = f"run_{run_idx:03d}"
        last_run_tag = run_tag
        validated_export_path = csv_outputs_dir / f"validated_{run_tag}.csv"
        rejected_export_path = csv_outputs_dir / f"rejected_{run_tag}.csv"
        run_manifest_path = json_outputs_dir / f"run_manifest_{run_tag}.json"
        candidates_path = runs_dir / f"{run_tag}_candidates.csv"
        filtered_candidates_path = runs_dir / f"{run_tag}_candidates.filtered.csv"
        validated_path = runs_dir / f"{run_tag}_validated.csv"
        final_path = runs_dir / f"{run_tag}_final.csv"
        new_leads_path = runs_dir / f"{run_tag}_new_leads.csv"
        tier_1_path = runs_dir / f"{run_tag}_tier_1_safest.csv"
        tier_2_path = runs_dir / f"{run_tag}_tier_2_usable_with_caution.csv"
        tier_3_path = runs_dir / f"{run_tag}_tier_3_weak_but_usable.csv"
        verified_path = runs_dir / f"{run_tag}_verified.csv"
        run_queries_file = runs_dir / f"{run_tag}_queries.txt"
        harvest_stats_path = runs_dir / f"{run_tag}_harvest_stats.json"
        validate_stats_path = runs_dir / f"{run_tag}_validate_stats.json"
        validate_domain_cache_path = derive_validate_debug_path(validate_stats_path, "_domain_cache.jsonl")
        validate_location_debug_path = derive_validate_debug_path(validate_stats_path, "_location_debug.jsonl")
        validate_listing_debug_path = derive_validate_debug_path(validate_stats_path, "_listing_debug.jsonl")
        run_stats_path = runs_dir / f"{run_tag}_stats.json"
        pipeline_stdout_path = runs_dir / f"{run_tag}_pipeline.stdout.log"
        pipeline_stderr_path = runs_dir / f"{run_tag}_pipeline.stderr.log"
        verify_stdout_path = runs_dir / f"{run_tag}_verify.stdout.log"
        verify_stderr_path = runs_dir / f"{run_tag}_verify.stderr.log"
        run_near_miss_location_path = runs_dir / f"{run_tag}_near_miss_location.csv"
        rejected_rows: List[Dict[str, str]] = []
        duplicate_count = 0

        # Prevent stale per-run artifacts from being reused across invocations.
        for path in (
            validated_export_path,
            rejected_export_path,
            run_manifest_path,
            candidates_path,
            filtered_candidates_path,
            validated_path,
            final_path,
            new_leads_path,
            tier_1_path,
            tier_2_path,
            tier_3_path,
            verified_path,
            run_queries_file,
            harvest_stats_path,
            validate_stats_path,
            validate_domain_cache_path,
            validate_location_debug_path,
            validate_listing_debug_path,
            run_stats_path,
            pipeline_stdout_path,
            pipeline_stderr_path,
            verify_stdout_path,
            verify_stderr_path,
            run_near_miss_location_path,
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

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
            str(max(0, run_idx - 1)),
            "--search-timeout",
            str(max(1.0, args.search_timeout)),
            "--goodreads-timeout",
            str(max(1.0, args.goodreads_timeout)),
            "--http-retries",
            str(max(0, args.harvest_http_retries)),
            "--harvest-time-budget",
            str(max(10.0, args.harvest_time_budget)),
            "--output",
            str(candidates_path),
            "--stats-output",
            str(harvest_stats_path),
        ]
        validate_cmd_common = [
            "--max-candidates",
            "0",
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
            "--delay",
            str(max(0.0, args.delay)),
            "--timeout",
            str(max(1.0, args.timeout)),
            "--min-year",
            str(args.min_year),
            "--max-year",
            str(args.max_year),
            "--validation-profile",
            validation_profile,
            "--location-recovery-mode",
            args.location_recovery_mode,
            "--location-recovery-pages",
            str(max(0, args.location_recovery_pages)),
        ]
        dedupe_cmd = [
            py,
            "prospect_dedupe.py",
            "--min-final",
            str(max(0, args.batch_min)),
            "--max-final",
            str(max(1, args.batch_max)),
            "--input",
            str(validated_path),
            "--output",
            str(final_path),
        ]
        effective_queries = args.queries_file
        if not effective_queries and not args.disable_auto_queries:
            rotated = build_rotating_queries(run_idx, stale_runs=stale_runs if is_agent_hunt_profile(validation_profile) else 0)
            write_queries_file(run_queries_file, rotated)
            effective_queries = str(run_queries_file)
            print(f"[INFO] batch {run_idx}: using {len(rotated)} rotated queries")
        if effective_queries:
            harvest_cmd.extend(["--queries-file", effective_queries])
        if args.brave_api_key:
            harvest_cmd.extend(["--brave-api-key", args.brave_api_key])
        if args.google_api_key:
            harvest_cmd.extend(["--google-api-key", args.google_api_key])
        if args.google_cx:
            harvest_cmd.extend(["--google-cx", args.google_cx])
        if args.disable_goodreads_outbound:
            harvest_cmd.append("--disable-goodreads-outbound")
        if args.require_email:
            validate_cmd_common.append("--require-email")
        if args.require_location_proof:
            validate_cmd_common.append("--require-location-proof")
        if args.us_only:
            validate_cmd_common.append("--us-only")
        effective_require_contact_path = args.require_contact_path and stale_runs < 3
        if args.require_contact_path and not effective_require_contact_path:
            print(f"[INFO] batch {run_idx}: auto-relaxing --require-contact-path after stale runs")
        if effective_require_contact_path:
            validate_cmd_common.append("--require-contact-path")
        if args.ignore_robots:
            validate_cmd_common.append("--ignore-robots")
        if args.robots_retry_seconds != 300.0:
            validate_cmd_common.extend(["--robots-retry-seconds", str(max(10.0, args.robots_retry_seconds))])
        if args.contact_path_strict:
            validate_cmd_common.append("--contact-path-strict")
        if args.listing_strict:
            validate_cmd_common.append("--listing-strict")

        print(f"[INFO] batch {run_idx}/{args.max_runs}: running pipeline")
        write_live_status(
            status="running",
            stage="harvest_starting",
            validation_profile=validation_profile,
            goal_target=goal_target,
            active=True,
            run_tag=run_tag,
            batch_index=run_idx,
            max_runs=max(1, args.max_runs),
            counts={
                "goal_qualified_total": current_total,
                "master_total": len(master_rows),
                "queue_total": len(queue_rows),
            },
            message="Harvest starting",
            runs_dir=runs_dir,
            log_path=pipeline_stdout_path,
        )
        suppression_stats: Dict[str, object] = {
            "suppressed_pre_validate_total": 0,
            "suppressed_pre_validate_by_reason": {},
        }
        harvest_stats: Dict[str, object] = {}
        append_log_line(pipeline_stdout_path, f"[RUN] {format_logged_command(harvest_cmd)}")
        harvest_result = run_logged_command(
            harvest_cmd,
            stdout_path=pipeline_stdout_path,
            stderr_path=pipeline_stderr_path,
            append=True,
        )
        pipeline_exit_code = harvest_result.returncode
        harvest_stats = load_json(harvest_stats_path)
        validate_slice_stats_list: List[Dict[str, object]] = []
        validation_orchestration_stats: Dict[str, object] = {
            "validated_rows": [],
            "slice_summaries": [],
            "processed_candidates": 0,
            "candidates_replaced": 0,
            "verified_target": 0,
            "verified_progress": 0,
            "dead_end_count": 0,
            "exhausted_before_target": False,
            "runtime_exhausted": False,
            "candidate_state_counts": {},
            "planned_action_counts": {},
            "reject_reasons": {},
            "runtime_seconds": 0.0,
        }
        candidate_scoring_context: Dict[str, object] = {
            "enabled": False,
            "distribution": {},
            "avg_score": 0.0,
            "max_score": 0,
            "min_score": 0,
            "score_records": [],
            "score_map": {},
            "score_map_by_url": {},
            "high_score_threshold": INTAKE_SCORE_HIGH_THRESHOLD,
            "low_score_threshold": INTAKE_SCORE_LOW_THRESHOLD,
            "top_candidates": [],
        }
        if pipeline_exit_code == 0:
            harvested_candidate_rows = read_candidate_rows(candidates_path)
            filtered_candidate_rows, suppression_stats = suppress_pre_validate_candidates(
                harvested_candidate_rows,
                master_rows,
            )
            filtered_candidate_rows, candidate_scoring_context = order_candidates_for_strict_validation(
                filtered_candidate_rows,
                validation_profile=validation_profile,
                agent_hunt_listing_feedback=agent_hunt_listing_feedback if is_agent_hunt_profile(validation_profile) else None,
                agent_hunt_source_quality_feedback=agent_hunt_source_quality_feedback if is_agent_hunt_profile(validation_profile) else None,
            )
            if is_agent_hunt_profile(validation_profile):
                filtered_candidate_rows, source_quality_suppression_stats = suppress_agent_hunt_source_quality_candidates(
                    filtered_candidate_rows,
                    scoring_context=candidate_scoring_context,
                )
                suppression_stats.update(source_quality_suppression_stats)
                filtered_candidate_rows, candidate_scoring_context = order_candidates_for_strict_validation(
                    filtered_candidate_rows,
                    validation_profile=validation_profile,
                    agent_hunt_listing_feedback=agent_hunt_listing_feedback,
                    agent_hunt_source_quality_feedback=agent_hunt_source_quality_feedback,
                )
                filtered_candidate_rows, epic_directory_throttle_stats = throttle_agent_hunt_epic_directory_candidates(
                    filtered_candidate_rows,
                    scoring_context=candidate_scoring_context,
                )
                suppression_stats.update(epic_directory_throttle_stats)
                filtered_candidate_rows, candidate_scoring_context = order_candidates_for_strict_validation(
                    filtered_candidate_rows,
                    validation_profile=validation_profile,
                    agent_hunt_listing_feedback=agent_hunt_listing_feedback,
                    agent_hunt_source_quality_feedback=agent_hunt_source_quality_feedback,
                )
            write_candidate_rows(filtered_candidates_path, filtered_candidate_rows)
            suppression_message = (
                f"[INFO] batch {run_idx}: suppressed {suppression_stats['suppressed_pre_validate_total']} "
                f"pre-validate candidates, kept {len(filtered_candidate_rows)} of {len(harvested_candidate_rows)}"
            )
            print(suppression_message)
            append_log_line(pipeline_stdout_path, suppression_message)
            if candidate_scoring_context.get("enabled"):
                scoring_message = (
                    f"[INFO] batch {run_idx}: intake scoring avg {candidate_scoring_context['avg_score']}, "
                    f"top band {candidate_scoring_context['distribution']}"
                )
                print(scoring_message)
                append_log_line(pipeline_stdout_path, scoring_message)
            write_live_status(
                status="running",
                stage="harvest_complete",
                validation_profile=validation_profile,
                goal_target=goal_target,
                active=True,
                run_tag=run_tag,
                batch_index=run_idx,
                max_runs=max(1, args.max_runs),
                counts={
                    "goal_qualified_total": current_total,
                    "harvested_candidates": len(harvested_candidate_rows),
                    "filtered_candidates": len(filtered_candidate_rows),
                    "suppressed_pre_validate_total": suppression_stats.get("suppressed_pre_validate_total", 0),
                },
                pipeline_exit_code=pipeline_exit_code,
                message="Harvest complete",
                runs_dir=runs_dir,
                log_path=pipeline_stdout_path,
                artifact_paths={
                    "candidates_csv": candidates_path,
                    "filtered_candidates_csv": filtered_candidates_path,
                    "harvest_stats_json": harvest_stats_path,
                },
            )
            batch_verified_target = compute_batch_target_count(
                batch_max=max(1, args.batch_max),
                goal_target=goal_target,
                current_total=current_total,
            )
            slice_location_debug_paths: List[Path] = []
            slice_listing_debug_paths: List[Path] = []
            slice_near_miss_paths: List[Path] = []

            def run_validate_slice(
                slice_rows: List[Dict[str, str]],
                slice_index: int,
                remaining_runtime: float,
            ) -> Dict[str, object]:
                slice_tag = f"{run_tag}_validate_slice_{slice_index:03d}"
                slice_candidates_path = runs_dir / f"{slice_tag}_candidates.csv"
                slice_validated_path = runs_dir / f"{slice_tag}_validated.csv"
                slice_stats_path = runs_dir / f"{slice_tag}_stats.json"
                slice_near_miss_path = runs_dir / f"{slice_tag}_near_miss_location.csv"
                slice_location_debug_path = derive_validate_debug_path(slice_stats_path, "_location_debug.jsonl")
                slice_listing_debug_path = derive_validate_debug_path(slice_stats_path, "_listing_debug.jsonl")
                for path in (
                    slice_candidates_path,
                    slice_validated_path,
                    slice_stats_path,
                    slice_near_miss_path,
                    slice_location_debug_path,
                    slice_listing_debug_path,
                ):
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass
                write_candidate_rows(slice_candidates_path, slice_rows)
                slice_validate_cmd = [
                    py,
                    "prospect_validate.py",
                    "--input",
                    str(slice_candidates_path),
                    "--output",
                    str(slice_validated_path),
                    "--stats-output",
                    str(slice_stats_path),
                    "--domain-cache-path",
                    str(validate_domain_cache_path),
                    *validate_cmd_common,
                    "--max-total-runtime",
                    str(max(0.0, remaining_runtime)),
                ]
                if not args.no_near_miss_location_output:
                    slice_validate_cmd.extend(["--near-miss-location-output", str(slice_near_miss_path)])
                append_log_line(
                    pipeline_stdout_path,
                    f"[RUN] {format_logged_command(slice_validate_cmd)}",
                )
                validate_result = run_logged_command(
                    slice_validate_cmd,
                    stdout_path=pipeline_stdout_path,
                    stderr_path=pipeline_stderr_path,
                    append=True,
                )
                if validate_result.returncode != 0:
                    raise subprocess.CalledProcessError(validate_result.returncode, slice_validate_cmd)
                slice_stats = load_json(slice_stats_path)
                validate_slice_stats_list.append(slice_stats)
                if slice_location_debug_path.exists():
                    slice_location_debug_paths.append(slice_location_debug_path)
                if slice_listing_debug_path.exists():
                    slice_listing_debug_paths.append(slice_listing_debug_path)
                if not args.no_near_miss_location_output and slice_near_miss_path.exists():
                    slice_near_miss_paths.append(slice_near_miss_path)
                return {
                    "validated_rows": read_rows(slice_validated_path),
                    "stats": slice_stats,
                    "stats_path": slice_stats_path,
                }

            try:
                if is_agent_hunt_profile(validation_profile):
                    progress_qualifier = row_is_agent_hunt_qualified
                elif is_fully_verified_profile(validation_profile):
                    progress_qualifier = (
                        lambda row: row_is_fully_verified(
                            row,
                            require_us_location=profile_requires_us_location(validation_profile),
                        )
                    )
                else:
                    progress_qualifier = None
                validation_orchestration_stats = orchestrate_candidate_replacements(
                    filtered_candidate_rows,
                    target_verified=max(1, batch_verified_target),
                    max_candidates_per_slice=max(0, args.max_candidates),
                    max_total_runtime=max(0.0, args.max_total_runtime),
                    validate_slice=run_validate_slice,
                    qualifies_for_progress=progress_qualifier,
                )
            except subprocess.CalledProcessError as exc:
                pipeline_exit_code = exc.returncode
            else:
                aggregated_validated_rows = list(validation_orchestration_stats.get("validated_rows", []) or [])
                write_rows(validated_path, aggregated_validated_rows)
                concatenate_jsonl_files(slice_location_debug_paths, validate_location_debug_path)
                concatenate_jsonl_files(slice_listing_debug_paths, validate_listing_debug_path)
                if not args.no_near_miss_location_output:
                    aggregated_near_miss_rows: List[Dict[str, str]] = []
                    for slice_near_miss_path in slice_near_miss_paths:
                        aggregated_near_miss_rows.extend(read_rows(slice_near_miss_path))
                    write_near_miss_location_rows(run_near_miss_location_path, aggregated_near_miss_rows)
                aggregate_stats = aggregate_validate_stats(
                    slice_stats_list=validate_slice_stats_list,
                    validated_rows=aggregated_validated_rows,
                    validation_profile=validation_profile,
                    domain_cache_path=validate_domain_cache_path,
                    location_debug_path=validate_location_debug_path,
                    listing_debug_path=validate_listing_debug_path,
                    near_miss_location_path=run_near_miss_location_path,
                    orchestration_stats=validation_orchestration_stats,
                    ordered_candidate_rows=filtered_candidate_rows,
                    scoring_context=candidate_scoring_context,
                )
                write_run_stats(validate_stats_path, aggregate_stats)
                write_rows(validated_export_path, aggregated_validated_rows)
                rejected_rows = build_rejected_rows(
                    list(aggregate_stats.get("candidate_outcome_records", []) or []),
                    run_tag,
                )
                duplicate_count = sum(1 for row in rejected_rows if "duplicate_lead" in (row.get("FailReasons", "") or "").split("|"))
                write_rejected_rows(rejected_export_path, rejected_rows)
                append_log_line(
                    pipeline_stdout_path,
                    (
                        f"[INFO] validation orchestration: processed "
                        f"{validation_orchestration_stats['processed_candidates']} candidates, "
                        f"verified {validation_orchestration_stats['verified_progress']}/"
                        f"{validation_orchestration_stats['verified_target']}, "
                        f"replaced {validation_orchestration_stats['candidates_replaced']}, "
                        f"dead_end {validation_orchestration_stats['dead_end_count']}"
                    ),
                )
                write_live_status(
                    status="running",
                    stage="validation_complete",
                    validation_profile=validation_profile,
                    goal_target=goal_target,
                    active=True,
                    run_tag=run_tag,
                    batch_index=run_idx,
                    max_runs=max(1, args.max_runs),
                    counts={
                        "goal_qualified_total": current_total,
                        "harvested_candidates": row_count(candidates_path),
                        "filtered_candidates": row_count(filtered_candidates_path),
                        "validated": row_count(validated_export_path),
                        "final": row_count(final_path),
                        "rejected": len(rejected_rows),
                        "duplicates": duplicate_count,
                    },
                    pipeline_exit_code=pipeline_exit_code,
                    message="Validation complete",
                    runs_dir=runs_dir,
                    log_path=pipeline_stdout_path,
                    artifact_paths={
                        "validated_csv": validated_export_path,
                        "rejected_csv": rejected_export_path,
                        "validate_stats_json": validate_stats_path,
                    },
                )
                append_log_line(pipeline_stdout_path, f"[RUN] {format_logged_command(dedupe_cmd)}")
                dedupe_result = run_logged_command(
                    dedupe_cmd,
                    stdout_path=pipeline_stdout_path,
                    stderr_path=pipeline_stderr_path,
                    append=True,
                )
                pipeline_exit_code = dedupe_result.returncode

        if pipeline_exit_code != 0:
            print(f"[WARN] batch {run_idx} failed with exit code {pipeline_exit_code}")
            validator_stats_failed = load_json(validate_stats_path)
            if not validated_export_path.exists():
                write_rows(validated_export_path, read_rows(validated_path))
            if not rejected_rows:
                rejected_rows = build_rejected_rows(
                    list(validator_stats_failed.get("candidate_outcome_records", []) or []),
                    run_tag,
                )
                duplicate_count = sum(
                    1 for row in rejected_rows if "duplicate_lead" in (row.get("FailReasons", "") or "").split("|")
                )
            write_rejected_rows(rejected_export_path, rejected_rows)
            reliability_counters = update_reliability_counters(
                reliability_counters,
                pipeline_failed=True,
                filtered_candidates=row_count(filtered_candidates_path),
                validated_rows=row_count(validated_export_path),
            )
            stop_reason = determine_loop_stop_reason(
                args,
                stale_runs=stale_runs,
                reliability_counters=reliability_counters,
            )
            run_stats = {
                "run_tag": run_tag,
                "batch_index": run_idx,
                "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "pipeline_exit_code": pipeline_exit_code,
                "pipeline_stdout_log": str(pipeline_stdout_path),
                "pipeline_stderr_log": str(pipeline_stderr_path),
                "validation_profile": validation_profile,
                "counts": {
                    "harvested_candidates": row_count(candidates_path),
                    "candidates": row_count(filtered_candidates_path),
                    "suppressed_pre_validate_total": suppression_stats.get("suppressed_pre_validate_total", 0),
                    "duplicate_count": duplicate_count,
                    "near_miss_location_rows": row_count(run_near_miss_location_path),
                    "verified_target": int(validation_orchestration_stats.get("verified_target", 0) or 0),
                    "verified_progress": int(validation_orchestration_stats.get("verified_progress", 0) or 0),
                    "candidates_replaced": int(validation_orchestration_stats.get("candidates_replaced", 0) or 0),
                    "dead_end_count": int(validation_orchestration_stats.get("dead_end_count", 0) or 0),
                    "exhausted_before_target": bool(validation_orchestration_stats.get("exhausted_before_target", False)),
                },
                "near_miss_location_output": str(run_near_miss_location_path),
                "harvested_candidates": analyze_candidates(candidates_path),
                "candidates": analyze_candidates(filtered_candidates_path),
                "harvest": harvest_stats,
                "pre_validate_suppression": suppression_stats,
                "validator": load_json(validate_stats_path),
                "loop_control": {
                    "stale_runs": stale_runs,
                    **reliability_counters,
                    "stop_reason": stop_reason,
                },
            }
            if harvest_stats:
                run_stats["google_cse_status"] = harvest_stats.get("google_cse_status", "")
            write_run_stats(run_stats_path, run_stats)
            write_run_manifest(
                run_manifest_path,
                build_run_manifest_payload(
                    run_tag=run_tag,
                    goal_target=goal_target,
                    validation_profile=validation_profile,
                    pipeline_exit_code=pipeline_exit_code,
                    counts={
                        "harvested_candidates": row_count(candidates_path),
                        "filtered_candidates": row_count(filtered_candidates_path),
                        "validated": row_count(validated_export_path),
                        "rejected": len(rejected_rows),
                        "duplicates": duplicate_count,
                        "near_miss_location_rows": row_count(run_near_miss_location_path),
                    },
                    rejected_rows=rejected_rows,
                    artifact_paths={
                        "candidates_csv": candidates_path,
                        "filtered_candidates_csv": filtered_candidates_path,
                        "validated_csv": validated_export_path,
                        "rejected_csv": rejected_export_path,
                        "run_stats_json": run_stats_path,
                        "validate_stats_json": validate_stats_path,
                    },
                    loop_control={
                        "stale_runs": stale_runs,
                        **reliability_counters,
                        "stop_reason": stop_reason,
                    },
                ),
            )
            last_pipeline_exit_code = pipeline_exit_code
            if stop_reason:
                final_stop_reason = stop_reason
                write_live_status(
                    status="stopped",
                    stage="stopped",
                    validation_profile=validation_profile,
                    goal_target=goal_target,
                    active=False,
                    run_tag=run_tag,
                    batch_index=run_idx,
                    max_runs=max(1, args.max_runs),
                    counts={
                        "goal_qualified_total": current_total,
                        "harvested_candidates": row_count(candidates_path),
                        "filtered_candidates": row_count(filtered_candidates_path),
                        "validated": row_count(validated_export_path),
                        "rejected": len(rejected_rows),
                        "duplicates": duplicate_count,
                    },
                    pipeline_exit_code=pipeline_exit_code,
                    stop_reason=stop_reason,
                    message="Loop stopped after failed batch",
                    runs_dir=runs_dir,
                    log_path=pipeline_stdout_path,
                    artifact_paths={"run_manifest_json": run_manifest_path},
                )
                print(f"[INFO] stopping early after {stop_reason.replace('_', ' ')}.")
            else:
                write_live_status(
                    status="running",
                    stage="batch_failed",
                    validation_profile=validation_profile,
                    goal_target=goal_target,
                    active=True,
                    run_tag=run_tag,
                    batch_index=run_idx,
                    max_runs=max(1, args.max_runs),
                    counts={
                        "goal_qualified_total": current_total,
                        "harvested_candidates": row_count(candidates_path),
                        "filtered_candidates": row_count(filtered_candidates_path),
                        "validated": row_count(validated_export_path),
                        "rejected": len(rejected_rows),
                        "duplicates": duplicate_count,
                    },
                    pipeline_exit_code=pipeline_exit_code,
                    message="Batch failed; loop continuing",
                    runs_dir=runs_dir,
                    log_path=pipeline_stdout_path,
                    artifact_paths={"run_manifest_json": run_manifest_path},
                )
            continue

        if not args.no_near_miss_location_output:
            near_miss_location_rows.extend(read_rows(run_near_miss_location_path))

        pre_verify_rows = read_rows(final_path)
        batch_rows = list(pre_verify_rows)
        if args.require_email and not args.skip_email_verify:
            if not batch_rows:
                print(f"[INFO] batch {run_idx}: no rows to verify; skipping email gate")
            else:
                verify_cmd = [
                    py,
                    "verify_emails.py",
                    "--input",
                    str(final_path),
                    "--output",
                    str(verified_path),
                    "--timeout",
                    str(max(1.0, args.timeout)),
                ]
                print(f"[INFO] batch {run_idx}: verifying email deliverability before merge")
                verify_result = run_logged_command(
                    verify_cmd,
                    stdout_path=verify_stdout_path,
                    stderr_path=verify_stderr_path,
                )
                if verify_result.returncode != 0:
                    print(f"[WARN] email verification failed for batch {run_idx}; skipping batch merge")
                    continue
                verified_rows = read_rows(verified_path)
                batch_rows = keep_verified_rows(verified_rows, gate=args.email_gate)
        if args.require_location:
            batch_rows = [
                row
                for row in batch_rows
                if normalize_location(row.get("Location", "")) not in ("", "unknown", "n/a", "na")
            ]
        if persistent_dedupe_store is not None and batch_rows:
            batch_rows, duplicate_rows = persistent_dedupe_store.filter_new_rows(batch_rows, run_id=run_tag)
            duplicate_count += len(duplicate_rows)
            if duplicate_rows:
                rejected_rows.extend(build_duplicate_rejected_rows(duplicate_rows, run_tag))
                write_rejected_rows(rejected_export_path, rejected_rows)
        existing_master_rows = list(master_rows)
        master_batch_rows, queue_only_batch_rows = split_rows_by_merge_policy(
            batch_rows,
            policy=args.merge_policy,
            validation_profile=validation_profile,
        )
        verified_output_count = 0
        scouted_output_count = 0
        agent_hunt_stats: Dict[str, object] = {}
        candidate_outcome_records = [
            record
            for stats in validate_slice_stats_list
            for record in list((stats.get("candidate_outcome_records", []) or []))
        ]
        if is_agent_hunt_profile(validation_profile):
            candidate_outcome_records = recover_agent_hunt_listing_friction_emails(candidate_outcome_records)
            agent_hunt_stats = build_agent_hunt_stats(
                validated_rows=batch_rows,
                validator_reject_reasons=validation_orchestration_stats.get("reject_reasons", {}),
                target=max(1, goal_target),
                existing_master_rows=existing_master_rows,
                candidate_outcome_records=candidate_outcome_records,
            )
            rescued_listing_rows = list(agent_hunt_stats.get("listing_blocked_scout_rows", []) or [])
            if rescued_listing_rows:
                master_batch_rows = dedupe(master_batch_rows + rescued_listing_rows)
            caution_listing_rows = list(agent_hunt_stats.get("listing_blocked_caution_rows", []) or [])
            if caution_listing_rows:
                queue_only_batch_rows = dedupe(queue_only_batch_rows + caution_listing_rows)
        before = count_goal_rows(existing_master_rows, validation_profile=validation_profile, policy=args.merge_policy)
        master_rows = dedupe(master_rows + master_batch_rows)
        after = count_goal_rows(master_rows, validation_profile=validation_profile, policy=args.merge_policy)
        added = after - before
        queue_before = len(queue_rows)
        queue_rows = dedupe(queue_rows + queue_only_batch_rows)
        added_to_queue = len(queue_rows) - queue_before
        if added == 0:
            stale_runs += 1
        else:
            stale_runs = 0
        reliability_counters = update_reliability_counters(
            reliability_counters,
            pipeline_failed=False,
            filtered_candidates=row_count(filtered_candidates_path),
            validated_rows=row_count(validated_export_path),
        )
        stop_reason = determine_loop_stop_reason(
            args,
            stale_runs=stale_runs,
            reliability_counters=reliability_counters,
        )
        write_rows(master_path, master_rows)
        if not args.no_minimal_output:
            minimal_count = write_minimal_rows(
                Path(args.minimal_output),
                master_rows,
                with_header=args.minimal_with_header,
            )
            print(f"[INFO] wrote {minimal_count} rows -> {args.minimal_output}")
        if is_agent_hunt_profile(validation_profile):
            agent_hunt_listing_feedback = build_agent_hunt_listing_feedback(
                candidate_outcome_records,
                rescued_candidate_identities=set(agent_hunt_stats.get("rescued_candidate_identities", []) or []),
            )
            agent_hunt_source_quality_feedback = build_agent_hunt_source_quality_feedback(candidate_outcome_records)
            scouted_rows = list(agent_hunt_stats.get("scouted_rows", []) or [])
            strict_rows = list(agent_hunt_stats.get("strict_rows", []) or [])
            existing_export_rows = build_projected_lead_export_rows(
                [row for row in existing_master_rows if row_is_agent_hunt_qualified(row)],
                source_url_getter=best_scout_source_url,
            )
            current_export_rows = build_projected_lead_export_rows(
                scouted_rows,
                source_url_getter=best_scout_source_url,
            )
            new_export_rows = build_new_lead_export_rows(current_export_rows, existing_export_rows)
            scouted_rows_by_tier = dict(agent_hunt_stats.get("scouted_rows_by_tier", {}) or {})
            write_projected_lead_export_rows(
                tier_1_path,
                build_projected_lead_export_rows(
                    list(scouted_rows_by_tier.get(AGENT_HUNT_TIER_1, []) or []),
                    source_url_getter=best_scout_source_url,
                ),
            )
            write_projected_lead_export_rows(
                tier_2_path,
                build_projected_lead_export_rows(
                    list(scouted_rows_by_tier.get(AGENT_HUNT_TIER_2, []) or []),
                    source_url_getter=best_scout_source_url,
                ),
            )
            write_projected_lead_export_rows(
                tier_3_path,
                build_projected_lead_export_rows(
                    list(scouted_rows_by_tier.get(AGENT_HUNT_TIER_3, []) or []),
                    source_url_getter=best_scout_source_url,
                ),
            )
            if not args.no_scouted_output:
                scouted_output_count = write_scouted_rows(Path(args.scouted_output), scouted_rows)
                print(f"[INFO] wrote {scouted_output_count} rows -> {args.scouted_output}")
            if not args.no_verified_output:
                verified_output_count = write_verified_rows(Path(args.verified_output), strict_rows)
                print(f"[INFO] wrote {verified_output_count} rows -> {args.verified_output}")
        elif is_fully_verified_profile(validation_profile) and not args.no_verified_output:
            verified_rows = [
                row for row in master_rows if row_is_fully_verified(row, require_us_location=profile_requires_us_location(validation_profile))
            ]
            existing_export_rows = build_projected_lead_export_rows(
                [
                    row
                    for row in existing_master_rows
                    if row_is_fully_verified(row, require_us_location=profile_requires_us_location(validation_profile))
                ],
                source_url_getter=best_verified_source_url,
            )
            current_export_rows = build_projected_lead_export_rows(
                verified_rows,
                source_url_getter=best_verified_source_url,
            )
            new_export_rows = build_new_lead_export_rows(current_export_rows, existing_export_rows)
            verified_output_count = write_verified_rows(Path(args.verified_output), verified_rows)
            print(f"[INFO] wrote {verified_output_count} rows -> {args.verified_output}")
        else:
            existing_export_rows = build_projected_lead_export_rows(
                existing_master_rows,
                source_url_getter=best_verified_source_url,
            )
            current_export_rows = build_projected_lead_export_rows(
                master_rows,
                source_url_getter=best_verified_source_url,
            )
            new_export_rows = build_new_lead_export_rows(current_export_rows, existing_export_rows)
        new_leads_count = write_projected_lead_export_rows(new_leads_path, new_export_rows)
        print(f"[INFO] wrote {new_leads_count} rows -> {new_leads_path}")
        if not args.no_contact_queue_output:
            contact_queue_count = write_contact_queue_rows(
                contact_queue_path,
                build_contact_queue_export_rows(master_rows, queue_rows),
                include_email_rows=True,
            )
            print(f"[INFO] wrote {contact_queue_count} rows -> {args.contact_queue_output}")
        if not args.no_near_miss_location_output:
            near_miss_count = write_near_miss_location_rows(near_miss_location_path, near_miss_location_rows)
            print(f"[INFO] wrote {near_miss_count} rows -> {args.near_miss_location_output}")
        print(
            f"[INFO] batch {run_idx}: got {len(batch_rows)} rows, "
            f"merged {len(master_batch_rows)} to master, queued {len(queue_only_batch_rows)}, "
            f"added {added} new, total {after}/{goal_target}"
        )
        run_stats = {
            "run_tag": run_tag,
            "batch_index": run_idx,
            "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "pipeline_exit_code": 0,
            "pipeline_stdout_log": str(pipeline_stdout_path),
            "pipeline_stderr_log": str(pipeline_stderr_path),
            "merge_policy": args.merge_policy,
            "validation_profile": validation_profile,
                "counts": {
                    "harvested_candidates": row_count(candidates_path),
                    "candidates": row_count(filtered_candidates_path),
                    "validated": row_count(validated_path),
                    "final": row_count(final_path),
                "verified": row_count(verified_path),
                "pre_verify_rows": len(pre_verify_rows),
                "kept_rows_after_gate": len(batch_rows),
                "master_merge_candidates": len(master_batch_rows),
                "queue_only_candidates": len(queue_only_batch_rows),
                "suppressed_pre_validate_total": suppression_stats.get("suppressed_pre_validate_total", 0),
                "duplicate_count": duplicate_count,
                "near_miss_location_rows": row_count(run_near_miss_location_path),
                "added_to_master": added,
                "added_to_queue": added_to_queue,
                "master_total": len(master_rows),
                "goal_qualified_total": after,
                    "queue_total": len(queue_rows),
                    "verified_output_rows": verified_output_count,
                    "scouted_output_rows": scouted_output_count,
                    "new_leads_rows": new_leads_count,
                    "near_miss_location_total": len(near_miss_location_rows),
                    "verified_target": int(validation_orchestration_stats.get("verified_target", 0) or 0),
                    "verified_progress": int(validation_orchestration_stats.get("verified_progress", 0) or 0),
                    "candidates_replaced": int(validation_orchestration_stats.get("candidates_replaced", 0) or 0),
                    "dead_end_count": int(validation_orchestration_stats.get("dead_end_count", 0) or 0),
                    "exhausted_before_target": bool(validation_orchestration_stats.get("exhausted_before_target", False)),
                },
            "near_miss_location_output": str(run_near_miss_location_path),
            "harvested_candidates": analyze_candidates(candidates_path),
            "candidates": analyze_candidates(filtered_candidates_path),
            "harvest": harvest_stats,
            "pre_validate_suppression": suppression_stats,
            "validator": load_json(validate_stats_path),
            "loop_control": {
                "stale_runs": stale_runs,
                **reliability_counters,
                "stop_reason": stop_reason,
            },
        }
        if agent_hunt_stats:
            run_stats["agent_hunt"] = {
                "scouted_target": int(agent_hunt_stats.get("scouted_target", 0) or 0),
                "scouted_progress": int(agent_hunt_stats.get("scouted_progress", 0) or 0),
                "scouted_rows_written": int(agent_hunt_stats.get("scouted_rows_written", 0) or 0),
                "strict_rows_written": int(agent_hunt_stats.get("strict_rows_written", 0) or 0),
                "top_reject_reasons": dict(agent_hunt_stats.get("top_reject_reasons", {}) or {}),
                "scout_gate_reject_reasons": dict(agent_hunt_stats.get("scout_gate_reject_reasons", {}) or {}),
                "routing_counts": dict(agent_hunt_stats.get("routing_counts", {}) or {}),
                "outreach_tier_counts": dict(agent_hunt_stats.get("outreach_tier_counts", {}) or {}),
                "outreach_decision_counts": dict(agent_hunt_stats.get("outreach_decision_counts", {}) or {}),
                "top_tier_1_reasons": dict(agent_hunt_stats.get("top_tier_1_reasons", {}) or {}),
                "top_tier_2_reasons": dict(agent_hunt_stats.get("top_tier_2_reasons", {}) or {}),
                "top_tier_3_reasons": dict(agent_hunt_stats.get("top_tier_3_reasons", {}) or {}),
                "top_caution_reasons": dict(agent_hunt_stats.get("top_caution_reasons", {}) or {}),
                "top_rejected_reasons": dict(agent_hunt_stats.get("top_rejected_reasons", {}) or {}),
                "top_reasons_caution_fail_promotion_to_qualified": dict(
                    agent_hunt_stats.get("top_reasons_caution_fail_promotion_to_qualified", {}) or {}
                ),
                "scoutworthy_not_outreach_ready_count": int(
                    agent_hunt_stats.get("scoutworthy_not_outreach_ready_count", 0) or 0
                ),
                "scoutworthy_not_outreach_ready_reasons": dict(
                    agent_hunt_stats.get("scoutworthy_not_outreach_ready_reasons", {}) or {}
                ),
                "scoutworthy_not_outreach_ready_source_domains": list(
                    agent_hunt_stats.get("scoutworthy_not_outreach_ready_source_domains", []) or []
                ),
                "caution_source_types": list(agent_hunt_stats.get("caution_source_types", []) or []),
                "caution_source_queries": list(agent_hunt_stats.get("caution_source_queries", []) or []),
                "rejected_source_domains": list(agent_hunt_stats.get("rejected_source_domains", []) or []),
                "rejected_source_types": list(agent_hunt_stats.get("rejected_source_types", []) or []),
                "rejected_source_queries": list(agent_hunt_stats.get("rejected_source_queries", []) or []),
                "listing_friction": dict(agent_hunt_stats.get("listing_friction", {}) or {}),
                "listing_friction_email_recovery_attempted_count": int(
                    agent_hunt_stats.get("listing_friction_email_recovery_attempted_count", 0) or 0
                ),
                "listing_friction_email_recovery_success_count": int(
                    agent_hunt_stats.get("listing_friction_email_recovery_success_count", 0) or 0
                ),
                "listing_friction_email_recovery_fail_count": int(
                    agent_hunt_stats.get("listing_friction_email_recovery_fail_count", 0) or 0
                ),
                "top_listing_friction_email_recovery_fail_reasons": dict(
                    agent_hunt_stats.get("top_listing_friction_email_recovery_fail_reasons", {}) or {}
                ),
                "listing_friction_email_recovered_source_domains": list(
                    agent_hunt_stats.get("listing_friction_email_recovered_source_domains", []) or []
                ),
                "listing_friction_email_recovered_source_types": list(
                    agent_hunt_stats.get("listing_friction_email_recovered_source_types", []) or []
                ),
                "listing_friction_email_recovered_source_queries": list(
                    agent_hunt_stats.get("listing_friction_email_recovered_source_queries", []) or []
                ),
                "listing_friction_email_recovery_failed_source_domains": list(
                    agent_hunt_stats.get("listing_friction_email_recovery_failed_source_domains", []) or []
                ),
                "listing_friction_email_recovery_failed_source_types": list(
                    agent_hunt_stats.get("listing_friction_email_recovery_failed_source_types", []) or []
                ),
                "listing_friction_email_recovery_failed_source_queries": list(
                    agent_hunt_stats.get("listing_friction_email_recovery_failed_source_queries", []) or []
                ),
                "scouted_source_domains": list(agent_hunt_stats.get("scouted_source_domains", []) or []),
                "strict_source_domains": list(agent_hunt_stats.get("strict_source_domains", []) or []),
                "author_convergence": dict(agent_hunt_stats.get("author_convergence", {}) or {}),
                "listing_feedback_for_next_batch": dict(agent_hunt_listing_feedback or {}),
                "source_quality_feedback_for_next_batch": dict(agent_hunt_source_quality_feedback or {}),
            }
        if harvest_stats:
            run_stats["google_cse_status"] = harvest_stats.get("google_cse_status", "")
        if verify_stdout_path.exists() or verify_stderr_path.exists():
            run_stats["verify_stdout_log"] = str(verify_stdout_path)
            run_stats["verify_stderr_log"] = str(verify_stderr_path)
        write_run_stats(run_stats_path, run_stats)
        write_run_manifest(
            run_manifest_path,
            build_run_manifest_payload(
                run_tag=run_tag,
                goal_target=goal_target,
                validation_profile=validation_profile,
                pipeline_exit_code=0,
                counts={
                    "harvested_candidates": row_count(candidates_path),
                    "filtered_candidates": row_count(filtered_candidates_path),
                    "validated": row_count(validated_export_path),
                    "final": row_count(final_path),
                    "pre_verify_rows": len(pre_verify_rows),
                    "kept_rows_after_gate": len(batch_rows),
                    "master_merge_candidates": len(master_batch_rows),
                    "queue_only_candidates": len(queue_only_batch_rows),
                    "rejected": len(rejected_rows),
                    "duplicates": duplicate_count,
                    "added_to_master": added,
                    "added_to_queue": added_to_queue,
                    "goal_qualified_total": after,
                    "near_miss_location_rows": row_count(run_near_miss_location_path),
                },
                rejected_rows=rejected_rows,
                artifact_paths={
                    "candidates_csv": candidates_path,
                    "filtered_candidates_csv": filtered_candidates_path,
                    "validated_csv": validated_export_path,
                    "rejected_csv": rejected_export_path,
                    "final_csv": final_path,
                    "new_leads_csv": new_leads_path,
                    "tier_1_csv": tier_1_path,
                    "tier_2_csv": tier_2_path,
                    "tier_3_csv": tier_3_path,
                    "run_stats_json": run_stats_path,
                    "validate_stats_json": validate_stats_path,
                },
                loop_control={
                    "stale_runs": stale_runs,
                    **reliability_counters,
                    "stop_reason": stop_reason,
                },
            ),
        )
        last_pipeline_exit_code = 0
        if stop_reason:
            final_stop_reason = stop_reason
            write_live_status(
                status="stopped",
                stage="stopped",
                validation_profile=validation_profile,
                goal_target=goal_target,
                active=False,
                run_tag=run_tag,
                batch_index=run_idx,
                max_runs=max(1, args.max_runs),
                counts={
                    "goal_qualified_total": after,
                    "harvested_candidates": row_count(candidates_path),
                    "filtered_candidates": row_count(filtered_candidates_path),
                    "validated": row_count(validated_export_path),
                    "final": row_count(final_path),
                    "rejected": len(rejected_rows),
                    "duplicates": duplicate_count,
                    "added_to_master": added,
                    "added_to_queue": added_to_queue,
                },
                pipeline_exit_code=0,
                stop_reason=stop_reason,
                message="Loop stopped after completed batch",
                runs_dir=runs_dir,
                log_path=pipeline_stdout_path,
                artifact_paths={"run_manifest_json": run_manifest_path},
            )
        else:
            write_live_status(
                status="running",
                stage="validation_complete",
                validation_profile=validation_profile,
                goal_target=goal_target,
                active=True,
                run_tag=run_tag,
                batch_index=run_idx,
                max_runs=max(1, args.max_runs),
                counts={
                    "goal_qualified_total": after,
                    "harvested_candidates": row_count(candidates_path),
                    "filtered_candidates": row_count(filtered_candidates_path),
                    "validated": row_count(validated_export_path),
                    "final": row_count(final_path),
                    "rejected": len(rejected_rows),
                    "duplicates": duplicate_count,
                    "added_to_master": added,
                    "added_to_queue": added_to_queue,
                },
                pipeline_exit_code=0,
                message="Batch completed",
                runs_dir=runs_dir,
                log_path=pipeline_stdout_path,
                artifact_paths={"run_manifest_json": run_manifest_path},
            )
        if stop_reason:
            print(f"[INFO] stopping early after {stop_reason.replace('_', ' ')}.")
            break

        if after < goal_target:
            time.sleep(max(0.0, args.sleep_seconds))

    write_rows(master_path, master_rows)
    if not args.no_minimal_output:
        minimal_count = write_minimal_rows(
            Path(args.minimal_output),
            master_rows,
            with_header=args.minimal_with_header,
        )
        print(f"[OK] wrote {minimal_count} rows -> {args.minimal_output}")
    if is_agent_hunt_profile(validation_profile):
        scouted_rows = [row for row in master_rows if row_is_agent_hunt_qualified(row)]
        strict_rows = [row for row in scouted_rows if row_is_fully_verified(row, require_us_location=True)]
        if not args.no_scouted_output:
            scouted_output_count = write_scouted_rows(Path(args.scouted_output), scouted_rows)
            print(f"[OK] wrote {scouted_output_count} rows -> {args.scouted_output}")
        if not args.no_verified_output:
            verified_output_count = write_verified_rows(Path(args.verified_output), strict_rows)
            print(f"[OK] wrote {verified_output_count} rows -> {args.verified_output}")
    elif is_fully_verified_profile(validation_profile) and not args.no_verified_output:
        verified_rows = [
            row for row in master_rows if row_is_fully_verified(row, require_us_location=profile_requires_us_location(validation_profile))
        ]
        verified_output_count = write_verified_rows(Path(args.verified_output), verified_rows)
        print(f"[OK] wrote {verified_output_count} rows -> {args.verified_output}")
    if not args.no_contact_queue_output:
        contact_queue_count = write_contact_queue_rows(
            contact_queue_path,
            build_contact_queue_export_rows(master_rows, queue_rows),
            include_email_rows=True,
        )
        print(f"[OK] wrote {contact_queue_count} rows -> {args.contact_queue_output}")
    if not args.no_near_miss_location_output:
        near_miss_count = write_near_miss_location_rows(near_miss_location_path, near_miss_location_rows)
        print(f"[OK] wrote {near_miss_count} rows -> {args.near_miss_location_output}")
    final_total = count_goal_rows(master_rows, validation_profile=validation_profile, policy=args.merge_policy)
    if final_stop_reason:
        final_status = "stopped"
        final_stage = "stopped"
        final_message = "Loop stopped"
    elif final_total >= goal_target:
        final_status = "completed"
        final_stage = "completed"
        final_message = "Goal reached"
    else:
        final_status = "stopped"
        final_stage = "stopped"
        final_stop_reason = "max_runs_exhausted"
        final_message = "Max runs exhausted before goal"
    write_live_status(
        status=final_status,
        stage=final_stage,
        validation_profile=validation_profile,
        goal_target=goal_target,
        active=False,
        run_tag=last_run_tag,
        counts={
            "goal_qualified_total": final_total,
            "master_total": len(master_rows),
            "queue_total": len(queue_rows),
        },
        pipeline_exit_code=last_pipeline_exit_code,
        stop_reason=final_stop_reason,
        message=final_message,
        runs_dir=runs_dir,
    )
    print(f"[OK] done: {final_total} goal-qualified leads -> {master_path}")
    if final_total < goal_target:
        print(f"[INFO] target not reached ({final_total}/{goal_target}). Increase --max-runs or broaden inputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
