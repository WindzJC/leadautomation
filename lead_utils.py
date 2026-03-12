#!/usr/bin/env python3
"""
Shared URL/blocklist helpers for the lead-finder pipeline.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

SECOND_LEVEL_SUFFIXES = {
    "co.uk",
    "org.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "co.jp",
    "com.br",
    "com.mx",
}


def _config_dir() -> Path:
    return Path(__file__).resolve().parent / "config"


def _read_config_lines(path: Path) -> Tuple[str, ...]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            values = []
            for line in fh:
                value = line.strip().lower()
                if not value or value.startswith("#"):
                    continue
                values.append(value)
            return tuple(values)
    except OSError:
        return ()


@lru_cache(maxsize=1)
def blocked_domains() -> Tuple[str, ...]:
    return _read_config_lines(_config_dir() / "blocked_domains.txt")


@lru_cache(maxsize=1)
def blocked_url_patterns() -> Tuple[str, ...]:
    return _read_config_lines(_config_dir() / "blocked_url_patterns.txt")


def normalize_host(value: str) -> str:
    host = (value or "").strip().lower()
    if "://" in host:
        host = urlparse(host).netloc.lower()
    host = host.split("@", 1)[-1].split(":", 1)[0].strip(".")
    return host


def domain_matches_blocklist(host_or_url: str, extra_domains: Iterable[str] = ()) -> bool:
    host = normalize_host(host_or_url)
    if not host:
        return False
    for domain in tuple(blocked_domains()) + tuple(extra_domains):
        blocked = normalize_host(domain)
        if not blocked:
            continue
        if host == blocked or host.endswith("." + blocked):
            return True
    return False


def url_matches_blocklist(url: str, extra_domains: Iterable[str] = (), extra_patterns: Iterable[str] = ()) -> bool:
    normalized = (url or "").strip().lower()
    if not normalized:
        return False
    if domain_matches_blocklist(normalized, extra_domains=extra_domains):
        return True
    for pattern in tuple(blocked_url_patterns()) + tuple(extra_patterns):
        lowered = (pattern or "").strip().lower()
        if lowered and lowered in normalized:
            return True
    return False


def registrable_domain(url_or_host: str) -> str:
    host = normalize_host(url_or_host)
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    parts = [part for part in host.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    suffix = ".".join(parts[-2:])
    if suffix in SECOND_LEVEL_SUFFIXES:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


ASIN_RE = re.compile(r"/(?:dp|gp/product|kindle-dbs/product)/([A-Z0-9]{10})(?:[/?#]|$)", flags=re.IGNORECASE)
AMAZON_ALLOWED_PATH_RE = re.compile(r"/(?:[^/]+/)?dp/[A-Z0-9]{10}(?:[/?#]|$)|/gp/product/[A-Z0-9]{10}(?:[/?#]|$)", re.IGNORECASE)
AMAZON_BLOCKED_PATH_PREFIXES = ("/b", "/s", "/stores", "/gp/browse", "/gp/search")
TARGET_BLOCKED_PATH_PREFIXES = ("/s",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_",
    "si",
}
TRACKING_QUERY_PREFIXES = ("utm_",)


def extract_asin(url: str) -> str:
    match = ASIN_RE.search((url or "").strip())
    if not match:
        return ""
    return match.group(1).upper()


def canonical_listing_key(url: str) -> str:
    normalized = (url or "").strip()
    if not normalized:
        return ""
    asin = extract_asin(normalized)
    if asin:
        return f"amazon:{asin}"
    parsed = urlparse(normalized)
    if not parsed.netloc:
        return ""
    canonical = parsed._replace(query="", fragment="").geturl().lower()
    return canonical


def normalize_person_name(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", (value or "").lower())
    return " ".join(tokens)


def strip_tracking_query_params(url: str) -> str:
    normalized = (url or "").strip()
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc or not parsed.query:
        return normalized

    kept = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.strip().lower()
        if lowered in TRACKING_QUERY_KEYS:
            continue
        if lowered.startswith(TRACKING_QUERY_PREFIXES):
            continue
        kept.append((key, value))
    return urlunparse(parsed._replace(query=urlencode(kept, doseq=True)))


def is_amazon_url(url: str) -> bool:
    host = normalize_host(url)
    return host == "amazon.com" or host.endswith(".amazon.com")


def is_target_url(url: str) -> bool:
    host = normalize_host(url)
    return host == "target.com" or host.endswith(".target.com")


def is_barnesandnoble_url(url: str) -> bool:
    host = normalize_host(url)
    return host == "barnesandnoble.com" or host.endswith(".barnesandnoble.com")


def is_retailer_url(url: str) -> bool:
    return is_amazon_url(url) or is_target_url(url) or is_barnesandnoble_url(url)


def is_allowed_retailer_url(url: str) -> bool:
    normalized = (url or "").strip()
    if not normalized:
        return False
    parsed = urlparse(normalized)
    path = parsed.path.lower() or "/"

    if is_amazon_url(normalized):
        if any(path == prefix or path.startswith(prefix + "/") for prefix in AMAZON_BLOCKED_PATH_PREFIXES):
            return False
        return bool(AMAZON_ALLOWED_PATH_RE.search(parsed.path))

    if is_target_url(normalized):
        return False

    if is_barnesandnoble_url(normalized):
        return "/w/" in path

    return True
