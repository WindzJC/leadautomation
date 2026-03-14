#!/usr/bin/env python3
"""
Stage 1: Search the web for candidate author/book pages.
Outputs a CSV with approximately N candidate URLs for later validation.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import html
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Dict, Iterable, List, Optional, TypeVar
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lead_utils import (
    domain_matches_blocklist,
    is_allowed_retailer_url,
    is_retailer_url,
    registrable_domain,
    strip_tracking_query_params,
    url_matches_blocklist,
)

DEFAULT_QUERIES = [
    '"indie author" "official website"',
    '"self-published author" "contact"',
    '"independently published author" "newsletter"',
    '"fantasy author" "official website" "indie"',
    '"science fiction author" "official website" "indie"',
    '"romance author" "official website" "indie"',
    '"thriller author" "official website" "indie"',
    '"young adult author" "official website" "indie"',
    '"urban fantasy author" "official website"',
    '"author website" "new release" "indie author"',
]

EPIC_DIRECTORY_URL = "https://www.epicindie.net/authordirectory"
INDEPENDENT_AUTHOR_NETWORK_DIRECTORY_URL = "https://www.independentauthornetwork.com/author-directory.html"
IABX_DIRECTORY_URL = "https://www.iabx.org/author-directory"

GOODREADS_SEEDS = [
    ("https://www.goodreads.com/genres/indie", "goodreads:genres:indie"),
    ("https://www.goodreads.com/shelf/show/indie", "goodreads:shelf:indie"),
    ("https://www.goodreads.com/shelf/show/self-published", "goodreads:shelf:self-published"),
    ("https://www.goodreads.com/shelf/show/independent-authors", "goodreads:shelf:independent-authors"),
    ("https://www.goodreads.com/shelf/show/kindle-unlimited", "goodreads:shelf:kindle-unlimited"),
    ("https://www.goodreads.com/shelf/show/indie-fantasy", "goodreads:shelf:indie-fantasy"),
    ("https://www.goodreads.com/shelf/show/indie-romance", "goodreads:shelf:indie-romance"),
    ("https://www.goodreads.com/shelf/show/indie-science-fiction", "goodreads:shelf:indie-scifi"),
    ("https://www.goodreads.com/shelf/show/self-published-fantasy", "goodreads:shelf:selfpub-fantasy"),
    ("https://www.goodreads.com/shelf/show/self-published-romance", "goodreads:shelf:selfpub-romance"),
]

OPENLIBRARY_SUBJECTS = (
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
)

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

BLOCKED_HOST_SNIPPETS = (
    "duckduckgo.com",
    "google.",
    "bing.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "linkedin.com",
    "wikipedia.org",
    "stackexchange.com",
    "stackoverflow.com",
    "quora.com",
    "reddit.com",
    "knoji.com",
    "baidu.com",
    "zhihu.com",
    "qyer.com",
    "fandom.com",
    "pinterest.com",
    "kdp.org",
    "weforum.org",
    "sciencenews.org",
    "worldbank.org",
    "history.state.gov",
    "justanswer.com",
    "merriam-webster.com",
    "dictionary.com",
    "dictionary.cambridge.org",
    "thefreedictionary.com",
    "collinsdictionary.com",
    "fifa.com",
    "gog.com",
    "ampreviews.net",
    "horror.com",
    "bluesnews.com",
    "fullstack.com.au",
    "self.inc",
    "cyberpunk.net",
    "britannica.com",
    "literarydevices.net",
    "onthisday.com",
    "aithor.com",
    "kindlepreneur.com",
    "reedsy.com",
    "toptenpublishers.com",
    "forthewriters.com",
    "draft2digital.com",
    "read.amazon.com",
    "apps.apple.com",
    "bestbuy.com",
    "pcmag.com",
    "lifehacker.com",
    "amazonforum.com",
)

BLOCKED_PATH_SNIPPETS = (
    "/question/",
    "/questions/",
    "/thread",
    "/forum",
    "/tag/",
    "/topic/",
    "/answers/",
    "/comments/",
    "/review",
    "/reviews",
    "/blog",
    "/news",
    "/editor",
    "/citation",
)

LEAD_HINTS = (
    "author",
    "book",
    "novel",
    "writer",
    "indie",
    "contact",
    "about",
    "press",
    "publication",
    "kdp",
    "ingramspark",
    "draft2digital",
    "barnesandnoble",
    "amazon",
)

RELEVANT_PATH_HINTS = (
    "/book/",
    "/books",
    "/author",
    "/bio",
    "/about",
    "/contact",
    "/dp/",
    "/gp/product/",
    "/kindle-dbs/product/",
    "/w/",
)

ALWAYS_ALLOWED_HOST_SNIPPETS = (
    "goodreads.com",
    "amazon.com",
    "barnesandnoble.com",
)

OPENLIBRARY_BLOCKED_EXTERNAL_HOST_SNIPPETS = (
    "openlibrary.org",
    "archive.org",
    "worldcat.org",
    "facebook.com",
    "twitter.com",
    "pinterest.com",
    "bsky.app",
)

T = TypeVar("T")

QUERY_INTENT_HINTS = (
    "author",
    "book",
    "novel",
    "writer",
    "contact",
    "mailto",
    "contact@",
    "indie",
    "self-published",
    "independently published",
    "kdp",
    "ingramspark",
    "draft2digital",
    "b&n",
    "barnes",
)

RESULT_TEXT_HINTS = (
    "author",
    "authors",
    "book",
    "books",
    "novel",
    "novels",
    "writer",
    "writers",
    "paperback",
    "hardcover",
    "ebook",
    "kindle",
    "independently published",
    "self-published",
    "self published",
    "kdp",
    "ingramspark",
    "draft2digital",
    "mailing list",
    "newsletter",
    "buy now",
    "new release",
    "author website",
)

PERSON_NAME_BLOCK_WORDS = {
    "about",
    "advanced",
    "adult",
    "ai",
    "amazon",
    "anydesk",
    "app",
    "apps",
    "best",
    "bowl",
    "calendar",
    "coffee",
    "contact",
    "digital",
    "download",
    "fantasy",
    "games",
    "gateway",
    "gemini",
    "guide",
    "help",
    "home",
    "horror",
    "how",
    "indie",
    "lyrics",
    "media",
    "mystery",
    "news",
    "official",
    "parts",
    "perplexity",
    "recipes",
    "remote",
    "review",
    "search",
    "seattle",
    "series",
    "shop",
    "software",
    "sports",
    "store",
    "support",
    "travel",
    "weather",
    "young",
}

SHORTLINK_HOSTS = ("amzn.to",)
AUTHOR_OUTBOUND_PATH_HINTS = (
    "/contact",
    "/about",
    "/press",
    "/media",
    "/newsletter",
    "/subscribe",
)
AUTHOR_OUTBOUND_CONTEXT_HINTS = (
    "author website",
    "official website",
    "official site",
    "website",
    "author page",
    "author profile",
    "contact",
    "about",
    "press",
    "media",
    "newsletter",
    "subscribe",
    "mailing list",
)
OUTBOUND_HOST_POSITIVE_HINTS = (
    "author",
    "books",
    "writes",
    "writer",
    "novels",
)
OUTBOUND_HOST_NEGATIVE_HINTS = (
    "blog",
    "blogs",
    "review",
    "reviews",
    "media",
    "news",
    "press",
    "fantasyreviews",
    "bookreview",
    "bookreviews",
    "bookerification",
    "novelnotions",
    "doorway",
    "cuddlebuggery",
    "photobucket",
    "imgbb",
    "hungama",
    "people",
)
OUTBOUND_BLOCKED_HOST_SNIPPETS = (
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "linkedin.com",
    "pinterest.com",
    "amazon.",
    "barnesandnoble.com",
    "goodreads.com",
    "openlibrary.org",
    "archive.org",
    "worldcat.org",
    "bookshop.org",
    "bookdepository.",
    "blackwells.",
    "betterworldbooks.com",
    "storygraph.com",
    "bookbrainz.org",
)
OUTBOUND_BLOCKED_PATH_SNIPPETS = (
    "/watch",
    "/playlist",
    "/channel/",
    "/shorts/",
    "/video/",
    "/videos/",
    "/catalog/",
    "/category/",
    "/categories/",
    "/collections/",
    "/archive/",
    "/archives/",
    "/product/",
    "/products/",
    "/shop/",
    "/store/",
    "/marketplace/",
)

DIRECTORY_AUTHOR_POSITIVE_HINTS = (
    "author",
    "author of",
    "official website",
    "official site",
    "author website",
    "writer",
    "novelist",
    "indie fantasy author",
    "indie sci-fi author",
    "indie scifi author",
    "indie author",
)
DIRECTORY_AUTHOR_NEGATIVE_HINTS = (
    "reviewer",
    "reviewers",
    "book review",
    "book reviews",
    "map maker",
    "game designer",
    "cover designer",
    "editorial services",
    "promo services",
    "resources",
    "ally of indie authors",
    "friend of epic",
    "friend and ally",
    "merch store",
    "patreon",
)
DIRECTORY_BOOK_HINTS = (
    "author of",
    "book 1",
    "series",
    "novel",
    "novella",
    "new release",
    "available now",
    "mailing list",
    "newsletter",
    "contact",
)
CANDIDATE_FIELDNAMES = [
    "CandidateURL",
    "SourceType",
    "SourceQuery",
    "SourceURL",
    "SourceTitle",
    "SourceSnippet",
    "DiscoveredAtUTC",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Harvest candidate author/book URLs from web search.")
    parser.add_argument("--target", type=int, default=80, help="Target candidate count (default: 80).")
    parser.add_argument(
        "--min-candidates",
        type=int,
        default=80,
        help="Minimum candidates to try to deliver before stopping harvest (default: 80).",
    )
    parser.add_argument(
        "--per-query",
        type=int,
        default=15,
        help="Maximum search results to keep per query (default: 15).",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.0,
        help="Pause between query requests (default: 1.0).",
    )
    parser.add_argument(
        "--output",
        default="candidates.csv",
        help="Output CSV path (default: candidates.csv).",
    )
    parser.add_argument(
        "--stats-output",
        default="",
        help="Optional JSON output for harvest stats and healthcheck metadata.",
    )
    parser.add_argument(
        "--max-per-domain",
        type=int,
        default=0,
        help="Maximum candidates kept per domain (0 disables cap).",
    )
    parser.add_argument(
        "--queries-file",
        default="",
        help="Optional text file (one query per line). If omitted, built-in queries are used.",
    )
    parser.add_argument(
        "--goodreads-pages",
        type=int,
        default=3,
        help="Number of pages to pull per Goodreads seed (default: 3).",
    )
    parser.add_argument(
        "--max-goodreads-candidates",
        type=int,
        default=12,
        help="Maximum candidates taken from Goodreads seeds before web queries (default: 12).",
    )
    parser.add_argument(
        "--goodreads-outbound-per-url",
        type=int,
        default=2,
        help="Max outbound author/contact URLs to add per Goodreads page (default: 2).",
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
        help="Timeout in seconds per search request (default: 10.0).",
    )
    parser.add_argument(
        "--goodreads-timeout",
        type=float,
        default=12.0,
        help="Timeout in seconds per Goodreads seed request (default: 12.0).",
    )
    parser.add_argument(
        "--http-retries",
        type=int,
        default=1,
        help="HTTP retry count for transient failures (default: 1).",
    )
    parser.add_argument(
        "--harvest-time-budget",
        type=float,
        default=90.0,
        help="Approximate seconds to spend escalating harvest before accepting a shortfall.",
    )
    parser.add_argument(
        "--max-openlibrary-candidates",
        type=int,
        default=16,
        help="Maximum candidates taken from Open Library work pages before web queries (default: 16).",
    )
    parser.add_argument(
        "--openlibrary-per-subject",
        type=int,
        default=2,
        help="Open Library works to inspect per rotated subject (default: 2).",
    )
    parser.add_argument(
        "--disable-openlibrary",
        action="store_true",
        help="Disable Open Library subject discovery.",
    )
    parser.add_argument(
        "--google-api-key",
        default=os.getenv("GOOGLE_API_KEY", ""),
        help="Optional Google Custom Search API key (or env GOOGLE_API_KEY).",
    )
    parser.add_argument(
        "--google-cx",
        default=os.getenv("GOOGLE_CSE_CX", ""),
        help="Optional Google Custom Search engine ID (or env GOOGLE_CSE_CX).",
    )
    parser.add_argument(
        "--brave-api-key",
        default=os.getenv("BRAVE_SEARCH_API_KEY", ""),
        help="Optional Brave Search API key (or env BRAVE_SEARCH_API_KEY).",
    )
    parser.add_argument(
        "--disable-web-fallback",
        action="store_true",
        help="Disable DDG/Bing HTML/RSS fallback queries after deterministic sources.",
    )
    return parser.parse_args()


def load_queries(queries_file: str) -> List[str]:
    if not queries_file:
        return DEFAULT_QUERIES

    queries: List[str] = []
    with open(queries_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                queries.append(line)
    return queries or DEFAULT_QUERIES


def clean_result_url(raw_url: str) -> str:
    if not raw_url:
        return ""

    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url

    parsed = urlparse(raw_url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        params = parse_qs(parsed.query)
        uddg = params.get("uddg", [""])[0]
        if uddg:
            return unquote(uddg)
    if parsed.netloc.endswith("bing.com") and parsed.path.startswith("/ck/"):
        params = parse_qs(parsed.query)
        encoded = params.get("u", [""])[0]
        if encoded.startswith("a1"):
            payload = encoded[2:]
            payload += "=" * (-len(payload) % 4)
            try:
                decoded = base64.b64decode(payload).decode("utf-8", errors="ignore")
                if decoded.startswith("http://") or decoded.startswith("https://"):
                    return decoded
            except (ValueError, UnicodeDecodeError):
                pass

    return raw_url


def normalize_url(url: str) -> str:
    url = url.strip()
    if not url:
        return ""
    if re.match(r"^(javascript|mailto|tel|data):", url, re.IGNORECASE):
        return ""
    if not re.match(r"^https?://", url, re.IGNORECASE):
        url = "https://" + url
    parsed = urlparse(url)
    if not parsed.netloc:
        return ""
    if parsed.scheme and parsed.scheme not in ("http", "https"):
        return ""
    return strip_tracking_query_params(parsed._replace(fragment="").geturl())


def truncate_snippet(value: str, limit: int = 280) -> str:
    compact = re.sub(r"\s+", " ", (value or "").strip())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 3)].rstrip() + "..."


def make_candidate_row(
    url: str,
    source_type: str,
    source_query: str,
    timestamp: str,
    source_url: str = "",
    source_title: str = "",
    source_snippet: str = "",
) -> Dict[str, str]:
    return {
        "CandidateURL": url,
        "SourceType": source_type,
        "SourceQuery": source_query,
        "SourceURL": source_url,
        "SourceTitle": truncate_snippet(source_title, limit=180),
        "SourceSnippet": truncate_snippet(source_snippet, limit=320),
        "DiscoveredAtUTC": timestamp,
    }


def is_candidate_url(url: str, source_query: str = "") -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    if url_matches_blocklist(normalized):
        return False
    if is_retailer_url(normalized) and not is_allowed_retailer_url(normalized):
        return False
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if domain_matches_blocklist(host):
        return False
    if any(bad in host for bad in BLOCKED_HOST_SNIPPETS):
        return False
    if any(bad in path for bad in BLOCKED_PATH_SNIPPETS):
        return False
    if path.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp")):
        return False
    # Accept root domains so validation can decide relevance.
    if path in ("", "/"):
        if is_retailer_url(normalized):
            return False
        return True
    if path in ("/home", "/homepage", "/index", "/search", "/about-us", "/aboutus"):
        return False

    signal = f"{host}{path}"
    has_lead_hint = any(hint in signal for hint in LEAD_HINTS) or any(hint in path for hint in RELEVANT_PATH_HINTS)
    is_known_book_host = any(snippet in host for snippet in ALWAYS_ALLOWED_HOST_SNIPPETS)
    if has_lead_hint or is_known_book_host:
        return True

    query_has_author_intent = any(hint in (source_query or "").lower() for hint in QUERY_INTENT_HINTS)
    if query_has_author_intent:
        depth = len([part for part in path.split("/") if part])
        if depth <= 2 and len(path) <= 120:
            return True
    return False


def expand_shortlink(url: str, session: requests.Session, timeout: float) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""

    for method_name in ("head", "get"):
        requester = getattr(session, method_name, None)
        if requester is None:
            continue
        try:
            try:
                resp = requester(normalized, timeout=timeout, allow_redirects=True)
            except TypeError:
                resp = requester(normalized, timeout=timeout)
        except requests.RequestException:
            continue
        expanded = normalize_url(clean_result_url(getattr(resp, "url", normalized)))
        if expanded:
            return expanded
    return ""


def resolve_discovered_url(
    raw_url: str,
    session: requests.Session,
    timeout: float,
) -> str:
    normalized = normalize_url(clean_result_url(raw_url))
    if not normalized:
        return ""

    host = urlparse(normalized).netloc.lower()
    if any(host == short_host or host.endswith("." + short_host) for short_host in SHORTLINK_HOSTS):
        expanded = expand_shortlink(normalized, session=session, timeout=timeout)
        if not expanded:
            return ""
        expanded_parsed = urlparse(expanded)
        expanded_host = expanded_parsed.netloc.lower()
        expanded_path = expanded_parsed.path.lower()
        if not (expanded_host == "amazon.com" or expanded_host.endswith(".amazon.com")):
            return ""
        if not any(token in expanded_path for token in ("/dp/", "/gp/product/")):
            return ""
        normalized = expanded

    if url_matches_blocklist(normalized):
        return ""
    return normalized


def normalize_result_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "").strip()).lower()


def slugify_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


def looks_like_person_name(value: str) -> bool:
    if not value:
        return False
    candidate = re.sub(r"\s*[|:-].*$", "", value).strip()
    tokens = re.findall(r"[A-Za-z][A-Za-z'.-]*", candidate)
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    lowered = [token.lower() for token in tokens]
    if any(token in PERSON_NAME_BLOCK_WORDS for token in lowered):
        return False
    return True


def is_candidate_result(url: str, title: str = "", snippet: str = "", source_query: str = "") -> bool:
    if not is_candidate_url(url, source_query=source_query):
        return False

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    is_known_book_host = any(allowed in host for allowed in ALWAYS_ALLOWED_HOST_SNIPPETS)
    if is_known_book_host:
        return True

    title_text = normalize_result_text(title)
    snippet_text = normalize_result_text(snippet)
    text_signal = f"{title_text} {snippet_text}".strip()
    if any(hint in text_signal for hint in RESULT_TEXT_HINTS):
        return True

    query_has_author_intent = any(hint in (source_query or "").lower() for hint in QUERY_INTENT_HINTS)
    if query_has_author_intent and looks_like_person_name(title):
        return True

    return False


def is_promotable_author_outbound(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    if url_matches_blocklist(normalized):
        return False
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if any(snippet in host for snippet in OUTBOUND_BLOCKED_HOST_SNIPPETS):
        return False
    if any(snippet in path for snippet in OUTBOUND_BLOCKED_PATH_SNIPPETS):
        return False

    host_label = registrable_domain(host).split(".", 1)[0]
    has_positive_host_hint = any(hint in host_label for hint in OUTBOUND_HOST_POSITIVE_HINTS)
    has_negative_host_hint = any(hint in host_label for hint in OUTBOUND_HOST_NEGATIVE_HINTS)
    if has_negative_host_hint and not has_positive_host_hint:
        return False
    if path in ("", "/"):
        return has_positive_host_hint
    return any(hint in path for hint in AUTHOR_OUTBOUND_PATH_HINTS)


def is_promotable_author_outbound_link(url: str, anchor_text: str = "", context_text: str = "") -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    if url_matches_blocklist(normalized):
        return False

    signal = normalize_result_text(f"{anchor_text} {context_text}")
    has_context_hint = any(hint in signal for hint in AUTHOR_OUTBOUND_CONTEXT_HINTS)
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if any(snippet in host for snippet in OUTBOUND_BLOCKED_HOST_SNIPPETS):
        return False
    if any(snippet in path for snippet in OUTBOUND_BLOCKED_PATH_SNIPPETS):
        return False
    host_label = registrable_domain(parsed.netloc).split(".", 1)[0]
    has_positive_host_hint = any(hint in host_label for hint in OUTBOUND_HOST_POSITIVE_HINTS)
    has_negative_host_hint = any(hint in host_label for hint in OUTBOUND_HOST_NEGATIVE_HINTS)
    if has_negative_host_hint and not has_positive_host_hint:
        return False

    if parsed.path.lower() in ("", "/"):
        return has_context_hint or has_positive_host_hint
    return any(hint in path for hint in AUTHOR_OUTBOUND_PATH_HINTS) and (has_context_hint or has_positive_host_hint)


def link_context_text(anchor: BeautifulSoup) -> str:
    pieces = [
        anchor.get_text(" ", strip=True),
        (anchor.get("title") or "").strip(),
        (anchor.get("aria-label") or "").strip(),
    ]
    parent = getattr(anchor, "parent", None)
    if parent is not None:
        pieces.append(parent.get_text(" ", strip=True))
    return " ".join(part for part in pieces if part)


def extract_goodreads_outbound_links(
    base_url: str,
    soup: BeautifulSoup,
    limit: int,
    session: requests.Session,
    timeout: float,
) -> List[str]:
    links: List[str] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        raw = (a.get("href") or "").strip()
        if not raw:
            continue
        abs_url = resolve_discovered_url(raw, session=session, timeout=timeout)
        if not abs_url:
            abs_url = resolve_discovered_url(urljoin(base_url, raw), session=session, timeout=timeout)
        if not abs_url:
            continue
        host = urlparse(abs_url).netloc.lower()
        if not host or host.endswith("goodreads.com"):
            continue
        context_text = link_context_text(a)
        if not is_promotable_author_outbound_link(abs_url, anchor_text=a.get_text(" ", strip=True), context_text=context_text):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        links.append(abs_url)
        if len(links) >= max(0, limit):
            break
    return links


def search_duckduckgo(query: str, per_query: int, session: requests.Session, timeout: float) -> List[str]:
    url = "https://duckduckgo.com/html/"
    params = {"q": query}
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    result_blocks = soup.select("div.result")
    if not result_blocks:
        result_blocks = soup.select("article.result")

    for block in result_blocks:
        anchor = block.select_one("a.result__a") or block.select_one("a.result-link") or block.select_one("a[href]")
        if anchor is None:
            continue
        raw = anchor.get("href", "").strip()
        cleaned = resolve_discovered_url(raw, session=session, timeout=timeout)
        title = anchor.get_text(" ", strip=True)
        snippet_node = block.select_one(".result__snippet") or block.select_one(".result__extras__url")
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        if cleaned and is_candidate_result(cleaned, title=title, snippet=snippet, source_query=query):
            links.append(cleaned)
        if len(links) >= per_query:
            break

    if links:
        return links

    selectors = ("a.result__a", "a.result-link", "a[data-testid='result-title-a']")
    anchors = []
    for selector in selectors:
        anchors.extend(soup.select(selector))
    if not anchors:
        anchors = soup.find_all("a", href=True)

    for anchor in anchors:
        raw = anchor.get("href", "").strip()
        cleaned = resolve_discovered_url(raw, session=session, timeout=timeout)
        title = anchor.get_text(" ", strip=True)
        if cleaned and is_candidate_result(cleaned, title=title, source_query=query):
            links.append(cleaned)
        if len(links) >= per_query:
            break
    return links


def search_bing_html(query: str, per_query: int, session: requests.Session, timeout: float) -> List[str]:
    url = "https://www.bing.com/search"
    params = {"q": query, "setlang": "en-us", "cc": "US", "mkt": "en-US"}
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    links: List[str] = []

    result_blocks = soup.select("li.b_algo")
    for block in result_blocks:
        anchor = block.select_one("h2 a[href]") or block.select_one("a[href]")
        if anchor is None:
            continue
        raw = anchor.get("href", "").strip()
        cleaned = resolve_discovered_url(raw, session=session, timeout=timeout)
        title = anchor.get_text(" ", strip=True)
        snippet_node = block.select_one(".b_caption p") or block.select_one("p")
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        if cleaned and is_candidate_result(cleaned, title=title, snippet=snippet, source_query=query):
            links.append(cleaned)
        if len(links) >= per_query:
            break

    if links:
        return links

    anchors = soup.select("li.b_algo h2 a[href]")
    if not anchors:
        anchors = soup.find_all("a", href=True)

    for anchor in anchors:
        raw = anchor.get("href", "").strip()
        cleaned = resolve_discovered_url(raw, session=session, timeout=timeout)
        title = anchor.get_text(" ", strip=True)
        if cleaned and is_candidate_result(cleaned, title=title, source_query=query):
            links.append(cleaned)
        if len(links) >= per_query:
            break
    return links


def search_bing_rss(query: str, per_query: int, session: requests.Session, timeout: float) -> List[str]:
    url = "https://www.bing.com/search"
    params = {"q": query, "format": "rss", "setlang": "en-us", "cc": "US", "mkt": "en-US"}
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()

    # RSS is lightweight and tends to be more script-friendly than HTML SERPs.
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        print(f"[WARN] malformed Bing RSS for query {query!r}: {exc}")
        return []
    links: List[str] = []
    for item in root.findall("./channel/item"):
        link_tag = item.find("link")
        if link_tag is None or not link_tag.text:
            continue
        raw = html.unescape(link_tag.text.strip())
        cleaned = resolve_discovered_url(raw, session=session, timeout=timeout)
        title_tag = item.find("title")
        desc_tag = item.find("description")
        title = title_tag.text.strip() if title_tag is not None and title_tag.text else ""
        snippet = desc_tag.text.strip() if desc_tag is not None and desc_tag.text else ""
        if cleaned and is_candidate_result(cleaned, title=title, snippet=snippet, source_query=query):
            links.append(cleaned)
        if len(links) >= per_query:
            break
    return links


def classify_google_cse_status(http_status: int, error_reason: str, error_message: str) -> str:
    lowered_reason = (error_reason or "").strip().lower()
    lowered_message = (error_message or "").strip().lower()
    if 200 <= http_status < 300:
        return "ok"
    if "accessnotconfigured" in lowered_reason or "servicedisabled" in lowered_reason:
        return "accessNotConfigured"
    if "keyinvalid" in lowered_reason or "api_key_invalid" in lowered_reason:
        return "keyInvalid"
    if "api key not valid" in lowered_message or "invalid api key" in lowered_message:
        return "keyInvalid"
    if http_status == 403:
        return "forbidden"
    return "other"


def google_cse_healthcheck(
    session: requests.Session,
    api_key: str,
    cx: str,
    timeout: float,
) -> Dict[str, object]:
    if not api_key or not cx:
        return {
            "status": "other",
            "http_status": 0,
            "error_reason": "missing_credentials",
            "error_message": "Google CSE key or cx not configured",
        }

    params = {
        "q": "indie author official website",
        "key": api_key,
        "cx": cx,
        "num": 1,
        "start": 1,
    }
    http_status = 0
    error_reason = ""
    error_message = ""

    try:
        resp = session.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=timeout)
        http_status = int(resp.status_code)
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        if response is not None:
            http_status = int(getattr(response, "status_code", 0) or 0)
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            error = payload.get("error", {}) if isinstance(payload, dict) else {}
            errors = error.get("errors", []) if isinstance(error, dict) else []
            if errors and isinstance(errors[0], dict):
                error_reason = str(errors[0].get("reason", "") or "")
                error_message = str(errors[0].get("message", "") or "")
            if not error_message:
                error_message = str(error.get("message", "") or "") if isinstance(error, dict) else ""
        if not error_message:
            error_message = str(exc)
        return {
            "status": classify_google_cse_status(http_status, error_reason, error_message),
            "http_status": http_status,
            "error_reason": error_reason,
            "error_message": error_message,
        }

    if 200 <= http_status < 300:
        return {
            "status": "ok",
            "http_status": http_status,
            "error_reason": "",
            "error_message": "",
        }

    try:
        payload = resp.json()
    except ValueError:
        payload = {}
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    errors = error.get("errors", []) if isinstance(error, dict) else []
    if errors and isinstance(errors[0], dict):
        error_reason = str(errors[0].get("reason", "") or "")
        error_message = str(errors[0].get("message", "") or "")
    if not error_message and isinstance(error, dict):
        error_message = str(error.get("message", "") or "")
    return {
        "status": classify_google_cse_status(http_status, error_reason, error_message),
        "http_status": http_status,
        "error_reason": error_reason,
        "error_message": error_message,
    }


def search_google_cse(
    query: str,
    per_query: int,
    session: requests.Session,
    api_key: str,
    cx: str,
    timeout: float,
) -> List[str]:
    links: List[str] = []
    start = 1
    # Google CSE returns up to 10 results per page.
    while len(links) < per_query and start <= 91:
        num = min(10, per_query - len(links))
        params = {
            "q": query,
            "key": api_key,
            "cx": cx,
            "num": num,
            "start": start,
        }
        resp = session.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as exc:
            print(f"[WARN] malformed Google CSE JSON for query {query!r}: {exc}")
            return []
        items = data.get("items", []) or []
        if not items:
            break
        for item in items:
            cleaned = resolve_discovered_url((item.get("link", "") or "").strip(), session=session, timeout=timeout)
            title = (item.get("title", "") or "").strip()
            snippet = (item.get("snippet", "") or "").strip()
            if cleaned and is_candidate_result(cleaned, title=title, snippet=snippet, source_query=query):
                links.append(cleaned)
            if len(links) >= per_query:
                break
        start += num
    return links


def search_brave_web(
    query: str,
    per_query: int,
    session: requests.Session,
    api_key: str,
    timeout: float,
) -> List[str]:
    links: List[str] = []
    offset = 0

    while len(links) < per_query:
        count = min(20, per_query - len(links))
        params = {
            "q": query,
            "count": count,
            "offset": offset,
            "country": "us",
            "search_lang": "en",
        }
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        }
        resp = session.get(
            "https://api.search.brave.com/res/v1/web/search",
            params=params,
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as exc:
            print(f"[WARN] malformed Brave Search JSON for query {query!r}: {exc}")
            return []

        items = ((data.get("web") or {}).get("results") or [])
        if not items:
            break
        for item in items:
            cleaned = resolve_discovered_url(
                (item.get("url", "") or item.get("link", "") or "").strip(),
                session=session,
                timeout=timeout,
            )
            title = (item.get("title", "") or "").strip()
            snippet_parts = []
            description = (item.get("description", "") or "").strip()
            if description:
                snippet_parts.append(description)
            extra_snippets = item.get("extra_snippets", []) or []
            snippet_parts.extend(str(value).strip() for value in extra_snippets if str(value).strip())
            snippet = " ".join(snippet_parts)
            if cleaned and is_candidate_result(cleaned, title=title, snippet=snippet, source_query=query):
                links.append(cleaned)
            if len(links) >= per_query:
                break
        if len(items) < count:
            break
        offset += count

    return links


def search_openlibrary_subject(
    subject: str,
    limit: int,
    session: requests.Session,
    timeout: float,
) -> List[Dict[str, str]]:
    params = {
        "subject": subject,
        "sort": "new",
        "limit": max(1, limit),
        "language": "eng",
    }
    resp = session.get("https://openlibrary.org/search.json", params=params, timeout=timeout)
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as exc:
        print(f"[WARN] malformed Open Library JSON for subject {subject!r}: {exc}")
        return []

    docs = data.get("docs", []) or []
    out: List[Dict[str, str]] = []
    for doc in docs:
        work_key = (doc.get("key", "") or "").strip()
        if not work_key.startswith("/works/"):
            continue
        out.append(
            {
                "work_key": work_key,
                "title": (doc.get("title", "") or "").strip(),
                "author": ", ".join(doc.get("author_name", []) or []).strip(),
            }
        )
    return out


def is_preferred_candidate_url(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if "goodreads.com" in host and "/book/show/" in path:
        return True
    if "amazon.com" in host and any(snippet in path for snippet in ("/dp/", "/gp/product/")):
        return True
    if "barnesandnoble.com" in host and "/w/" in path:
        return True
    return False


def extract_openlibrary_candidate_links(
    work_url: str,
    session: requests.Session,
    timeout: float,
    max_links: int,
) -> List[str]:
    try:
        resp = session.get(work_url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[WARN] Open Library work failed: {work_url!r}: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    promoted: List[str] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        raw = (a.get("href") or "").strip()
        normalized = resolve_discovered_url(raw, session=session, timeout=timeout)
        if not normalized:
            normalized = resolve_discovered_url(urljoin(work_url, raw), session=session, timeout=timeout)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        host = urlparse(normalized).netloc.lower()
        if any(snippet in host for snippet in OPENLIBRARY_BLOCKED_EXTERNAL_HOST_SNIPPETS):
            continue
        context_text = link_context_text(a)
        if is_promotable_author_outbound_link(
            normalized,
            anchor_text=a.get_text(" ", strip=True),
            context_text=context_text,
        ):
            promoted.append(normalized)

    deduped: List[str] = []
    seen_ordered = set()
    for url in promoted:
        if url in seen_ordered:
            continue
        seen_ordered.add(url)
        deduped.append(url)
        if len(deduped) >= max(0, max_links):
            break
    return deduped


def harvest_goodreads_candidates(
    session: requests.Session,
    per_seed: int,
    pages_per_seed: int,
    timeout: float,
    outbound_per_url: int,
    include_outbound: bool,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    visited_goodreads_pages = set()

    if not include_outbound or outbound_per_url <= 0:
        return out

    for seed_url, source_label in GOODREADS_SEEDS:
        for page_num in range(1, max(1, pages_per_seed) + 1):
            seed_page_url = seed_url if page_num == 1 else f"{seed_url}?page={page_num}"
            try:
                resp = session.get(seed_page_url, timeout=timeout)
                resp.raise_for_status()
            except requests.RequestException as exc:
                print(f"[WARN] Goodreads seed failed: {seed_page_url!r}: {exc}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            links: List[str] = []
            for a in soup.find_all("a", href=True):
                href = a.get("href", "").strip()
                if "/book/show/" not in href and "/author/show/" not in href:
                    continue
                cleaned = normalize_url(clean_result_url(href))
                if not cleaned:
                    cleaned = normalize_url(clean_result_url("https://www.goodreads.com" + href))
                if cleaned and is_candidate_url(cleaned, source_query=source_label):
                    links.append(cleaned)
                if len(links) >= per_seed:
                    break

            for url in links:
                if url in visited_goodreads_pages:
                    continue
                visited_goodreads_pages.add(url)
                try:
                    detail_resp = session.get(url, timeout=timeout)
                    detail_resp.raise_for_status()
                except requests.RequestException:
                    continue
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                for outbound_url in extract_goodreads_outbound_links(
                    base_url=url,
                    soup=detail_soup,
                    limit=outbound_per_url,
                    session=session,
                    timeout=timeout,
                ):
                    out.append(
                        {
                            "CandidateURL": outbound_url,
                            "SourceQuery": f"{source_label}:author-outbound:p{page_num}",
                            "DiscoveredAtUTC": timestamp,
                        }
                    )
    return out


def rotate_values(values: List[T], offset: int) -> List[T]:
    if not values:
        return values
    shift = offset % len(values)
    if shift == 0:
        return values
    return values[shift:] + values[:shift]


def rotate_rows(rows: List[Dict[str, str]], offset: int) -> List[Dict[str, str]]:
    return rotate_values(rows, offset)


def harvest_openlibrary_candidates(
    session: requests.Session,
    per_subject: int,
    timeout: float,
    rotation_offset: int,
    max_links_per_work: int,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    subjects = rotate_values(list(OPENLIBRARY_SUBJECTS), rotation_offset)

    # Focus each run on a small rotating subject window to reduce duplicates.
    subject_window = min(4, len(subjects))
    for subject in subjects[:subject_window]:
        docs = search_openlibrary_subject(
            subject=subject,
            limit=max(1, per_subject),
            session=session,
            timeout=timeout,
        )
        for doc in rotate_values(docs, rotation_offset):
            work_url = normalize_url(urljoin("https://openlibrary.org", doc.get("work_key", "")))
            if not work_url:
                continue
            for candidate_url in extract_openlibrary_candidate_links(
                work_url=work_url,
                session=session,
                timeout=timeout,
                max_links=max(1, max_links_per_work),
            ):
                out.append(
                    {
                        "CandidateURL": candidate_url,
                        "SourceQuery": f"openlibrary:subject:{slugify_label(subject)}",
                        "DiscoveredAtUTC": timestamp,
                    }
                )
    return out


def looks_like_directory_author_context(text: str) -> bool:
    normalized = normalize_result_text(text)
    if not normalized:
        return False
    if any(hint in normalized for hint in DIRECTORY_AUTHOR_NEGATIVE_HINTS):
        return False
    return any(hint in normalized for hint in DIRECTORY_AUTHOR_POSITIVE_HINTS)


def score_directory_candidate_row(row: Dict[str, str]) -> int:
    source_title = (row.get("SourceTitle", "") or "").strip()
    source_snippet = normalize_result_text(row.get("SourceSnippet", ""))
    candidate_url = row.get("CandidateURL", "")
    host_label = registrable_domain(candidate_url).split(".", 1)[0]

    score = 0
    if looks_like_person_name(source_title):
        score += 3
    if "author of" in source_snippet:
        score += 4
    if any(hint in source_snippet for hint in DIRECTORY_BOOK_HINTS):
        score += 2
    if any(hint in source_snippet for hint in ("book 1", "available now", "new release")):
        score += 2
    if any(token in source_snippet for token in ("fantasy", "sci-fi", "scifi", "romance", "thriller", "horror")):
        score += 1
    if any(token in host_label for token in ("author", "books", "writes", "writer")):
        score += 1
    return score


def prioritize_directory_candidates(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    scored = [(-score_directory_candidate_row(row), idx, row) for idx, row in enumerate(rows)]
    scored.sort()
    return [item[2] for item in scored]


def interleave_candidate_groups(*groups: List[Dict[str, str]]) -> List[Dict[str, str]]:
    pending = [list(group) for group in groups if group]
    out: List[Dict[str, str]] = []
    seen = set()

    while pending:
        next_round: List[List[Dict[str, str]]] = []
        for group in pending:
            while group:
                row = group.pop(0)
                url = row.get("CandidateURL", "")
                if not url or url in seen:
                    continue
                seen.add(url)
                out.append(row)
                break
            if group:
                next_round.append(group)
        pending = next_round
    return out


def rotate_candidate_groups(groups: List[List[Dict[str, str]]], offset: int) -> List[List[Dict[str, str]]]:
    non_empty = [group for group in groups if group]
    return rotate_values(non_empty, offset)


def first_heading_text(node: BeautifulSoup) -> str:
    for selector in ("h1", "h2", "h3", "h4", "strong"):
        heading = node.find(selector)
        if heading and heading.get_text(strip=True):
            return heading.get_text(" ", strip=True)
    return ""


def extract_epic_directory_candidates(
    session: requests.Session,
    timeout: float,
) -> List[Dict[str, str]]:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        resp = session.get(EPIC_DIRECTORY_URL, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[WARN] EPIC directory failed: {describe_request_exception(exc)}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    out: List[Dict[str, str]] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        raw = (anchor.get("href") or "").strip()
        candidate_url = resolve_discovered_url(urljoin(EPIC_DIRECTORY_URL, raw), session=session, timeout=timeout)
        if not candidate_url:
            continue
        if registrable_domain(candidate_url) == registrable_domain(EPIC_DIRECTORY_URL):
            continue

        container = None
        for parent in anchor.parents:
            if getattr(parent, "name", "") in {"article", "section", "div", "li"}:
                text = " ".join(parent.get_text(" ", strip=True).split())
                if len(text) >= 40:
                    container = parent
                    break
        context_text = " ".join(container.get_text(" ", strip=True).split()) if container is not None else ""
        if not looks_like_directory_author_context(context_text):
            continue
        title = first_heading_text(container) if container is not None else ""
        if not is_promotable_author_outbound_link(
            candidate_url,
            anchor_text=title or anchor.get_text(" ", strip=True),
            context_text=context_text,
        ):
            continue
        if candidate_url in seen:
            continue
        seen.add(candidate_url)
        out.append(
            make_candidate_row(
                candidate_url,
                source_type="epic_directory",
                source_query="epic:author-directory",
                timestamp=timestamp,
                source_url=EPIC_DIRECTORY_URL,
                source_title=title,
                source_snippet=context_text,
            )
        )
    return prioritize_directory_candidates(out)


def extract_ian_listing_pages(base_url: str, soup: BeautifulSoup) -> List[str]:
    pages = [base_url]
    seen = {base_url}
    ranked: List[tuple[int, str]] = []
    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(base_url, anchor.get("href", "").strip()))
        if not href or href in seen:
            continue
        parsed = urlparse(href)
        if registrable_domain(parsed.netloc) != registrable_domain(base_url):
            continue
        match = re.search(r"/members-page-(\d+)\.html$", parsed.path, flags=re.IGNORECASE)
        if not match:
            continue
        ranked.append((int(match.group(1)), href))
    for _, href in sorted(ranked):
        if href in seen:
            continue
        seen.add(href)
        pages.append(href)
    return pages


def is_ian_profile_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    if not path.endswith(".html"):
        return False
    if any(
        token in path
        for token in (
            "/author-directory.html",
            "/fiction-directory.html",
            "/non-fiction-directory.html",
            "/contact-us.html",
            "/about-ian.html",
            "/privacy-policy.html",
            "/author-services.html",
            "/member-page-changes.html",
            "/faq.html",
            "/join-ian.html",
            "/book-of-the-year.html",
            "/gold-and-platinum.html",
            "/bronze-and-silver.html",
        )
    ):
        return False
    if "/members-page-" in path:
        return False
    return registrable_domain(parsed.netloc) == registrable_domain(INDEPENDENT_AUTHOR_NETWORK_DIRECTORY_URL)


def extract_ian_profile_links(listing_url: str, soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        href = normalize_url(urljoin(listing_url, anchor.get("href", "").strip()))
        if not href or href in seen or not is_ian_profile_url(href):
            continue
        seen.add(href)
        links.append(href)
    return links


def extract_ian_profile_snippet(soup: BeautifulSoup) -> str:
    meta = soup.find("meta", attrs={"property": "og:description"}) or soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()
    for node in soup.select("div.paragraph"):
        text = " ".join(node.get_text(" ", strip=True).split())
        if len(text) >= 40:
            return text
    return ""


def extract_ian_profile_title(soup: BeautifulSoup) -> str:
    for selector in ("h2", "h1"):
        heading = soup.select_one(selector)
        if heading and heading.get_text(strip=True):
            return heading.get_text(" ", strip=True)
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(" ", strip=True).split(" - ", 1)[0].strip()
    return ""


def extract_ian_directory_candidates(
    session: requests.Session,
    timeout: float,
    rotation_offset: int,
    page_budget: int,
    outbound_per_profile: int,
) -> List[Dict[str, str]]:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        resp = session.get(INDEPENDENT_AUTHOR_NETWORK_DIRECTORY_URL, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[WARN] Independent Author Network directory failed: {describe_request_exception(exc)}")
        return []

    directory_soup = BeautifulSoup(resp.text, "html.parser")
    listing_pages = extract_ian_listing_pages(INDEPENDENT_AUTHOR_NETWORK_DIRECTORY_URL, directory_soup)
    if len(listing_pages) > 1:
        listing_pages = [listing_pages[0]] + rotate_values(listing_pages[1:], rotation_offset)

    out: List[Dict[str, str]] = []
    seen = set()
    for listing_url in listing_pages[: max(1, page_budget)]:
        try:
            if listing_url == INDEPENDENT_AUTHOR_NETWORK_DIRECTORY_URL:
                listing_soup = directory_soup
            else:
                listing_resp = session.get(listing_url, timeout=timeout)
                listing_resp.raise_for_status()
                listing_soup = BeautifulSoup(listing_resp.text, "html.parser")
        except requests.RequestException as exc:
            print(f"[WARN] IAN listing page failed: {listing_url!r}: {describe_request_exception(exc)}")
            continue

        profile_links = rotate_values(extract_ian_profile_links(listing_url, listing_soup), rotation_offset)
        for profile_url in profile_links:
            try:
                profile_resp = session.get(profile_url, timeout=timeout)
                profile_resp.raise_for_status()
            except requests.RequestException:
                continue
            profile_soup = BeautifulSoup(profile_resp.text, "html.parser")
            source_title = extract_ian_profile_title(profile_soup)
            source_snippet = extract_ian_profile_snippet(profile_soup)
            if not looks_like_directory_author_context(source_snippet or source_title):
                continue
            promoted = extract_goodreads_outbound_links(
                base_url=profile_url,
                soup=profile_soup,
                limit=max(1, outbound_per_profile),
                session=session,
                timeout=timeout,
            )
            for candidate_url in promoted:
                if candidate_url in seen:
                    continue
                seen.add(candidate_url)
                out.append(
                    make_candidate_row(
                        candidate_url,
                        source_type="ian_directory",
                        source_query="independent-author-network:author-directory",
                        timestamp=timestamp,
                        source_url=profile_url,
                        source_title=source_title,
                        source_snippet=source_snippet,
                    )
                )
    return prioritize_directory_candidates(out)


def extract_iabx_directory_candidates(
    session: requests.Session,
    timeout: float,
) -> List[Dict[str, str]]:
    timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        resp = session.get(IABX_DIRECTORY_URL, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[WARN] IABX directory failed: {describe_request_exception(exc)}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    page_title = soup.title.get_text(" ", strip=True) if soup.title else "IABX Author Directory"
    out: List[Dict[str, str]] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        anchor_text = " ".join(anchor.get_text(" ", strip=True).split())
        if not looks_like_person_name(anchor_text):
            continue
        raw = (anchor.get("href") or "").strip()
        candidate_url = resolve_discovered_url(urljoin(IABX_DIRECTORY_URL, raw), session=session, timeout=timeout)
        if not candidate_url:
            continue
        if registrable_domain(candidate_url) == registrable_domain(IABX_DIRECTORY_URL):
            continue

        parent = anchor.parent if getattr(anchor.parent, "get_text", None) else None
        context_text = " ".join(parent.get_text(" ", strip=True).split()) if parent is not None else ""
        if len(context_text) < 40:
            context_text = f"{page_title} author directory listing for {anchor_text}"
        if not is_promotable_author_outbound_link(
            candidate_url,
            anchor_text=anchor_text,
            context_text=context_text,
        ):
            continue
        if candidate_url in seen:
            continue
        seen.add(candidate_url)
        out.append(
            make_candidate_row(
                candidate_url,
                source_type="iabx_directory",
                source_query="iabx:author-directory",
                timestamp=timestamp,
                source_url=IABX_DIRECTORY_URL,
                source_title=anchor_text,
                source_snippet=context_text,
            )
        )
    return prioritize_directory_candidates(out)


def harvest(
    queries: Iterable[str],
    per_query: int,
    target: int,
    min_candidates: int,
    pause_seconds: float,
    max_per_domain: int,
    goodreads_pages: int,
    max_goodreads_candidates: int,
    goodreads_outbound_per_url: int,
    include_goodreads_outbound: bool,
    goodreads_rotation_offset: int,
    max_openlibrary_candidates: int,
    openlibrary_per_subject: int,
    include_openlibrary: bool,
    brave_api_key: str,
    google_api_key: str,
    google_cx: str,
    search_timeout: float,
    goodreads_timeout: float,
    http_retries: int,
    harvest_time_budget: float,
    disable_web_fallback: bool,
) -> tuple[List[Dict[str, str]], Dict[str, object]]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=max(0, http_retries),
        read=max(0, http_retries),
        connect=max(0, http_retries),
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    started_at = time.monotonic()
    effective_target = max(1, target, min_candidates)
    out: List[Dict[str, str]] = []
    seen = set()
    per_domain_counts: Dict[str, int] = {}
    rotation_offset = max(0, goodreads_rotation_offset)
    source_counts: Counter[str] = Counter()
    google_status = {
        "status": "other",
        "http_status": 0,
        "error_reason": "",
        "error_message": "",
    }
    if google_api_key and google_cx:
        google_status = google_cse_healthcheck(
            session=session,
            api_key=google_api_key,
            cx=google_cx,
            timeout=max(1.0, search_timeout),
        )
        if google_status["status"] != "ok":
            print(
                "[INFO] Google CSE disabled for this run: "
                f"{google_status['status']} ({google_status.get('http_status', 0)}) "
                f"{google_status.get('error_reason', '')} {google_status.get('error_message', '')}".strip()
            )

    def infer_source_type(row: Dict[str, str]) -> str:
        source_type = (row.get("SourceType", "") or "").strip()
        if source_type:
            return source_type
        lowered = (row.get("SourceQuery", "") or "").lower()
        if lowered.startswith("goodreads:"):
            return "goodreads"
        if lowered.startswith("openlibrary:"):
            return "openlibrary"
        return "web_search"

    def add_row(row: Dict[str, str]) -> bool:
        url = normalize_url(row.get("CandidateURL", "") or "")
        source_query = (row.get("SourceQuery", "") or "").strip()
        if not url or not is_candidate_url(url, source_query=source_query):
            return False
        if url in seen:
            return False
        domain = urlparse(url).netloc.lower()
        if max_per_domain > 0 and per_domain_counts.get(domain, 0) >= max_per_domain:
            return False
        normalized_row = {
            field: (row.get(field, "") or "").strip()
            for field in CANDIDATE_FIELDNAMES
        }
        normalized_row["CandidateURL"] = url
        normalized_row["SourceType"] = infer_source_type(row)
        if not normalized_row["DiscoveredAtUTC"]:
            normalized_row["DiscoveredAtUTC"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        seen.add(url)
        per_domain_counts[domain] = per_domain_counts.get(domain, 0) + 1
        source_counts[normalized_row["SourceType"]] += 1
        out.append(normalized_row)
        return True

    def add_rows(rows: Iterable[Dict[str, str]], limit: int = 0) -> int:
        added = 0
        for row in rows:
            if limit > 0 and added >= limit:
                break
            if len(out) >= effective_target:
                break
            if add_row(row):
                added += 1
        return added

    directory_goal = max(1, int(round(max(1, target) * 0.8)))
    deterministic_goal = max(directory_goal, min_candidates)
    time_budget_seconds = max(10.0, harvest_time_budget)
    use_google = google_status["status"] == "ok"

    def time_budget_remaining() -> bool:
        return (time.monotonic() - started_at) < time_budget_seconds

    if use_google:
        for query in queries:
            if len(out) >= effective_target or not time_budget_remaining():
                break
            results: List[str] = []
            try:
                results = search_google_cse(
                    query=query,
                    per_query=per_query,
                    session=session,
                    api_key=google_api_key,
                    cx=google_cx,
                    timeout=max(1.0, search_timeout),
                )
            except requests.RequestException as exc:
                print(f"[WARN] Google CSE query failed: {query!r}: {describe_request_exception(exc)}")
                results = []

            timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            add_rows(
                (
                    make_candidate_row(
                        url,
                        source_type="google_cse",
                        source_query=query,
                        timestamp=timestamp,
                    )
                    for url in results
                )
            )
            time.sleep(max(0.0, pause_seconds))
    else:
        epic_rows = extract_epic_directory_candidates(
            session=session,
            timeout=max(1.0, goodreads_timeout),
        )
        iabx_rows = extract_iabx_directory_candidates(
            session=session,
            timeout=max(1.0, goodreads_timeout),
        )
        initial_ian_page_budget = min(4, max(2, 2 + (rotation_offset % 3)))
        ian_rows = extract_ian_directory_candidates(
            session=session,
            timeout=max(1.0, goodreads_timeout),
            rotation_offset=rotation_offset,
            page_budget=initial_ian_page_budget,
            outbound_per_profile=max(1, goodreads_outbound_per_url or 2),
        )
        add_rows(
            interleave_candidate_groups(
                *rotate_candidate_groups([epic_rows, ian_rows, iabx_rows], rotation_offset)
            )
        )
        if len(out) < deterministic_goal and time_budget_remaining():
            shortfall = max(0, deterministic_goal - len(out))
            expanded_ian_page_budget = min(10, max(initial_ian_page_budget + 1, 1 + shortfall // 12))
            if expanded_ian_page_budget > initial_ian_page_budget:
                add_rows(
                    interleave_candidate_groups(
                        *rotate_candidate_groups(
                            [
                                extract_ian_directory_candidates(
                                    session=session,
                                    timeout=max(1.0, goodreads_timeout),
                                    rotation_offset=rotation_offset + 1,
                                    page_budget=expanded_ian_page_budget,
                                    outbound_per_profile=max(1, goodreads_outbound_per_url or 2),
                                )
                            ],
                            rotation_offset,
                        )
                    )
                )

        if len(out) < deterministic_goal and include_openlibrary and time_budget_remaining():
            openlibrary_quota = min(max_openlibrary_candidates, max(6, int(round(effective_target * 0.1))))
            add_rows(
                harvest_openlibrary_candidates(
                    session=session,
                    per_subject=max(1, openlibrary_per_subject),
                    timeout=max(1.0, goodreads_timeout),
                    rotation_offset=rotation_offset,
                    max_links_per_work=3,
                ),
                limit=openlibrary_quota,
            )

        if len(out) < deterministic_goal and include_goodreads_outbound and time_budget_remaining():
            goodreads_quota = min(max_goodreads_candidates, max(6, int(round(effective_target * 0.1))))
            add_rows(
                rotate_rows(
                    harvest_goodreads_candidates(
                        session,
                        per_seed=max(10, per_query),
                        pages_per_seed=max(1, goodreads_pages),
                        timeout=max(1.0, goodreads_timeout),
                        outbound_per_url=max(0, goodreads_outbound_per_url),
                        include_outbound=include_goodreads_outbound,
                    ),
                    rotation_offset,
                ),
                limit=goodreads_quota,
            )

    duckduckgo_blocked = False
    allow_web_fallback = not disable_web_fallback and len(out) < effective_target
    if not use_google:
        allow_web_fallback = allow_web_fallback and len(out) >= directory_goal

    if allow_web_fallback:
        for query in queries:
            if len(out) >= effective_target or not time_budget_remaining():
                break
            results: List[str] = []

            if brave_api_key and not use_google:
                try:
                    results = search_brave_web(
                        query=query,
                        per_query=per_query,
                        session=session,
                        api_key=brave_api_key,
                        timeout=max(1.0, search_timeout),
                    )
                except requests.RequestException as exc:
                    print(f"[WARN] Brave Search query failed: {query!r}: {describe_request_exception(exc)}")
                    results = []

            if not results and not duckduckgo_blocked:
                try:
                    results = search_duckduckgo(
                        query,
                        per_query=per_query,
                        session=session,
                        timeout=max(1.0, search_timeout),
                    )
                except requests.RequestException as exc:
                    status_code = getattr(getattr(exc, "response", None), "status_code", None)
                    if status_code == 403:
                        duckduckgo_blocked = True
                        print("[INFO] DuckDuckGo HTML returned 403; skipping it for remaining queries in this run")
                    else:
                        print(f"[WARN] query failed: {query!r}: {describe_request_exception(exc)}")
                    results = []

            if len(results) < max(3, per_query // 2):
                try:
                    html_results = search_bing_html(
                        query,
                        per_query=per_query,
                        session=session,
                        timeout=max(1.0, search_timeout),
                    )
                except requests.RequestException as exc:
                    print(f"[WARN] HTML fallback query failed: {query!r}: {describe_request_exception(exc)}")
                    html_results = []
                if html_results:
                    results = html_results

            if len(results) < max(3, per_query // 2):
                try:
                    fallback = search_bing_rss(
                        query,
                        per_query=per_query,
                        session=session,
                        timeout=max(1.0, search_timeout),
                    )
                except requests.RequestException as exc:
                    print(f"[WARN] fallback query failed: {query!r}: {describe_request_exception(exc)}")
                    fallback = []
                if fallback:
                    results = fallback

            timestamp = dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            add_rows(
                (
                    make_candidate_row(
                        url,
                        source_type="web_search",
                        source_query=query,
                        timestamp=timestamp,
                    )
                    for url in results
                )
            )
            time.sleep(max(0.0, pause_seconds))

    domain_counts = Counter(urlparse(row.get("CandidateURL", "")).netloc.lower() for row in out if row.get("CandidateURL"))
    harvest_shortfall_reason = ""
    if len(out) < min_candidates:
        if not time_budget_remaining():
            harvest_shortfall_reason = "time_budget_reached"
        elif not use_google and len(out) < directory_goal:
            harvest_shortfall_reason = "directory_sources_exhausted"
        elif use_google and google_status["status"] != "ok":
            harvest_shortfall_reason = f"google_cse_{google_status['status']}"
        else:
            harvest_shortfall_reason = "search_sources_exhausted"

    stats = {
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "target": target,
        "min_candidates": min_candidates,
        "effective_target": effective_target,
        "google_cse_status": google_status["status"],
        "google_cse_http_status": google_status["http_status"],
        "google_cse_error_reason": google_status["error_reason"],
        "google_cse_error_message": google_status["error_message"],
        "counts": {
            "harvested_candidates": len(out),
            "directory_goal_before_web_fallback": directory_goal,
            "deterministic_goal": deterministic_goal,
        },
        "per_source": dict(sorted(source_counts.items(), key=lambda item: item[1], reverse=True)),
        "top_domains": [{"domain": domain, "count": count} for domain, count in domain_counts.most_common(15)],
        "harvest_shortfall_reason": harvest_shortfall_reason,
    }
    return out, stats


def describe_request_exception(exc: requests.RequestException) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    reason = getattr(response, "reason", "") or ""
    if status_code is not None:
        if reason:
            return f"HTTP {status_code} {reason}"
        return f"HTTP {status_code}"
    return exc.__class__.__name__


def write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANDIDATE_FIELDNAMES)
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in CANDIDATE_FIELDNAMES} for row in rows])


def main() -> int:
    args = parse_args()
    queries = load_queries(args.queries_file)
    rows, stats = harvest(
        queries=queries,
        per_query=max(1, args.per_query),
        target=max(1, args.target),
        min_candidates=max(1, args.min_candidates),
        pause_seconds=args.pause_seconds,
        max_per_domain=max(0, args.max_per_domain),
        goodreads_pages=max(1, args.goodreads_pages),
        max_goodreads_candidates=max(0, args.max_goodreads_candidates),
        goodreads_outbound_per_url=max(0, args.goodreads_outbound_per_url),
        include_goodreads_outbound=not args.disable_goodreads_outbound,
        max_openlibrary_candidates=max(0, args.max_openlibrary_candidates),
        openlibrary_per_subject=max(1, args.openlibrary_per_subject),
        include_openlibrary=not args.disable_openlibrary,
        brave_api_key=(args.brave_api_key or "").strip(),
        google_api_key=(args.google_api_key or "").strip(),
        google_cx=(args.google_cx or "").strip(),
        goodreads_rotation_offset=max(0, args.goodreads_rotation_offset),
        search_timeout=max(1.0, args.search_timeout),
        goodreads_timeout=max(1.0, args.goodreads_timeout),
        http_retries=max(0, args.http_retries),
        harvest_time_budget=max(10.0, args.harvest_time_budget),
        disable_web_fallback=args.disable_web_fallback,
    )
    write_csv(args.output, rows)
    if args.stats_output:
        with open(args.stats_output, "w", encoding="utf-8") as fh:
            json.dump(stats, fh, indent=2, ensure_ascii=True)
    print(f"[OK] wrote {len(rows)} candidates to {args.output}")
    if len(rows) < args.min_candidates:
        print(
            f"[INFO] gathered fewer than requested minimum ({len(rows)}/{args.min_candidates}). "
            f"Reason: {stats.get('harvest_shortfall_reason', 'unknown')}."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
