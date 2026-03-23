#!/usr/bin/env python3
"""Persistent dedupe store for cross-run lead suppression."""

from __future__ import annotations

import datetime as dt
import hashlib
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from lead_utils import canonical_listing_key, normalize_person_name, strip_tracking_query_params
from pipeline_paths import ensure_parent


def canonicalize_url(value: str) -> str:
    url = strip_tracking_query_params((value or "").strip())
    if not url:
        return ""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    query = urlencode(parse_qsl(parsed.query, keep_blank_values=True), doseq=True)
    normalized = parsed._replace(
        netloc=parsed.netloc.lower(),
        query=query,
        fragment="",
    )
    result = urlunparse(normalized)
    if result.endswith("/") and normalized.path in {"", "/"}:
        return result[:-1]
    return result


def normalized_identity_fields(row: Dict[str, str]) -> Dict[str, str]:
    author_norm = normalize_person_name(row.get("AuthorName", ""))
    email_norm = (row.get("AuthorEmail", "") or "").strip().lower()
    source_url_norm = canonicalize_url(row.get("SourceURL", ""))
    raw_book_url = row.get("ListingURL", "") or row.get("BookTitleSourceURL", "")
    book_url_norm = canonical_listing_key(raw_book_url) or canonicalize_url(raw_book_url)
    return {
        "author_norm": author_norm,
        "email_norm": email_norm,
        "source_url_norm": source_url_norm,
        "book_url_norm": book_url_norm,
    }


def fingerprint_from_fields(fields: Dict[str, str]) -> str:
    joined = "|".join(
        [
            fields.get("author_norm", ""),
            fields.get("email_norm", ""),
            fields.get("source_url_norm", ""),
            fields.get("book_url_norm", ""),
        ]
    )
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def identity_keys_from_fields(fields: Dict[str, str]) -> List[Tuple[str, str]]:
    keys: List[Tuple[str, str]] = []
    if fields.get("email_norm", ""):
        keys.append(("email", f"email:{fields['email_norm']}"))
    if fields.get("author_norm", ""):
        keys.append(("author", f"author:{fields['author_norm']}"))
    if fields.get("book_url_norm", ""):
        keys.append(("listing", f"listing:{fields['book_url_norm']}"))
    if fields.get("source_url_norm", ""):
        keys.append(("source_url", f"source_url:{fields['source_url_norm']}"))
    return keys


class PersistentDedupeStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        ensure_parent(path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_fingerprints (
                    fingerprint TEXT PRIMARY KEY,
                    author_norm TEXT NOT NULL,
                    email_norm TEXT NOT NULL,
                    source_url_norm TEXT NOT NULL,
                    book_url_norm TEXT NOT NULL,
                    first_run_id TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_author ON lead_fingerprints(author_norm)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_email ON lead_fingerprints(email_norm)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_identity_keys (
                    identity_key TEXT PRIMARY KEY,
                    key_type TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    first_run_id TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_identity_key_type ON lead_identity_keys(key_type)")

    def _insert_row(self, conn: sqlite3.Connection, row: Dict[str, str], run_id: str, now_utc: str) -> None:
        fields = normalized_identity_fields(row)
        fingerprint = fingerprint_from_fields(fields)
        conn.execute(
            """
            INSERT OR IGNORE INTO lead_fingerprints (
                fingerprint,
                author_norm,
                email_norm,
                source_url_norm,
                book_url_norm,
                first_run_id,
                created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fingerprint,
                fields["author_norm"],
                fields["email_norm"],
                fields["source_url_norm"],
                fields["book_url_norm"],
                run_id,
                now_utc,
            ),
        )
        for key_type, identity_key in identity_keys_from_fields(fields):
            conn.execute(
                """
                INSERT OR IGNORE INTO lead_identity_keys (
                    identity_key,
                    key_type,
                    fingerprint,
                    first_run_id,
                    created_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (identity_key, key_type, fingerprint, run_id, now_utc),
            )

    def remember_rows(self, rows: Iterable[Dict[str, str]], run_id: str) -> None:
        now_utc = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self._connect() as conn:
            for row in rows:
                self._insert_row(conn, row, run_id=run_id, now_utc=now_utc)

    def filter_new_rows(self, rows: Iterable[Dict[str, str]], run_id: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        keep: List[Dict[str, str]] = []
        duplicates: List[Dict[str, str]] = []
        now_utc = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

        with self._connect() as conn:
            for row in rows:
                fields = normalized_identity_fields(row)
                fingerprint = fingerprint_from_fields(fields)
                identity_keys = identity_keys_from_fields(fields)
                existing = None
                if identity_keys:
                    placeholders = ", ".join("?" for _ in identity_keys)
                    existing = conn.execute(
                        f"""
                        SELECT identity_key, fingerprint, first_run_id
                        FROM lead_identity_keys
                        WHERE identity_key IN ({placeholders})
                        ORDER BY created_at_utc ASC
                        LIMIT 1
                        """,
                        tuple(identity_key for _, identity_key in identity_keys),
                    ).fetchone()
                if existing is None:
                    existing = conn.execute(
                        "SELECT fingerprint, first_run_id FROM lead_fingerprints WHERE fingerprint = ?",
                        (fingerprint,),
                    ).fetchone()
                if existing is not None:
                    dup_row = dict(row)
                    dup_row["DedupFingerprint"] = str(existing["fingerprint"] or fingerprint)
                    dup_row["DuplicateOfRunID"] = str(existing["first_run_id"] or "")
                    duplicates.append(dup_row)
                    continue

                self._insert_row(conn, row, run_id=run_id, now_utc=now_utc)
                keep.append(row)

        return keep, duplicates
