from __future__ import annotations

from pathlib import Path

from persistent_dedupe import PersistentDedupeStore, fingerprint_from_fields, normalized_identity_fields


def _row(**overrides: str) -> dict[str, str]:
    base = {
        "AuthorName": "Jane Doe",
        "AuthorEmail": "jane@example.com",
        "SourceURL": "https://janedoeauthor.com/about",
        "ListingURL": "https://www.amazon.com/dp/B012345678",
        "BookTitleSourceURL": "",
    }
    base.update(overrides)
    return base


def test_persistent_dedupe_survives_across_store_instances(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "dedupe.sqlite"
    first = PersistentDedupeStore(db_path)

    keep_rows, duplicate_rows = first.filter_new_rows([_row()], run_id="run_001")
    assert len(keep_rows) == 1
    assert duplicate_rows == []

    second = PersistentDedupeStore(db_path)
    keep_rows_2, duplicate_rows_2 = second.filter_new_rows([_row()], run_id="run_002")
    assert keep_rows_2 == []
    assert len(duplicate_rows_2) == 1
    assert duplicate_rows_2[0]["DuplicateOfRunID"] == "run_001"


def test_fingerprint_normalizes_author_email_and_urls() -> None:
    row_a = _row(
        AuthorName="  JANE   DOE ",
        AuthorEmail="JANE@EXAMPLE.COM",
        SourceURL="https://janedoeauthor.com/about?utm_source=test",
        ListingURL="https://www.amazon.com/dp/B012345678?ref_=abc",
    )
    row_b = _row(
        AuthorName="jane doe",
        AuthorEmail="jane@example.com",
        SourceURL="https://janedoeauthor.com/about",
        ListingURL="https://www.amazon.com/dp/B012345678",
    )

    assert fingerprint_from_fields(normalized_identity_fields(row_a)) == fingerprint_from_fields(
        normalized_identity_fields(row_b)
    )


def test_persistent_dedupe_matches_same_author_without_email(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "dedupe.sqlite"
    store = PersistentDedupeStore(db_path)

    keep_rows, duplicate_rows = store.filter_new_rows(
        [
            _row(
                AuthorEmail="",
                SourceURL="https://janedoeauthor.com/about",
                ListingURL="",
                BookTitleSourceURL="https://www.amazon.com/dp/B012345678?ref_=abc",
            )
        ],
        run_id="run_001",
    )
    assert len(keep_rows) == 1
    assert duplicate_rows == []

    later_keep, later_duplicates = PersistentDedupeStore(db_path).filter_new_rows(
        [
            _row(
                AuthorEmail="jane@example.com",
                SourceURL="https://janedoeauthor.com/about?utm_source=test",
                ListingURL="https://www.amazon.com/dp/B012345678",
            )
        ],
        run_id="run_002",
    )

    assert later_keep == []
    assert len(later_duplicates) == 1
    assert later_duplicates[0]["DuplicateOfRunID"] == "run_001"


def test_remember_rows_seeds_existing_outputs_for_future_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "dedupe.sqlite"
    store = PersistentDedupeStore(db_path)
    store.remember_rows([_row()], run_id="existing_master")

    keep_rows, duplicate_rows = PersistentDedupeStore(db_path).filter_new_rows([_row()], run_id="run_003")

    assert keep_rows == []
    assert len(duplicate_rows) == 1
    assert duplicate_rows[0]["DuplicateOfRunID"] == "existing_master"
