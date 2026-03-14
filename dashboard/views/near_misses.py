from __future__ import annotations

import csv
import io

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import filter_rows
from dashboard.utils import maybe_dataframe


def _to_csv(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def render_near_misses(outputs) -> None:
    st.subheader("Near Misses")
    render_page_intro(
        "Strict rows that mostly qualified",
        "This page highlights rows that failed strict mode mainly because author-tied U.S. location proof was still missing or ambiguous.",
        bullets=[
            "Check RejectReason first.",
            "Use CandidateRecoveryURLs to inspect the best next pages manually.",
        ],
    )
    rows = outputs.get("near_miss_location", [])
    if not rows:
        st.info("No near-miss location rows found for this run context.")
        return

    query = st.text_input("Search near misses", placeholder="Author, reject reason, snippet, or source URL")
    filtered = filter_rows(
        rows,
        query,
        fields=[
            "AuthorName",
            "BookTitle",
            "AuthorEmail",
            "SourceURL",
            "RejectReason",
            "BestLocationSnippet",
            "CandidateRecoveryURLs",
        ],
    )
    st.caption(f"{len(filtered)} of {len(rows)} near-miss rows")
    st.download_button(
        "Download visible rows",
        data=_to_csv(filtered),
        file_name="near_miss_location.filtered.csv",
        mime="text/csv",
    )
    st.dataframe(
        maybe_dataframe(filtered),
        use_container_width=True,
        hide_index=True,
        column_config={"SourceURL": st.column_config.LinkColumn("SourceURL")},
    )
