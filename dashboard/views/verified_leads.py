from __future__ import annotations

import csv
import io

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import filter_rows
from dashboard.utils import maybe_dataframe


def _to_no_header_csv(rows: list[dict[str, object]]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(
            [
                row.get("AuthorName", ""),
                row.get("AuthorEmail", ""),
                row.get("SourceURL", ""),
            ]
        )
    return output.getvalue().encode("utf-8")


def render_verified_leads(outputs) -> None:
    st.subheader("Verified Leads")
    render_page_intro(
        "Outbound-ready rows only",
        "These are the rows that passed the strict verified export path. Use this page for copy/export and quick source checks.",
        bullets=[
            "Search by author, email, or source URL.",
            "Use the SourceURL link to inspect the proof page quickly.",
        ],
    )
    rows = outputs.get("fully_verified_leads", [])
    if not rows:
        st.info("No verified leads found for this run context.")
        return

    query = st.text_input("Search verified leads", placeholder="Author, email, or source URL")
    filtered = filter_rows(rows, query, fields=["AuthorName", "AuthorEmail", "SourceURL"])
    st.caption(f"{len(filtered)} of {len(rows)} verified rows")
    st.download_button(
        "Download visible rows",
        data=_to_no_header_csv(filtered),
        file_name="fully_verified_leads.csv",
        mime="text/csv",
    )
    st.dataframe(
        maybe_dataframe(filtered),
        use_container_width=True,
        hide_index=True,
        column_config={
            "SourceURL": st.column_config.LinkColumn("SourceURL"),
            "AuthorEmail": st.column_config.TextColumn("AuthorEmail"),
        },
    )
