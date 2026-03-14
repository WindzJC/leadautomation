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


def render_scouted_leads(outputs) -> None:
    st.subheader("Scouted Leads")
    render_page_intro(
        "Scout-qualified leads for outreach review",
        "These rows passed the lighter `agent_hunt` qualification layer. They are useful for scouting and manual review, but they are not the strict gold-standard export unless they also appear in Verified Leads.",
        bullets=[
            "Use this page when you want breadth before strict proof completeness.",
            "Keep using Verified Leads for the audited strict set.",
        ],
    )
    rows = outputs.get("scouted_leads", [])
    if not rows:
        st.info("No scouted leads found for this run context.")
        return

    query = st.text_input("Search scouted leads", placeholder="Author, email, or source URL")
    filtered = filter_rows(rows, query, fields=["AuthorName", "AuthorEmail", "SourceURL"])
    st.caption(f"{len(filtered)} of {len(rows)} scouted rows")
    st.download_button(
        "Download visible rows",
        data=_to_no_header_csv(filtered),
        file_name="scouted_leads.csv",
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
