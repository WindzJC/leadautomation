from __future__ import annotations

import csv
import io

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import filter_rows
from dashboard.utils import maybe_dataframe


QUEUE_COLUMNS = [
    "AuthorName",
    "BookTitle",
    "AuthorEmail",
    "ContactURL",
    "ContactURLMethod",
    "SourceURL",
    "BookTitleStatus",
    "BookTitleRejectReason",
    "ListingFailReason",
    "RecencyStatus",
    "Location",
]


def _to_csv(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def render_contact_queue(outputs) -> None:
    st.subheader("Contact Queue")
    render_page_intro(
        "Rows worth reviewing next",
        "These rows are contactable or promising, but they are not in the strict verified export yet.",
        bullets=[
            "Use ContactURL and SourceURL to inspect the best outreach path.",
            "BookTitleStatus and ListingFailReason explain why a row stayed in queue.",
        ],
    )
    rows = outputs.get("contact_queue", [])
    if not rows:
        st.info("No contact queue rows found for this run context.")
        return

    query = st.text_input("Search contact queue", placeholder="Author, book, email, or contact URL")
    filtered = filter_rows(rows, query, fields=QUEUE_COLUMNS)
    st.caption(f"{len(filtered)} of {len(rows)} queue rows")
    st.download_button(
        "Download visible rows",
        data=_to_csv(filtered),
        file_name="contact_queue.filtered.csv",
        mime="text/csv",
    )
    st.dataframe(
        maybe_dataframe(filtered),
        use_container_width=True,
        hide_index=True,
        column_config={
            "SourceURL": st.column_config.LinkColumn("SourceURL"),
            "ContactURL": st.column_config.LinkColumn("ContactURL"),
        },
    )
