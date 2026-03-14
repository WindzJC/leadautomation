from __future__ import annotations

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import build_candidate_index
from dashboard.utils import path_to_uri, safe_get


def render_candidate_detail(run_bundle) -> None:
    st.subheader("Candidate Detail / Evidence")
    render_page_intro(
        "Inspect one candidate deeply",
        "Use this page only when the default pages are not enough. It surfaces listing attempts, location evidence, and the validated row for one candidate at a time.",
        bullets=[
            "Search by domain or URL first.",
            "Read the attempts tables before opening the raw JSON expanders.",
        ],
    )
    candidates = build_candidate_index(run_bundle)
    if not candidates:
        st.info("No candidate evidence/debug records found for this run.")
        return

    query = st.text_input("Filter candidate detail", placeholder="Candidate URL or domain")
    if query.strip():
        lowered = query.strip().lower()
        candidates = [
            row
            for row in candidates
            if lowered in row["candidate_url"].lower() or lowered in row["candidate_domain"].lower()
        ]
    if not candidates:
        st.warning("No candidate matched that filter.")
        return

    def _label(row: dict[str, object]) -> str:
        candidate_url = str(row.get("candidate_url", ""))
        domain = str(row.get("candidate_domain", "")) or candidate_url
        listing = safe_get(row.get("listing_debug", {}), "reject_reason", "")
        location = safe_get(row.get("location_debug", {}), "final_decision", "")
        return f"{domain} | listing={listing or '-'} | location={location or '-'}"

    selected = st.selectbox("Candidate", candidates, format_func=_label)
    validated_row = selected.get("validated_row", {}) or {}
    listing_debug = selected.get("listing_debug", {}) or {}
    location_debug = selected.get("location_debug", {}) or {}

    summary_cols = st.columns(4)
    summary_cols[0].metric("Domain", str(selected.get("candidate_domain", "")))
    summary_cols[1].metric("Listing reject", str(listing_debug.get("reject_reason", "")) or "-")
    summary_cols[2].metric("Location decision", str(location_debug.get("final_decision", "")) or "-")
    summary_cols[3].metric("Recovered row", "yes" if validated_row else "no")

    candidate_url = str(selected.get("candidate_url", ""))
    if candidate_url:
        st.markdown(f"[Open candidate URL]({candidate_url})")

    if validated_row:
        st.markdown("**Validated summary**")
        st.dataframe(
            [
                {
                    "AuthorName": validated_row.get("AuthorName", ""),
                    "BookTitle": validated_row.get("BookTitle", ""),
                    "AuthorEmail": validated_row.get("AuthorEmail", ""),
                    "SourceURL": validated_row.get("SourceURL", ""),
                    "ListingStatus": validated_row.get("ListingStatus", ""),
                    "RecencyStatus": validated_row.get("RecencyStatus", ""),
                    "Location": validated_row.get("Location", ""),
                }
            ],
            use_container_width=True,
            hide_index=True,
            column_config={"SourceURL": st.column_config.LinkColumn("SourceURL")},
        )

    left, right = st.columns(2)
    with left:
        st.markdown("**Listing evidence**")
        if listing_debug:
            if listing_debug.get("best_listing_candidate_url"):
                st.markdown(
                    f"[Best listing candidate]({listing_debug['best_listing_candidate_url']})"
                )
            attempts = listing_debug.get("listing_attempts", []) or []
            if attempts:
                st.dataframe(attempts, use_container_width=True, hide_index=True)
            with st.expander("Raw listing debug"):
                st.json(listing_debug, expanded=False)
        else:
            st.info("No listing debug record for this candidate.")

    with right:
        st.markdown("**Location evidence**")
        if location_debug:
            selected_source_url = location_debug.get("selected_source_url")
            if selected_source_url:
                st.markdown(f"[Selected location source]({selected_source_url})")
            attempts = location_debug.get("attempts", []) or []
            if attempts:
                st.dataframe(attempts, use_container_width=True, hide_index=True)
            with st.expander("Raw location debug"):
                st.json(location_debug, expanded=False)
        else:
            st.info("No location debug record for this candidate.")

    if validated_row:
        with st.expander("Raw validated row"):
            st.json(validated_row, expanded=False)

    if run_bundle.get("paths", {}).get("pipeline_stdout"):
        log_path = run_bundle["paths"]["pipeline_stdout"]
        with st.expander("Run log path"):
            st.caption(f"Run log: {log_path}")
            st.markdown(f"[Open run log]({path_to_uri(log_path)})")
