from __future__ import annotations

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import filter_rows, summarize_counter
from dashboard.utils import maybe_dataframe, safe_get


def render_reject_analysis(run_bundle) -> None:
    st.subheader("Reject Analysis")
    render_page_intro(
        "Why rows failed",
        "Use this page when you need to tune the engine. It is intentionally more diagnostic than the default review pages.",
        bullets=[
            "Final Reject Reasons show what actually stopped strict success.",
            "Listing and location buckets help isolate proof-side bottlenecks.",
            "Candidate states and planned actions show the internal agent-like flow.",
        ],
    )
    validate_stats = run_bundle.get("validate_stats", {})
    if not validate_stats:
        st.info("No validate stats found for this run.")
        return
    rejected_rows = list(run_bundle.get("rejected_rows", []) or [])

    left, right = st.columns(2)
    with left:
        st.markdown("**Final Reject Reasons**")
        reject_rows = summarize_counter(validate_stats.get("reject_reasons"), limit=20)
        if reject_rows:
            reject_frame = maybe_dataframe(reject_rows)
            try:
                st.bar_chart(reject_frame.set_index("label"))
            except Exception:
                pass
            st.dataframe(reject_frame, use_container_width=True, hide_index=True)
        else:
            st.info("No final reject stats found.")
        st.markdown("**Listing Reject Buckets**")
        listing_rows = summarize_counter(validate_stats.get("listing_reject_reason_counts"), limit=20)
        if listing_rows:
            listing_frame = maybe_dataframe(listing_rows)
            try:
                st.bar_chart(listing_frame.set_index("label"))
            except Exception:
                pass
            st.dataframe(listing_frame, use_container_width=True, hide_index=True)
        else:
            st.info("No listing buckets found.")
        st.markdown("**Location Decision Counts**")
        location_rows = summarize_counter(validate_stats.get("location_decision_counts"), limit=20)
        if location_rows:
            location_frame = maybe_dataframe(location_rows)
            try:
                st.bar_chart(location_frame.set_index("label"))
            except Exception:
                pass
            st.dataframe(location_frame, use_container_width=True, hide_index=True)
        else:
            st.info("No location decision counts found.")
    with right:
        st.markdown("**Candidate States**")
        st.dataframe(
            maybe_dataframe(summarize_counter(validate_stats.get("candidate_state_counts"), limit=20)),
            use_container_width=True,
            hide_index=True,
        )
        st.markdown("**Planned Actions**")
        st.dataframe(
            maybe_dataframe(summarize_counter(validate_stats.get("planned_action_counts"), limit=20)),
            use_container_width=True,
            hide_index=True,
        )
        st.markdown("**Intake Scoring**")
        intake = safe_get(validate_stats, "intake_scoring", {})
        if intake:
            st.json(
                {
                    "distribution": intake.get("prevalidate_candidate_score_distribution", {}),
                    "summary": intake.get("prevalidate_candidate_score_summary", {}),
                    "average_candidate_score_by_outcome": intake.get("average_candidate_score_by_outcome", {}),
                    "top_high_score_failure_reasons": intake.get("top_high_score_failure_reasons", {}),
                },
                expanded=False,
            )
        else:
            st.info("No intake-scoring calibration recorded for this run.")

    st.markdown("**Rejected Leads**")
    if rejected_rows:
        search_query = st.text_input("Filter rejected leads", value="", key="rejected_leads_filter")
        reason_options = ["All"] + sorted(
            {
                str(row.get("PrimaryFailReason", "") or "").strip()
                for row in rejected_rows
                if str(row.get("PrimaryFailReason", "") or "").strip()
            }
        )
        selected_reason = st.selectbox("Primary fail reason", reason_options, index=0, key="rejected_leads_reason")
        filtered_rows = filter_rows(
            rejected_rows,
            search_query,
            fields=["AuthorName", "BookTitle", "PrimaryFailReason", "RejectReason", "SourceURL", "CandidateURL", "Email"],
        )
        if selected_reason != "All":
            filtered_rows = [row for row in filtered_rows if (row.get("PrimaryFailReason", "") or "").strip() == selected_reason]
        st.dataframe(maybe_dataframe(filtered_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No rejected lead export found for this run.")

    st.markdown("**Top Candidate Budget Burn**")
    top_burn = validate_stats.get("top_candidate_budget_burn", [])
    if top_burn:
        st.dataframe(maybe_dataframe(top_burn), use_container_width=True, hide_index=True)
    else:
        st.info("No candidate budget-burn records available.")
