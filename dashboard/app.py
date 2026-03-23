from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.components import apply_app_chrome, render_sidebar_guide  # noqa: E402
from dashboard.data_loader import (  # noqa: E402
    PROJECT_ROOT,
    discover_run_contexts,
    list_run_tags,
    latest_run_tag,
    load_context_outputs,
    load_run_bundle,
    resolve_context,
)
from dashboard.views.candidate_detail import render_candidate_detail  # noqa: E402
from dashboard.views.contact_queue import render_contact_queue  # noqa: E402
from dashboard.views.near_misses import render_near_misses  # noqa: E402
from dashboard.views.overview import render_overview  # noqa: E402
from dashboard.views.reject_analysis import render_reject_analysis  # noqa: E402
from dashboard.views.run_pipeline import render_run_pipeline  # noqa: E402
from dashboard.views.scouted_leads import render_scouted_leads  # noqa: E402
from dashboard.views.verified_leads import render_verified_leads  # noqa: E402


def main() -> None:
    st.set_page_config(page_title="Lead Finder Dashboard", layout="wide")
    apply_app_chrome()
    st.title("Lead Finder Dashboard")
    st.caption("Local operator console for lead-finder runs, queue review, and debug evidence.")

    show_advanced = st.sidebar.checkbox("Show advanced analysis pages", value=False)
    pages = ["Run Pipeline", "Overview", "Verified Leads", "Scouted Leads", "Contact Queue", "Near Misses"]
    if show_advanced:
        pages.extend(["Reject Analysis", "Candidate Detail / Evidence"])
    page = st.sidebar.radio("Page", pages)

    contexts = discover_run_contexts(PROJECT_ROOT)
    if page != "Run Pipeline" and not contexts:
        st.error("No run contexts with `runs/` or `outputs/runs/` were found.")
        return

    context = None
    run_tag = None
    outputs = {}
    run_bundle = {}
    if contexts:
        context_labels = {ctx.label: ctx for ctx in contexts}
        st.sidebar.header("Operator View")
        selected_label = st.sidebar.selectbox("Run folder", list(context_labels.keys()), index=0)
        with st.sidebar.expander("Advanced run selection"):
            manual_path = st.text_input("Manual run folder path", value="")
        context = resolve_context(manual_path) if manual_path.strip() else context_labels[selected_label]
        if context is None:
            st.error("The manual run folder path does not contain a `runs/` or `outputs/runs/` directory.")
            return

        run_tags = list_run_tags(context.runs_path)
        if page != "Run Pipeline" and not run_tags:
            st.error(f"No `run_XXX_stats.json` files were found in {context.runs_path}.")
            return
        if run_tags:
            latest_tag = latest_run_tag(context.runs_path)
            default_index = run_tags.index(latest_tag) if latest_tag in run_tags else len(run_tags) - 1
            run_tag = st.sidebar.selectbox("Run tag", run_tags, index=default_index)
        st.sidebar.caption(str(context.root_path))

        outputs = load_context_outputs(context)
        if run_tag:
            run_bundle = load_run_bundle(context, run_tag)

        st.sidebar.markdown("**Quick counts**")
        st.sidebar.caption(
            f"Verified: {len(outputs.get('fully_verified_leads', []))} | "
            f"Scouted: {len(outputs.get('scouted_leads', []))} | "
            f"Queue: {len(outputs.get('contact_queue', []))} | "
            f"Near misses: {len(outputs.get('near_miss_location', []))}"
        )

    render_sidebar_guide()

    if page == "Run Pipeline":
        render_run_pipeline()
    elif page == "Overview":
        assert context is not None and run_tag is not None
        render_overview(context, outputs, run_bundle, run_tag)
    elif page == "Verified Leads":
        render_verified_leads(outputs)
    elif page == "Scouted Leads":
        render_scouted_leads(outputs)
    elif page == "Contact Queue":
        render_contact_queue(outputs)
    elif page == "Near Misses":
        render_near_misses(outputs)
    elif page == "Reject Analysis":
        render_reject_analysis(run_bundle)
    else:
        render_candidate_detail(run_bundle)


if __name__ == "__main__":
    main()
