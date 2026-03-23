from __future__ import annotations

import csv
import io

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.data_loader import build_funnel_rows, summarize_counter
from dashboard.utils import maybe_dataframe, metric_value, path_to_uri


def _render_counter_block(title: str, counter: dict | list | None, *, limit: int = 10) -> None:
    st.markdown(f"**{title}**")
    if isinstance(counter, list):
        rows = []
        for row in counter:
            if not isinstance(row, dict) or "count" not in row:
                continue
            label = row.get("label", row.get("domain", ""))
            rows.append({"label": str(label), "count": row.get("count", 0)})
        rows = rows[:limit]
    else:
        rows = summarize_counter(counter, limit=limit)
    if not rows:
        st.info(f"No {title.lower()} available.")
        return
    frame = maybe_dataframe(rows)
    try:
        st.bar_chart(frame.set_index("label"))
    except Exception:
        pass
    st.dataframe(frame, use_container_width=True, hide_index=True)


def _csv_bytes(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def render_overview(context, outputs, run_bundle, run_tag: str) -> None:
    st.subheader("Overview")
    st.caption(f"Context: `{context.root_path}` | Run tag: `run_{run_tag}`")
    render_page_intro(
        "Understand the latest run quickly",
        "Start here to see counts, the main blockers, and the files you are most likely to open next.",
        bullets=[
            "Use Verified Leads for outbound-ready rows.",
            "Use Contact Queue for reviewable but not-yet-verified rows.",
            "Use Near Misses for location-only strict failures.",
        ],
    )

    run_stats = run_bundle.get("stats", {})
    validate_stats = run_bundle.get("validate_stats", {})
    run_manifest = run_bundle.get("run_manifest", {})
    counts = run_stats.get("counts", {})
    validator = run_stats.get("validator", {})
    agent_hunt = run_stats.get("agent_hunt", {}) if isinstance(run_stats.get("agent_hunt"), dict) else {}
    validation_profile = str(run_stats.get("validation_profile") or "")
    scout_mode = validation_profile == "agent_hunt" or bool(agent_hunt)
    manifest_counts = dict(run_manifest.get("counts", {}) or {})
    rejected_rows = list(run_bundle.get("rejected_rows", []) or [])
    rejected_count = int(manifest_counts.get("rejected", len(rejected_rows)) or 0)
    duplicate_count = int(manifest_counts.get("duplicates", counts.get("duplicate_count", 0)) or 0)

    metric_cols = st.columns(6)
    metric_cols[0].metric("Harvested", metric_value(run_stats.get("harvested_candidates", 0)))
    metric_cols[1].metric("Filtered", metric_value(manifest_counts.get("filtered_candidates", counts.get("candidates", 0))))
    metric_cols[2].metric("Validated", metric_value(manifest_counts.get("validated", validate_stats.get("kept_rows", 0))))
    if scout_mode:
        metric_cols[3].metric(
            "Scouted Progress",
            f"{metric_value(agent_hunt.get('scouted_progress', 0))}/{metric_value(agent_hunt.get('scouted_target', 0))}",
        )
        metric_cols[4].metric("Scouted Rows", metric_value(agent_hunt.get("scouted_rows_written", len(outputs.get("scouted_leads", [])))))
        metric_cols[5].metric("Strict Rows", metric_value(agent_hunt.get("strict_rows_written", len(outputs.get("fully_verified_leads", [])))))
    else:
        metric_cols[3].metric("Rejected", metric_value(rejected_count))
        metric_cols[4].metric("Duplicates", metric_value(duplicate_count))
        metric_cols[5].metric("Added To Master", metric_value(manifest_counts.get("added_to_master", counts.get("added_to_master", 0))))

    top_reject_source = agent_hunt.get("top_reject_reasons") if scout_mode else (
        run_manifest.get("rejected_reason_counts") or validate_stats.get("reject_reasons")
    )
    top_rejects = summarize_counter(top_reject_source, limit=3)
    if top_rejects:
        summary_text = ", ".join(f"{row['label']} ({row['count']})" for row in top_rejects)
        st.info(f"Main blockers in this run: {summary_text}")

    st.markdown("**Main output files**")
    output_cols = st.columns(4)
    for idx, (label, path) in enumerate(outputs.get("paths", {}).items()):
        with output_cols[idx % 4]:
            st.markdown(f"- [{label}]({path_to_uri(path)})")
            st.caption(str(path))

    summary_payload = {
        "run_folder": str(context.root_path),
        "run_tag": f"run_{run_tag}",
        "harvested": metric_value(run_stats.get("harvested_candidates", 0)),
        "validated": metric_value(validator.get("validated", validate_stats.get("kept_rows", 0))),
        "final": metric_value(validator.get("final", counts.get("final", 0))),
        "verified_progress": metric_value(validate_stats.get("verified_progress", 0)),
        "verified_target": metric_value(validate_stats.get("verified_target", 0)),
        "scouted_progress": metric_value(agent_hunt.get("scouted_progress", 0)),
        "scouted_target": metric_value(agent_hunt.get("scouted_target", 0)),
        "scouted_rows_written": metric_value(agent_hunt.get("scouted_rows_written", len(outputs.get("scouted_leads", [])))),
        "strict_rows_written": metric_value(agent_hunt.get("strict_rows_written", len(outputs.get("fully_verified_leads", [])))),
        "batch_runtime_exceeded": validate_stats.get("batch_runtime_exceeded"),
        "exhausted_before_target": validate_stats.get("exhausted_before_target"),
    }
    st.download_button(
        "Download run summary",
        data=_csv_bytes([summary_payload]),
        file_name=f"run_{run_tag}_summary.csv",
        mime="text/csv",
    )

    summary_tab, breakdown_tab = st.tabs(["Summary", "Failure Breakdown"])
    with summary_tab:
        left, right = st.columns(2)
        with left:
            st.markdown("**Funnel Health**")
            funnel_rows = build_funnel_rows(run_bundle)
            if funnel_rows:
                st.dataframe(maybe_dataframe(funnel_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No funnel data available.")
            st.markdown("**Top Reject Reasons**")
            _render_counter_block("Top Reject Reasons", top_reject_source)
        with right:
            _render_counter_block("Top Location Buckets", validate_stats.get("location_decision_counts"))

    with breakdown_tab:
        left, right = st.columns(2)
        with left:
            if scout_mode:
                _render_counter_block("Scout Gate Reject Reasons", agent_hunt.get("scout_gate_reject_reasons"))
                _render_counter_block("Scouted Source Domains", agent_hunt.get("scouted_source_domains"))
                _render_counter_block("Strict Source Domains", agent_hunt.get("strict_source_domains"))
            else:
                _render_counter_block("Top Listing Buckets", validate_stats.get("listing_reject_reason_counts"))
        with right:
            if scout_mode:
                _render_counter_block("Top Listing Buckets", validate_stats.get("listing_reject_reason_counts"))
            with st.expander("Run health and planner state", expanded=True):
                st.json(
                    {
                        "validation_profile": validation_profile,
                        "pipeline_exit_code": run_stats.get("pipeline_exit_code"),
                        "batch_runtime_exceeded": validate_stats.get("batch_runtime_exceeded"),
                        "exhausted_before_target": validate_stats.get("exhausted_before_target"),
                        "candidate_state_counts": validate_stats.get("candidate_state_counts", {}),
                        "planned_action_counts": validate_stats.get("planned_action_counts", {}),
                        "agent_hunt": agent_hunt,
                    },
                    expanded=False,
                )
