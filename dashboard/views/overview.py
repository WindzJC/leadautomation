from __future__ import annotations

import csv
import io

import streamlit as st

from dashboard.data_loader import summarize_counter
from dashboard.utils import maybe_dataframe, metric_value, path_to_uri


def _render_counter_block(title: str, counter: dict | None, *, limit: int = 10) -> None:
    st.markdown(f"**{title}**")
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

    run_stats = run_bundle.get("stats", {})
    validate_stats = run_bundle.get("validate_stats", {})
    counts = run_stats.get("counts", {})
    validator = run_stats.get("validator", {})

    metric_cols = st.columns(6)
    metric_cols[0].metric("Harvested", metric_value(run_stats.get("harvested_candidates", 0)))
    metric_cols[1].metric(
        "Validated",
        metric_value(validator.get("validated", validate_stats.get("kept_rows", 0))),
    )
    metric_cols[2].metric("Final", metric_value(validator.get("final", counts.get("final", 0))))
    metric_cols[3].metric(
        "Verified Progress",
        f"{metric_value(validate_stats.get('verified_progress', 0))}/{metric_value(validate_stats.get('verified_target', 0))}",
    )
    metric_cols[4].metric("Near Misses", metric_value(validate_stats.get("near_miss_location_rows", 0)))
    metric_cols[5].metric("Email Rows", metric_value(outputs.get("author_email_book_rows", 0)))

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
        "batch_runtime_exceeded": validate_stats.get("batch_runtime_exceeded"),
        "exhausted_before_target": validate_stats.get("exhausted_before_target"),
    }
    st.download_button(
        "Download run summary",
        data=_csv_bytes([summary_payload]),
        file_name=f"run_{run_tag}_summary.csv",
        mime="text/csv",
    )

    left, right = st.columns(2)
    with left:
        _render_counter_block("Top Reject Reasons", validate_stats.get("reject_reasons"))
        _render_counter_block("Top Listing Buckets", validate_stats.get("listing_reject_reason_counts"))

    with right:
        _render_counter_block("Top Location Buckets", validate_stats.get("location_decision_counts"))

        st.markdown("**Run health**")
        st.json(
            {
                "pipeline_exit_code": run_stats.get("pipeline_exit_code"),
                "batch_runtime_exceeded": validate_stats.get("batch_runtime_exceeded"),
                "exhausted_before_target": validate_stats.get("exhausted_before_target"),
                "candidate_state_counts": validate_stats.get("candidate_state_counts", {}),
                "planned_action_counts": validate_stats.get("planned_action_counts", {}),
            },
            expanded=False,
        )
