from __future__ import annotations

from pathlib import Path
from typing import Any, MutableMapping

import streamlit as st

from dashboard.components import render_page_intro
from dashboard.runner import (
    DEFAULT_RUN_CONFIG,
    build_run_paths,
    get_run_status,
    load_run_form_state,
    save_run_form_state,
    start_run,
    stop_run,
    suggested_run_folder_name,
    tail_log,
)
from dashboard.utils import path_to_uri

VALIDATION_PROFILES = [
    "agent_hunt",
    "strict_full",
    "strict_interactive",
    "fully_verified",
    "verified_no_us",
    "astra_outbound",
]

FORM_SESSION_KEYS = {
    "validation_profile": "run_validation_profile",
    "goal_final": "run_goal_final",
    "max_runs": "run_max_runs",
    "max_stale_runs": "run_max_stale_runs",
    "listing_strict": "run_listing_strict",
    "target": "run_target",
    "min_candidates": "run_min_candidates",
    "max_candidates": "run_max_candidates",
    "batch_min": "run_batch_min",
    "batch_max": "run_batch_max",
    "queries_file": "run_queries_file",
    "run_folder_name": "run_folder_name",
}

PROFILE_PRESETS = {
    "agent_hunt": {
        "validation_profile": "agent_hunt",
        "listing_strict": False,
        "goal_final": 20,
        "max_runs": 1,
        "max_stale_runs": 1,
        "target": 60,
        "min_candidates": 40,
        "max_candidates": 40,
        "batch_min": 20,
        "batch_max": 20,
    },
    "strict_full": {
        "validation_profile": "strict_full",
        "listing_strict": True,
        "goal_final": 20,
        "max_runs": 1,
        "max_stale_runs": 1,
        "target": 80,
        "min_candidates": 80,
        "max_candidates": 80,
        "batch_min": 20,
        "batch_max": 20,
    },
}

def merged_form_defaults(status_config: dict[str, Any] | None = None) -> dict[str, Any]:
    status_config = status_config or {}
    profile = str(status_config.get("validation_profile") or DEFAULT_RUN_CONFIG["validation_profile"])
    return {**DEFAULT_RUN_CONFIG, **PROFILE_PRESETS.get(profile, {}), **status_config}


def sync_run_form_state(
    session_state: MutableMapping[str, Any],
    *,
    status_config: dict[str, Any] | None,
    run_folder_name: str,
) -> None:
    defaults = merged_form_defaults(status_config)
    defaults["run_folder_name"] = run_folder_name
    for config_key, session_key in FORM_SESSION_KEYS.items():
        session_state.setdefault(session_key, defaults.get(config_key, ""))


def apply_profile_preset_to_session(session_state: MutableMapping[str, Any], profile: str) -> None:
    preset = {**DEFAULT_RUN_CONFIG, **PROFILE_PRESETS.get(profile, {}), "validation_profile": profile}
    for config_key, session_key in FORM_SESSION_KEYS.items():
        if config_key == "run_folder_name":
            continue
        if config_key in preset:
            if config_key == "validation_profile" and session_key in session_state:
                continue
            session_state[session_key] = preset[config_key]


def status_snapshot_changed(previous: dict[str, object], current: dict[str, object]) -> bool:
    keys = ("status_label", "active", "pid", "exit_code", "finished_at")
    return any(previous.get(key) != current.get(key) for key in keys)


def _render_status_banner(status: dict[str, object]) -> None:
    label = str(status.get("status_label") or "idle")
    if label == "running":
        st.success(f"Running: PID {status.get('pid')}")
    elif label == "finished":
        st.success("Finished successfully.")
    elif label == "failed":
        st.error(f"Failed with exit code {status.get('exit_code')}.")
    elif label == "stopped":
        st.warning("Run was stopped.")
    else:
        st.info("No dashboard-started run yet.")


def _render_output_presence(paths: dict[str, Path]) -> None:
    st.markdown("**Output status**")
    output_rows = []
    for label, key in [
        ("Scouted leads", "scouted_output"),
        ("Verified leads", "verified_output"),
        ("Contact queue", "contact_queue_output"),
        ("Near misses", "near_miss_location_output"),
        ("Run stats folder", "runs_dir"),
    ]:
        path = paths[key]
        output_rows.append(
            {
                "Output": label,
                "Ready": "yes" if path.exists() else "no",
                "Path": str(path),
            }
        )
    st.dataframe(output_rows, use_container_width=True, hide_index=True)


def render_run_pipeline() -> None:
    st.subheader("Run Pipeline")
    render_page_intro(
        "Start or monitor a pipeline run",
        "Use this page to launch the existing CLI loop in the background and watch its progress without leaving the browser.",
        bullets=[
            "Use conservative settings first. This page does not change validation rules.",
            "Each browser-started run writes into its own top-level folder.",
            "After a run starts, switch to Overview, Verified Leads, or Scouted Leads to inspect the new folder.",
        ],
    )

    status = get_run_status()
    status_config = status.get("config", {}) if isinstance(status.get("config"), dict) else {}
    draft_config = load_run_form_state()
    default_folder = status.get("run_root")
    sync_run_form_state(
        st.session_state,
        status_config={**status_config, **draft_config},
        run_folder_name=Path(default_folder).name if default_folder else suggested_run_folder_name(),
    )
    banner_placeholder = st.empty()
    control_cols = st.columns([1, 1, 4])
    with control_cols[0]:
        if st.button("Refresh status", use_container_width=True):
            st.rerun()
    with control_cols[1]:
        if status.get("active") and st.button("Stop run", type="secondary", use_container_width=True):
            stop_run()
            st.rerun()

    with banner_placeholder.container():
        _render_status_banner(status)

    start_tab, monitor_tab = st.tabs(["Start Run", "Monitor Run"])
    submitted = False
    with start_tab:
        left, right = st.columns(2)
        with left:
            validation_profile = st.selectbox(
                "Validation profile",
                VALIDATION_PROFILES,
                key=FORM_SESSION_KEYS["validation_profile"],
            )
            st.caption("Change the profile to change the mode. Use the preset button only if you want the recommended values for that profile.")
            if st.button("Use recommended values for selected profile", use_container_width=True):
                apply_profile_preset_to_session(st.session_state, str(validation_profile))
                st.rerun()
            goal_final = st.number_input(
                "Goal final rows",
                min_value=1,
                key=FORM_SESSION_KEYS["goal_final"],
            )
            max_runs = st.number_input(
                "Max runs",
                min_value=1,
                key=FORM_SESSION_KEYS["max_runs"],
            )
            max_stale_runs = st.number_input(
                "Max stale runs",
                min_value=1,
                key=FORM_SESSION_KEYS["max_stale_runs"],
            )
            listing_strict = st.checkbox(
                "Use --listing-strict",
                key=FORM_SESSION_KEYS["listing_strict"],
            )
        with right:
            target = st.number_input(
                "Harvest target",
                min_value=1,
                key=FORM_SESSION_KEYS["target"],
            )
            min_candidates = st.number_input(
                "Minimum harvested candidates",
                min_value=1,
                key=FORM_SESSION_KEYS["min_candidates"],
            )
            max_candidates = st.number_input(
                "Validator candidate cap",
                min_value=1,
                key=FORM_SESSION_KEYS["max_candidates"],
            )
            batch_min = st.number_input(
                "Batch min",
                min_value=1,
                key=FORM_SESSION_KEYS["batch_min"],
            )
            batch_max = st.number_input(
                "Batch max",
                min_value=1,
                key=FORM_SESSION_KEYS["batch_max"],
            )
        run_folder_name = st.text_input(
            "Output folder name",
            help="Creates a top-level folder with root CSV outputs plus a runs/ directory.",
            key=FORM_SESSION_KEYS["run_folder_name"],
        )
        with st.expander("Optional inputs"):
            queries_file = st.text_input(
                "Optional queries file",
                key=FORM_SESSION_KEYS["queries_file"],
            )
        submitted = st.button("Start pipeline run", type="primary")
        current_form_config = {
            "validation_profile": validation_profile,
            "goal_final": int(goal_final),
            "max_runs": int(max_runs),
            "max_stale_runs": int(max_stale_runs),
            "listing_strict": bool(listing_strict),
            "target": int(target),
            "min_candidates": int(min_candidates),
            "max_candidates": int(max_candidates),
            "batch_min": int(batch_min),
            "batch_max": int(batch_max),
            "queries_file": queries_file,
            "run_folder_name": run_folder_name,
        }
        save_run_form_state(current_form_config)

    if submitted:
        try:
            state = start_run(current_form_config)
        except RuntimeError as exc:
            st.error(str(exc))
        else:
            st.success(f"Started run in {state['run_root']}")
            st.rerun()

    status = get_run_status()
    with monitor_tab:
        auto_refresh = st.checkbox(
            "Auto refresh while running",
            value=True,
            help="Refreshes the monitor tab every 5 seconds while the dashboard-started run is active.",
        )

        def _render_monitor_body() -> None:
            status_now = get_run_status()
            with banner_placeholder.container():
                _render_status_banner(status_now)
            if status_snapshot_changed(status, status_now):
                st.rerun()
            detail_cols = st.columns(4)
            detail_cols[0].metric("Status", str(status_now.get("status_label") or "idle").title())
            detail_cols[1].metric("PID", str(status_now.get("pid") or "-"))
            detail_cols[2].metric("Exit code", str(status_now.get("exit_code") if status_now.get("exit_code") is not None else "-"))
            detail_cols[3].metric("Last log update", str(status_now.get("log_updated_at") or "-"))

            run_root = Path(status_now["run_root"]) if status_now.get("run_root") else None
            if run_root:
                paths = build_run_paths(run_root)
                output_cols = st.columns(4)
                for idx, key in enumerate(
                    [
                        "scouted_output",
                        "verified_output",
                        "contact_queue_output",
                        "near_miss_location_output",
                    ]
                ):
                    with output_cols[idx]:
                        st.markdown(f"- [{key}]({path_to_uri(paths[key])})")
                        st.caption(str(paths[key]))
                _render_output_presence(paths)
            else:
                st.info("Start a run to see its output links here.")

            log_text = tail_log(status_now.get("log_path"))
            st.markdown("**Log tail**")
            if log_text:
                st.code(log_text, language="text")
            else:
                st.info("No log output yet.")

            if status_now.get("command"):
                with st.expander("Command"):
                    st.code(" ".join(status_now["command"]), language="bash")
            if status_now.get("config"):
                with st.expander("Effective run settings"):
                    st.json(status_now["config"], expanded=False)

        if auto_refresh and status.get("active"):
            st.caption("Auto-refresh is on while the run is active.")

            @st.fragment(run_every="5s")
            def _monitor_fragment() -> None:
                _render_monitor_body()

            _monitor_fragment()
        else:
            _render_monitor_body()
