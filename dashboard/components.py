from __future__ import annotations

from typing import Iterable

import streamlit as st


def apply_app_chrome() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
            max-width: 1400px;
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(250, 250, 250, 0.08);
            border-radius: 0.75rem;
            padding: 0.4rem 0.6rem;
        }
        .dashboard-guide {
            border: 1px solid rgba(250, 250, 250, 0.08);
            border-radius: 0.85rem;
            padding: 0.9rem 1rem 0.7rem 1rem;
            margin: 0.2rem 0 1rem 0;
            background: rgba(255, 255, 255, 0.02);
        }
        .dashboard-guide h4 {
            margin: 0 0 0.35rem 0;
            font-size: 1rem;
        }
        .dashboard-guide p {
            margin: 0 0 0.4rem 0;
        }
        .dashboard-guide ul {
            margin: 0.2rem 0 0 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_intro(title: str, description: str, *, bullets: Iterable[str] | None = None) -> None:
    bullet_list = "".join(f"<li>{item}</li>" for item in (bullets or []))
    bullet_block = f"<ul>{bullet_list}</ul>" if bullet_list else ""
    st.markdown(
        f"""
        <div class="dashboard-guide">
          <h4>{title}</h4>
          <p>{description}</p>
          {bullet_block}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_guide() -> None:
    with st.sidebar.expander("How to use this dashboard"):
        st.markdown(
            "\n".join(
                [
                    "1. Start a run in `Run Pipeline`.",
                    "2. Open `Overview` to see counts and top blockers.",
                    "3. Review `Verified Leads` for outbound-ready rows.",
                    "4. Review `Scouted Leads` for agent-hunt scouting output when available.",
                    "5. Review `Contact Queue` and `Near Misses` for manual follow-up.",
                    "6. Turn on `Show advanced analysis pages` only when you need raw evidence/debug detail.",
                ]
            )
        )
