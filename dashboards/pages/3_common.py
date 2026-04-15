from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboards.data import load_common_summary

_TABLE_COLUMNS = [
    "repo_full_name",
    "owner_login",
    "language",
    "repo_total_events",
    "repo_avg_composite_score",
    "stargazers_count",
    "org_name",
    "org_location",
    "org_company",
    "org_followers",
    "org_public_repos",
    "org_is_verified",
]


def load_common_data() -> pd.DataFrame:
    with st.spinner("Loading common repository and organization data from Athena..."):
        return load_common_summary()


def render_dashboard(dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        st.warning("No rows found in the common dashboard table.")
        return

    st.subheader("Repository and Organization Summary Table")
    st.dataframe(
        dataframe[_TABLE_COLUMNS].sort_values(
            ["repo_total_events", "stargazers_count"], ascending=[False, False]
        ),
        use_container_width=True,
        hide_index=True,
    )

    location_language_distribution = (
        dataframe.groupby(["org_location", "language"], as_index=False)[
            "repo_total_events"
        ]
        .sum()
        .sort_values("repo_total_events", ascending=False)
    )

    st.subheader("Repository Activity vs Organization Reach")
    st.plotly_chart(
        px.scatter(
            dataframe,
            x="stargazers_count",
            y="repo_total_events",
            size="org_followers",
            color="language",
            hover_name="repo_full_name",
            hover_data=[
                "org_name",
                "org_location",
                "org_company",
                "repo_avg_composite_score",
            ],
            title="Repository activity versus organization reach",
            labels={
                "stargazers_count": "Repository stargazers",
                "repo_total_events": "Repository total events",
                "org_followers": "Organization followers",
                "language": "Language",
            },
            log_x=True,
            log_y=True,
        ),
        use_container_width=True,
    )

    st.subheader("Location and Language Composition")
    st.plotly_chart(
        px.treemap(
            location_language_distribution,
            path=["org_location", "language"],
            values="repo_total_events",
            title="Repository activity by organization location and language",
        ),
        use_container_width=True,
    )


st.title("Common Dashboard")
st.caption("Cross-domain repository and organization dashboard backed by Athena.")

try:
    dataframe = load_common_data()
except Exception as exc:
    st.error(f"Failed to load data from Athena: {exc}")
    st.stop()

render_dashboard(dataframe=dataframe)
