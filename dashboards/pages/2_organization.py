from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboards.data import load_organization_summary

_TABLE_COLUMNS = [
    "dt",
    "hr",
    "org_login",
    "org_name",
    "location",
    "company",
    "is_verified",
    "total_events",
    "push_events",
    "pull_request_events",
    "public_repos",
    "followers",
    "avg_composite_score",
    "avg_bot_ratio",
]


def load_organization_data(start_date: date, end_date: date) -> pd.DataFrame:
    with st.spinner("Loading organization data from Athena..."):
        return load_organization_summary(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )


def render_dashboard(dataframe: pd.DataFrame, top_n: int) -> None:
    if dataframe.empty:
        st.warning("No rows found for the selected date range.")
        return

    st.subheader("Organization Summary Table")
    st.dataframe(
        dataframe[_TABLE_COLUMNS].sort_values(
            ["dt", "hr", "total_events"], ascending=[False, False, False]
        ),
        use_container_width=True,
        hide_index=True,
    )

    organization_distribution = (
        dataframe.groupby("org_login", as_index=False)["total_events"]
        .sum()
        .sort_values("total_events", ascending=False)
        .head(top_n)
    )
    location_distribution = (
        dataframe.groupby("location", as_index=False)["total_events"]
        .sum()
        .sort_values("total_events", ascending=False)
    )
    timeline = (
        dataframe.groupby("timestamp", as_index=False)[
            ["total_events", "push_events", "pull_request_events"]
        ]
        .sum()
        .sort_values("timestamp")
    )

    st.subheader(f"Top {top_n} Organizations by Total Events")
    st.plotly_chart(
        px.bar(
            organization_distribution,
            x="org_login",
            y="total_events",
            title=f"Top {top_n} organizations by total GitHub events",
            labels={"org_login": "Organization", "total_events": "Total events"},
        ),
        use_container_width=True,
    )

    st.subheader("Organization Location Distribution by Total Events")
    st.plotly_chart(
        px.treemap(
            location_distribution,
            path=["location"],
            values="total_events",
            title="Organization location distribution by total events",
        ),
        use_container_width=True,
    )

    st.subheader("Total GitHub Events Over Time")
    st.plotly_chart(
        px.line(
            timeline,
            x="timestamp",
            y="total_events",
            title="Total GitHub events over time",
            labels={"timestamp": "Time", "total_events": "Total events"},
            markers=True,
        ),
        use_container_width=True,
    )

    push_vs_pr = timeline.melt(
        id_vars=["timestamp"],
        value_vars=["push_events", "pull_request_events"],
        var_name="event_type",
        value_name="events",
    )
    push_vs_pr["event_type"] = push_vs_pr["event_type"].map(
        {
            "push_events": "Push Events",
            "pull_request_events": "Pull Request Events",
        }
    )

    st.subheader("Push vs Pull Request Events Over Time")
    st.plotly_chart(
        px.line(
            push_vs_pr,
            x="timestamp",
            y="events",
            color="event_type",
            title="Push versus pull request events over time",
            labels={
                "timestamp": "Time",
                "events": "Events",
                "event_type": "Event type",
            },
            markers=True,
        ),
        use_container_width=True,
    )


st.title("Organization Dashboard")
st.caption("Organization summary dashboard backed by Athena.")

today = date.today()
default_start = today - timedelta(days=7)

st.sidebar.header("Organization Filters")
start_date = st.sidebar.date_input("Start date", value=default_start, max_value=today)
end_date = st.sidebar.date_input(
    "End date", value=today, min_value=start_date, max_value=today
)
top_n = st.sidebar.slider(
    "Top organizations", min_value=5, max_value=50, value=25, step=5
)

try:
    dataframe = load_organization_data(start_date=start_date, end_date=end_date)
except Exception as exc:
    st.error(f"Failed to load data from Athena: {exc}")
    st.stop()

render_dashboard(dataframe=dataframe, top_n=top_n)
