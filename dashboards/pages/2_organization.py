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
    verification_distribution = (
        dataframe.assign(
            verification_status=dataframe["is_verified"].map(
                {True: "Verified", False: "Not Verified"}
            )
        )
        .groupby("verification_status", as_index=False)["total_events"]
        .sum()
        .sort_values("total_events", ascending=False)
    )
    timeline = (
        dataframe.groupby("timestamp", as_index=False)[
            ["total_events", "avg_bot_ratio", "avg_composite_score"]
        ]
        .agg(
            {
                "total_events": "sum",
                "avg_bot_ratio": "mean",
                "avg_composite_score": "mean",
            }
        )
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
        px.bar(
            location_distribution,
            x="location",
            y="total_events",
            title="Organization location distribution by total events",
            labels={"location": "Location", "total_events": "Total events"},
            log_y=True,
        ),
        use_container_width=True,
    )

    st.subheader("Verified vs Non-Verified Organization Activity")
    st.plotly_chart(
        px.pie(
            verification_distribution,
            names="verification_status",
            values="total_events",
            title="Share of total events by verification status",
            hole=0.45,
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
            labels={
                "timestamp": "Time",
                "total_events": "Total events",
            },
            markers=True,
        ),
        use_container_width=True,
    )

    organization_quality = timeline.melt(
        id_vars=["timestamp"],
        value_vars=["avg_bot_ratio", "avg_composite_score"],
        var_name="metric",
        value_name="value",
    )
    organization_quality["metric"] = organization_quality["metric"].map(
        {
            "avg_bot_ratio": "Average Bot Ratio",
            "avg_composite_score": "Average Composite Score",
        }
    )

    st.subheader("Organization Quality Metrics Over Time")
    st.plotly_chart(
        px.line(
            organization_quality,
            x="timestamp",
            y="value",
            color="metric",
            title="Average bot ratio and composite score over time",
            labels={
                "timestamp": "Time",
                "value": "Metric value",
                "metric": "Metric",
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
