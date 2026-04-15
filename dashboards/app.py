from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="GitHub Analytics Dashboard",
    layout="wide",
)

st.title("GitHub Analytics Dashboard")
st.caption("Multipage Streamlit dashboard backed by Athena.")

st.markdown("""
    Use the page navigation on the left to switch between:

    - `Repository`
    - `Organization`
    - `Common`
    """)
