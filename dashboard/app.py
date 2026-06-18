import streamlit as st

from dashboard_pages.business_analytics import render_business_dashboard
from dashboard_pages.platform_monitoring import render_platform_dashboard
from dashboard_pages.smart_reports import render_smart_reports_page


st.set_page_config(
    page_title="AfroSurvey Intelligence Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.sidebar.title("AfroSurvey")
st.sidebar.caption("Intelligence Platform")

page = st.sidebar.radio(
    "Navigation",
    [
        "Business Analytics",
        "Platform Monitoring",
        "Smart Reports",
    ],
)

st.sidebar.divider()
st.sidebar.caption("### Built by")
st.sidebar.caption("**Jimmy Ukaba and Chidimma**")
st.sidebar.divider()
st.sidebar.caption("Powered by MinIO, Spark, Airflow, Streamlit, and Python.")

st.title("AfroSurvey Intelligence Platform")
st.caption("Gold-layer analytics, platform monitoring, and automated reporting.")

if page == "Business Analytics":
    render_business_dashboard()

elif page == "Platform Monitoring":
    render_platform_dashboard()

else:
    render_smart_reports_page()