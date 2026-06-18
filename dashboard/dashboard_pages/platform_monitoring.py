import streamlit as st
import plotly.express as px

from utils.minio_reader import read_gold_table


@st.cache_data(ttl=300)
def load_platform_data():
    return {
        "data_quality": read_gold_table("data_quality_summary_gold"),
        "reliability": read_gold_table("reliability_index_gold"),
        "pipeline_runtime": read_gold_table("pipeline_runtime_gold"),
        "pipeline_status": read_gold_table("pipeline_status_gold"),
        "data_freshness": read_gold_table("data_freshness_gold"),
    }


def render_platform_dashboard():
    st.header("Platform Monitoring")
    st.caption("Pipeline health, data quality, reliability, runtime, and freshness monitoring.")

    try:
        data = load_platform_data()
    except Exception as e:
        st.error("Could not load platform Gold tables from MinIO.")
        st.exception(e)
        st.stop()

    data_quality = data["data_quality"]
    reliability = data["reliability"]
    pipeline_runtime = data["pipeline_runtime"]
    pipeline_status = data["pipeline_status"]
    data_freshness = data["data_freshness"]

    dq = data_quality.iloc[0]
    rel = reliability.iloc[0]
    ps = pipeline_status.iloc[0]
    rt = pipeline_runtime.iloc[0]
    fresh = data_freshness.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Pipeline Status", ps["pipeline_status"])
    col2.metric("Reliability Index", f"{rel['overall_reliability_index']:.2f}%")
    col3.metric("Quality Status", dq["quality_status"])
    col4.metric("Freshness Status", fresh["freshness_status"])

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        quality_scores = {
            "Completeness": dq["completeness_percentage"],
            "Validity": dq["validity_percentage"],
            "Uniqueness": dq["uniqueness_percentage"],
        }

        fig = px.bar(
            x=list(quality_scores.keys()),
            y=list(quality_scores.values()),
            title="Data Quality Scores",
            labels={"x": "Quality Metric", "y": "Score (%)"},
            text=list(quality_scores.values()),
            color=list(quality_scores.values()),
            color_continuous_scale="Greens",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        record_counts = {
            "Valid Records": dq["valid_records"],
            "Invalid Records": dq["invalid_records"],
            "Duplicate Records": dq["duplicate_records"],
        }

        fig = px.pie(
            names=list(record_counts.keys()),
            values=list(record_counts.values()),
            title="Record Quality Breakdown",
            hole=0.45,
        )
        st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        runtime_df = pipeline_runtime.copy()

        fig = px.bar(
            runtime_df,
            x="pipeline_name",
            y="runtime_minutes",
            title="Pipeline Runtime",
            text="runtime_minutes",
            labels={"runtime_minutes": "Runtime (Minutes)", "pipeline_name": "Pipeline"},
            color="runtime_minutes",
            color_continuous_scale="Blues",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        freshness_df = data_freshness.copy()

        fig = px.bar(
            freshness_df,
            x="dataset_name",
            y="freshness_hours",
            title="Data Freshness",
            text="freshness_status",
            labels={"freshness_hours": "Freshness (Hours)", "dataset_name": "Dataset"},
            color="freshness_status",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Operational Health Summary")

    c5, c6 = st.columns(2)

    with c5:
        pipeline_health = {
            "Successful Jobs": ps["successful_jobs"],
            "Failed Jobs": ps["failed_jobs"],
            "Warning Jobs": ps["warning_jobs"],
        }

        fig = px.bar(
            x=list(pipeline_health.keys()),
            y=list(pipeline_health.values()),
            title="Pipeline Job Health",
            labels={"x": "Job Category", "y": "Count"},
            text=list(pipeline_health.values()),
            color=list(pipeline_health.keys()),
        )
        st.plotly_chart(fig, use_container_width=True)

    with c6:
        reliability_scores = {
            "Completeness": rel["completeness_score"],
            "Validity": rel["validity_score"],
            "Uniqueness": rel["uniqueness_score"],
            "Overall Reliability": rel["overall_reliability_index"],
        }

        fig = px.bar(
            x=list(reliability_scores.keys()),
            y=list(reliability_scores.values()),
            title="Reliability Score Breakdown",
            labels={"x": "Reliability Metric", "y": "Score (%)"},
            text=list(reliability_scores.values()),
            color=list(reliability_scores.values()),
            color_continuous_scale="Teal",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    with st.expander("View Pipeline Status Gold Table"):
        st.dataframe(pipeline_status, use_container_width=True)

    with st.expander("View Data Freshness Gold Table"):
        st.dataframe(data_freshness, use_container_width=True)

    with st.expander("View Data Quality Gold Table"):
        st.dataframe(data_quality, use_container_width=True)