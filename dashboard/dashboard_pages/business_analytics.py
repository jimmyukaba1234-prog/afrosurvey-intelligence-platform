import streamlit as st
import plotly.express as px
import pandas as pd

from utils.minio_reader import read_gold_table


@st.cache_data(ttl=300)
def load_business_data():
    return {
        "country_kpis": read_gold_table("country_survey_kpis_gold"),
        "demographics": read_gold_table("demographic_distribution_gold"),
        "response_trends": read_gold_table("response_volume_trends_gold"),
        "governance": read_gold_table("governance_trust_gold"),
        "democracy": read_gold_table("democracy_perception_gold"),
        "corruption": read_gold_table("corruption_perception_gold"),
        "election": read_gold_table("election_fairness_gold"),
        "population": read_gold_table("population_coverage_gold"),
    }


def apply_chart_theme(fig, height=420):
    fig.update_layout(
        template="plotly_white",
        height=height,
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(
            family="Arial",
            size=12,
            color="#0F172A",
        ),
        title=dict(
            font=dict(size=17, color="#0F172A"),
            x=0.02,
            xanchor="left",
        ),
        margin=dict(l=35, r=25, t=60, b=35),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
    )

    fig.update_xaxes(
        showgrid=False,
        linecolor="#CBD5E1",
        tickfont=dict(size=11, color="#334155"),
        title_font=dict(size=12, color="#334155"),
    )

    fig.update_yaxes(
        gridcolor="#E2E8F0",
        linecolor="#CBD5E1",
        tickfont=dict(size=11, color="#334155"),
        title_font=dict(size=12, color="#334155"),
    )

    return fig



def render_kpi_card(title, value):
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_business_dashboard():
    st.markdown(
        """
        <style>
            .business-hero {
                background: linear-gradient(135deg, #0F172A 0%, #164E63 55%, #0891B2 100%);
                padding: 28px 32px;
                border-radius: 24px;
                color: white;
                box-shadow: 0 18px 45px rgba(15, 23, 42, 0.22);
                margin-bottom: 24px;
            }

            .business-hero h1 {
                color: white;
                margin-bottom: 6px;
                font-size: 34px;
                font-weight: 800;
            }

            .business-hero p {
                color: #DDF7FF;
                font-size: 15px;
                margin-bottom: 0;
            }

            .kpi-card {
                background: #FFFFFF;
                border: 1px solid #E2E8F0;
                border-radius: 20px;
                padding: 22px 24px;
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
                min-height: 110px;
                margin-bottom: 18px;
            }

            .kpi-title {
                color: #64748B;
                font-size: 13px;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.04em;
            }

            .kpi-value {
                color: #0F172A;
                font-size: 36px;
                font-weight: 850;
                margin-top: 10px;
                line-height: 1.1;
            }

        


            .section-title {
                font-size: 22px;
                font-weight: 800;
                color: #0F172A;
                margin: 10px 0 14px 0;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="business-hero">
            <h1>AfroSurvey Business Analytics</h1>
            <p>Country-level survey participation, civic perception, demographic coverage, and population-weighted insights across Africa.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        data = load_business_data()
    except Exception as e:
        st.error("Could not load business Gold tables from MinIO.")
        st.exception(e)
        st.stop()

    country_kpis = data["country_kpis"].copy()
    demographics = data["demographics"].copy()
    response_trends = data["response_trends"].copy()
    governance = data["governance"].copy()
    democracy = data["democracy"].copy()
    corruption = data["corruption"].copy()
    election = data["election"].copy()
    population = data["population"].copy()



    country_summary = (
        country_kpis
        .groupby("country", as_index=False)
        .agg({
            "total_responses": "sum",
            "completed_responses": "sum",
            "incomplete_responses": "sum",
            "completion_rate": "mean"
        })
    )

    country_summary["completion_rate"] = (
        country_summary["completed_responses"] / country_summary["total_responses"] * 100
    )


    total_responses = int(country_kpis["total_responses"].sum())
    total_countries = country_kpis["country"].nunique()
    avg_completion = country_kpis["completion_rate"].mean()
    avg_trust = governance["average_trust_score"].mean()

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        render_kpi_card("Total Responses", f"{total_responses:,}")

    with k2:
        render_kpi_card("Countries Covered", f"{total_countries}")

    with k3:
        render_kpi_card("Avg Completion Rate", f"{avg_completion:.2f}%")

    with k4:
        render_kpi_card("Avg Trust Score", f"{avg_trust:.2f}")





    def top_n_title(df, label, n=10):
        count = min(n, len(df))
        return f"Top {count} Countries by {label}"


    def bottom_n_title(df, label, n=10):
        count = min(n, len(df))
        return f"Bottom {count} Countries by {label}"


    st.markdown("## Geographic Survey Coverage")

    map_df = population.copy()

    if "responses_per_million" in map_df.columns:
        fig = px.choropleth(
            map_df,
            locations="country",
            locationmode="country names",
            color="responses_per_million",
            hover_name="country",
            hover_data={
                "responses_per_million": ":.2f",
                "total_responses": True,
                "population": ":,",
            },
            scope="africa",
            title="Africa Survey Coverage Map",
            color_continuous_scale="Viridis",
        )

        fig.update_geos(
            fitbounds="locations",
            visible=False,
            showcountries=True,
            countrycolor="#444",
            projection_scale=1.25,
            center=dict(lat=1, lon=20),
        )

        fig.update_layout(
            height=650,
            margin=dict(l=0, r=0, t=45, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            geo=dict(
                bgcolor="rgba(0,0,0,0)",
            ),
            coloraxis_colorbar=dict(
                title="Responses<br>per million",
                thickness=16,
                len=0.75,
                x=0.98,
            ),
        )

        st.plotly_chart(fig, width="stretch")

        with st.expander("View map data"):
            st.dataframe(
                map_df[["country", "total_responses", "population", "responses_per_million"]]
                .sort_values("responses_per_million", ascending=False),
                width="stretch"
            )
    else:
        st.warning("Population coverage data is missing responses_per_million.")


    st.markdown("## Participation & Completion Performance")

    c1, c2 = st.columns(2)

    with c1:
        top_responses = (
            country_summary
            .dropna(subset=["country", "total_responses"])
            .sort_values("total_responses", ascending=False)
            .head(10)
        )

        fig = px.bar(
            top_responses.sort_values("total_responses", ascending=True),
            x="total_responses",
            y="country",
            orientation="h",
            text="total_responses",
            title=top_n_title(top_responses, "Survey Responses"),
        )

        fig.update_traces(textposition="outside")
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Check response ranking data"):
            st.dataframe(top_responses, use_container_width=True)


    with c2:
        bottom_completion = (
            country_summary
            .dropna(subset=["country", "completion_rate"])
            .sort_values("completion_rate", ascending=True)
            .head(10)
        )

        fig = px.bar(
            bottom_completion.sort_values("completion_rate", ascending=False),
            x="completion_rate",
            y="country",
            orientation="h",
            text="completion_rate",
            title=bottom_n_title(bottom_completion, "Completion Rate"),
        )

        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(height=450, xaxis_ticksuffix="%")
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Check completion ranking data"):
            st.dataframe(bottom_completion, use_container_width=True)


    c3, c4 = st.columns(2)

    with c3:
        top_coverage = (
            population
            .dropna(subset=["country", "responses_per_million"])
            .sort_values("responses_per_million", ascending=False)
            .head(10)
        )

        fig = px.bar(
            top_coverage.sort_values("responses_per_million", ascending=True),
            x="responses_per_million",
            y="country",
            orientation="h",
            text="responses_per_million",
            title=top_n_title(top_coverage, "Population Coverage"),
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Check coverage ranking data"):
            st.dataframe(top_coverage, use_container_width=True)


    with c4:
        response_by_country = (
            country_summary
            .dropna(subset=["country", "total_responses"])
            .sort_values("total_responses", ascending=False)
            .head(10)
        )

        fig = px.bar(
            response_by_country.sort_values("total_responses", ascending=True),
            x="total_responses",
            y="country",
            orientation="h",
            text="total_responses",
            title="Top 10 Countries by Response Volume",
        )

        fig.update_traces(textposition="outside")
        fig.update_layout(
            height=450,
            xaxis_title="Responses",
            yaxis_title="Country"
        )

        st.plotly_chart(fig, width="stretch")

        with st.expander("Check response volume data"):
            st.dataframe(response_by_country, width="stretch")



    st.markdown("## Civic Perception Insights")

    c5, c6 = st.columns(2)

    with c5:
        governance_summary = (
            governance
            .dropna(subset=["country", "average_trust_score"])
            .groupby("country", as_index=False)["average_trust_score"]
            .mean()
            .sort_values("average_trust_score", ascending=False)
            .head(10)
        )

        fig = px.bar(
            governance_summary.sort_values("average_trust_score", ascending=True),
            x="average_trust_score",
            y="country",
            orientation="h",
            text="average_trust_score",
            title="Top 10 Countries by Governance Trust Score",
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(height=450, xaxis_title="Avg Trust Score", yaxis_title="Country")
        st.plotly_chart(fig, width="stretch")


    with c6:
        democracy_summary = (
            democracy
            .dropna(subset=["country", "average_democracy_score"])
            .groupby("country", as_index=False)["average_democracy_score"]
            .mean()
            .sort_values("average_democracy_score", ascending=False)
            .head(10)
        )

        fig = px.bar(
            democracy_summary.sort_values("average_democracy_score", ascending=True),
            x="average_democracy_score",
            y="country",
            orientation="h",
            text="average_democracy_score",
            title="Top 10 Countries by Democracy Satisfaction",
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(height=450, xaxis_title="Avg Democracy Score", yaxis_title="Country")
        st.plotly_chart(fig, width="stretch")


    c7, c8 = st.columns(2)


    with c7:
        gender_summary = (
            demographics
            .dropna(subset=["gender", "response_count"])
            .groupby("gender", as_index=False)["response_count"]
            .sum()
            .sort_values("response_count", ascending=False)
        )

        fig = px.pie(
            gender_summary,
            names="gender",
            values="response_count",
            title="Survey Responses by Gender",
            hole=0.45,
        )

        fig.update_layout(
            height=450,
            margin=dict(l=20, r=20, t=60, b=20),
        )

        st.plotly_chart(fig, width="stretch")

        with st.expander("Check gender distribution data"):
            st.dataframe(gender_summary, width="stretch")

    with c8:
        election_summary = (
            election
            .dropna(subset=["country", "average_fairness_score"])
            .groupby("country", as_index=False)["average_fairness_score"]
            .mean()
            .sort_values("average_fairness_score", ascending=False)
            .head(10)
        )

        fig = px.bar(
            election_summary.sort_values("average_fairness_score", ascending=True),
            x="average_fairness_score",
            y="country",
            orientation="h",
            text="average_fairness_score",
            title="Top 10 Countries by Election Fairness",
        )

        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(
            height=450,
            xaxis_title="Avg Fairness Score",
            yaxis_title="Country"
        )

        st.plotly_chart(fig, width="stretch")

        with st.expander("Check election fairness data"):
            st.dataframe(election_summary, width="stretch")