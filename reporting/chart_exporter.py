import os
import tempfile

import plotly.express as px


CHART_COLORS = {
    "blue": "#2563EB",
    "cyan": "#06B6D4",
    "green": "#10B981",
    "orange": "#F59E0B",
    "red": "#EF4444",
    "purple": "#8B5CF6",
}


def apply_report_chart_theme(fig):
    """
    Apply a clean, colorful report theme to Plotly charts.
    """

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(
            family="Arial",
            color="#0F172A",
            size=12,
        ),
        title_font=dict(
            color="#0F172A",
            size=18,
        ),
        margin=dict(l=40, r=30, t=60, b=40),
        legend=dict(
            font=dict(color="#0F172A"),
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
    )

    fig.update_xaxes(
        tickfont=dict(color="#0F172A"),
        title_font=dict(color="#0F172A"),
        gridcolor="#E2E8F0",
        linecolor="#CBD5E1",
    )

    fig.update_yaxes(
        tickfont=dict(color="#0F172A"),
        title_font=dict(color="#0F172A"),
        gridcolor="#E2E8F0",
        linecolor="#CBD5E1",
    )

    return fig


def build_report_charts(country_kpis, governance, democracy, corruption, election, data_quality, reliability):
    """
    Build selected colorful Plotly charts for the PDF report.
    """

    charts = {}

    # 1. Responses by Country
    fig = px.bar(
        country_kpis.sort_values("total_responses", ascending=False),
        x="country",
        y="total_responses",
        text="total_responses",
        title="Survey Responses by Country",
        color="total_responses",
        color_continuous_scale="Blues",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False)
    charts["responses_by_country"] = apply_report_chart_theme(fig)

    # 2. Governance Trust
    fig = px.bar(
        governance.sort_values("average_trust_score", ascending=False),
        x="country",
        y="average_trust_score",
        text="average_trust_score",
        title="Governance Trust Score by Country",
        color="average_trust_score",
        color_continuous_scale="Teal",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False)
    charts["governance_trust"] = apply_report_chart_theme(fig)

    # 3. Civic Perception Combined
    democracy_summary = democracy[["country", "democracy_index"]].copy()
    election_summary = election[["country", "fairness_index"]].copy()

    civic = democracy_summary.merge(
        election_summary,
        on="country",
        how="outer",
    )

    fig = px.bar(
        civic,
        x="country",
        y=["democracy_index", "fairness_index"],
        barmode="group",
        title="Democracy vs Election Fairness Index",
        color_discrete_sequence=[
            CHART_COLORS["green"],
            CHART_COLORS["purple"],
        ],
    )
    charts["civic_perception"] = apply_report_chart_theme(fig)

    # 4. Corruption Perception
    fig = px.bar(
        corruption.sort_values("corruption_index", ascending=False),
        x="country",
        y="corruption_index",
        color="corruption_level",
        title="Corruption Perception Index",
        color_discrete_sequence=[
            CHART_COLORS["red"],
            CHART_COLORS["orange"],
            CHART_COLORS["blue"],
            CHART_COLORS["green"],
        ],
    )
    charts["corruption_perception"] = apply_report_chart_theme(fig)

    # 5. Data Quality Scores
    dq = data_quality.iloc[0]

    quality_df = {
        "metric": ["Completeness", "Validity", "Uniqueness"],
        "score": [
            dq["completeness_percentage"],
            dq["validity_percentage"],
            dq["uniqueness_percentage"],
        ],
    }

    fig = px.bar(
        quality_df,
        x="metric",
        y="score",
        text="score",
        title="Data Quality Scores",
        color="score",
        color_continuous_scale="Greens",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, yaxis_range=[0, 100])
    charts["data_quality_scores"] = apply_report_chart_theme(fig)

    # 6. Reliability Breakdown
    rel = reliability.iloc[0]

    reliability_df = {
        "metric": [
            "Completeness",
            "Validity",
            "Uniqueness",
            "Overall Reliability",
        ],
        "score": [
            rel["completeness_score"],
            rel["validity_score"],
            rel["uniqueness_score"],
            rel["overall_reliability_index"],
        ],
    }

    fig = px.bar(
        reliability_df,
        x="metric",
        y="score",
        text="score",
        title="Reliability Score Breakdown",
        color="score",
        color_continuous_scale="Teal",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, yaxis_range=[0, 100])
    charts["reliability_breakdown"] = apply_report_chart_theme(fig)

    return charts


def export_charts_to_images(charts, output_dir=None):
    """
    Export Plotly charts to PNG images for PDF generation.
    If one chart fails, skip it and continue generating the PDF.
    """

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="afrosurvey_charts_")

    os.makedirs(output_dir, exist_ok=True)

    chart_paths = {}

    for chart_name, fig in charts.items():
        output_path = os.path.join(output_dir, f"{chart_name}.png")

        try:
            fig.write_image(
                output_path,
                width=900,
                height=500,
                scale=1,
            )

            chart_paths[chart_name] = output_path
            print(f"Exported chart: {chart_name}")

        except Exception as e:
            print(f"FAILED CHART: {chart_name}")
            print(e)
            continue

    return chart_paths