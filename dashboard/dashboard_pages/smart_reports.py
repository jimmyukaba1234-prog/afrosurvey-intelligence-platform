import os
import tempfile

import streamlit as st

from utils.minio_reader import read_gold_table
from reporting.summary_generator import generate_full_report_summary
from reporting.chart_exporter import build_report_charts, export_charts_to_images
from reporting.pdf_generator import generate_pdf_report
from reporting.email_reporter import send_email_report

@st.cache_data(ttl=300)
def load_report_data():
    """
    Load Gold tables required for smart report generation.
    """

    return {
        # Business analytics
        "country_kpis": read_gold_table("country_survey_kpis_gold"),
        "governance": read_gold_table("governance_trust_gold"),
        "democracy": read_gold_table("democracy_perception_gold"),
        "corruption": read_gold_table("corruption_perception_gold"),
        "election": read_gold_table("election_fairness_gold"),
        "population": read_gold_table("population_coverage_gold"),

        # Platform monitoring
        "data_quality": read_gold_table("data_quality_summary_gold"),
        "reliability": read_gold_table("reliability_index_gold"),
        "pipeline_runtime": read_gold_table("pipeline_runtime_gold"),
        "pipeline_status": read_gold_table("pipeline_status_gold"),
        "data_freshness": read_gold_table("data_freshness_gold"),
    }


def get_severity_color(severity):
    """
    Return Streamlit-friendly color label for severity.
    """

    severity = severity.lower()

    if severity == "high":
        return "🔴"
    elif severity == "medium":
        return "🟠"
    else:
        return "🟢"


def build_business_kpis(data):
    """
    Build KPI dictionary for the business report section.
    """

    country_kpis = data["country_kpis"]
    governance = data["governance"]

    return {
        "Total Responses": f"{int(country_kpis['total_responses'].sum()):,}",
        "Countries Covered": country_kpis["country"].nunique(),
        "Average Completion Rate": f"{country_kpis['completion_rate'].mean():.2f}%",
        "Average Governance Trust Score": f"{governance['average_trust_score'].mean():.2f}",
    }


def build_platform_kpis(data):
    """
    Build KPI dictionary for the platform report section.
    """

    data_quality = data["data_quality"].iloc[0]
    reliability = data["reliability"].iloc[0]
    pipeline_status = data["pipeline_status"].iloc[0]
    data_freshness = data["data_freshness"].iloc[0]

    return {
        "Pipeline Status": pipeline_status["pipeline_status"],
        "Quality Status": data_quality["quality_status"],
        "Reliability Index": f"{reliability['overall_reliability_index']:.2f}%",
        "Freshness Status": data_freshness["freshness_status"],
    }


def render_findings(title, findings):
    """
    Render findings in Streamlit cards.
    """

    st.subheader(title)

    for finding in findings:
        icon = get_severity_color(finding["severity"])

        with st.container(border=True):
            st.markdown(f"### {icon} {finding['title']}")
            st.write(f"**Severity:** {finding['severity']}")
            st.write(f"**Category:** {finding['category']}")
            st.write(finding["message"])
            st.info(f"Recommendation: {finding['recommendation']}")


def render_smart_reports_page():
    st.header("Smart Reports")
    st.caption(
        "Rule-based intelligence layer for generating stakeholder-ready PDF summaries from the latest Gold analytics."
    )

    if "latest_pdf_path" not in st.session_state:
        st.session_state["latest_pdf_path"] = None

    try:
        data = load_report_data()
    except Exception as e:
        st.error("Could not load Gold tables required for smart reporting.")
        st.exception(e)
        st.stop()

    report_summary = generate_full_report_summary(
        country_kpis=data["country_kpis"],
        governance=data["governance"],
        democracy=data["democracy"],
        corruption=data["corruption"],
        election=data["election"],
        population=data["population"],
        data_quality=data["data_quality"],
        reliability=data["reliability"],
        pipeline_runtime=data["pipeline_runtime"],
        pipeline_status=data["pipeline_status"],
        data_freshness=data["data_freshness"],
    )

    business_kpis = build_business_kpis(data)
    platform_kpis = build_platform_kpis(data)
    risk_summary = report_summary["risk_summary"]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Findings", risk_summary["total_findings"])
    col2.metric("High Risk", risk_summary["high_risk_count"])
    col3.metric("Medium Risk", risk_summary["medium_risk_count"])
    col4.metric("Low Risk", risk_summary["low_risk_count"])

    st.divider()

    st.subheader("Executive Summary Preview")

    if risk_summary["high_risk_count"] > 0:
        st.warning(
            "The latest analytics contain high-risk findings that should be reviewed by stakeholders."
        )
    elif risk_summary["medium_risk_count"] > 0:
        st.info(
            "The latest analytics show moderate issues that require monitoring."
        )
    else:
        st.success(
            "The latest analytics show stable business and platform health."
        )

    st.divider()

    render_findings("Business Findings", report_summary["business_findings"])

    st.divider()

    render_findings("Platform Findings", report_summary["platform_findings"])

    st.divider()



    st.subheader("Report Actions")

    stakeholder_email = st.text_input(
        "Stakeholder Email",
        placeholder="example@company.com",
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Generate PDF Report"):
            try:
                with st.spinner("Generating PDF report..."):
                    output_dir = tempfile.mkdtemp(prefix="afrosurvey_report_")

                    charts = build_report_charts(
                        country_kpis=data["country_kpis"],
                        governance=data["governance"],
                        democracy=data["democracy"],
                        corruption=data["corruption"],
                        election=data["election"],
                        data_quality=data["data_quality"],
                        reliability=data["reliability"],
                    )

                    chart_paths = export_charts_to_images(
                        charts,
                        output_dir=output_dir,
                    )

                    pdf_path = os.path.join(
                        output_dir,
                        "afrosurvey_intelligence_report.pdf",
                    )

                    generate_pdf_report(
                        output_path=pdf_path,
                        report_summary=report_summary,
                        business_kpis=business_kpis,
                        platform_kpis=platform_kpis,
                        chart_paths=chart_paths,
                    )

                    st.session_state["latest_pdf_path"] = pdf_path

                st.success("PDF report generated successfully.")

                with open(pdf_path, "rb") as pdf_file:
                    st.download_button(
                        label="Download PDF Report",
                        data=pdf_file,
                        file_name="afrosurvey_intelligence_report.pdf",
                        mime="application/pdf",
                    )

            except Exception as e:
                st.error("Failed to generate PDF report.")
                st.exception(e)

        with col2:
            if st.button("Email Report to Stakeholder"):
                if not stakeholder_email:
                    st.warning("Please enter a stakeholder email address first.")

                elif not st.session_state.get("latest_pdf_path"):
                    st.warning("Please generate the PDF report first before sending email.")

                elif not os.path.exists(st.session_state["latest_pdf_path"]):
                    st.warning("The generated PDF file could not be found. Please generate the PDF report again.")

                else:
                    try:
                        with st.spinner("Emailing existing PDF report..."):
                            email_body = """
    Hello,

    Please find attached the latest AfroSurvey Intelligence Report.

    The report contains:
    - Executive summary
    - Business analytics findings
    - Platform monitoring status
    - Data quality and reliability checks
    - Recommendations

    Best regards,
    AfroSurvey Intelligence Platform
    """

                            send_email_report(
                                recipients=stakeholder_email,
                                subject="AfroSurvey Weekly Intelligence Report",
                                body=email_body,
                                attachment_path=st.session_state["latest_pdf_path"],
                            )

                        st.success(f"Report emailed successfully to {stakeholder_email}")

                    except Exception as e:
                        st.error("Failed to email report.")
                        st.exception(e)