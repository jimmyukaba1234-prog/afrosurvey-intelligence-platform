"""
PDF report generator for AfroSurvey Intelligence Platform.

This module builds stakeholder-ready PDF reports using:
- Rule-based findings
- KPI summaries
- Exported Plotly chart images
- Platform monitoring results
"""

import os
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    PageBreak,
    Image,
    Table,
    TableStyle,
)


BRAND_COLORS = {
    "primary": colors.HexColor("#0F172A"),
    "secondary": colors.HexColor("#334155"),
    "muted": colors.HexColor("#64748B"),
    "accent": colors.HexColor("#06B6D4"),
    "success": colors.HexColor("#10B981"),
    "warning": colors.HexColor("#F59E0B"),
    "danger": colors.HexColor("#EF4444"),
    "light_bg": colors.HexColor("#F8FAFC"),
    "border": colors.HexColor("#E2E8F0"),
}


def get_report_styles():
    """
    Define custom PDF styles.
    """

    styles = getSampleStyleSheet()

    styles.add(
        ParagraphStyle(
            name="CoverTitle",
            parent=styles["Title"],
            fontSize=26,
            leading=32,
            textColor=BRAND_COLORS["primary"],
            spaceAfter=20,
        )
    )

    styles.add(
        ParagraphStyle(
            name="SectionTitle",
            parent=styles["Heading1"],
            fontSize=18,
            leading=24,
            textColor=BRAND_COLORS["primary"],
            spaceBefore=18,
            spaceAfter=12,
        )
    )

    styles.add(
        ParagraphStyle(
            name="SubSectionTitle",
            parent=styles["Heading2"],
            fontSize=14,
            leading=18,
            textColor=BRAND_COLORS["secondary"],
            spaceBefore=12,
            spaceAfter=8,
        )
    )

    styles.add(
        ParagraphStyle(
            name="BodyTextCustom",
            parent=styles["BodyText"],
            fontSize=10,
            leading=14,
            textColor=BRAND_COLORS["secondary"],
            spaceAfter=8,
        )
    )

    styles.add(
        ParagraphStyle(
            name="SmallMuted",
            parent=styles["BodyText"],
            fontSize=8,
            leading=10,
            textColor=BRAND_COLORS["muted"],
        )
    )

    return styles



def build_cover_page(story, styles, report_title="AfroSurvey Intelligence Report"):
    """
    Build the report cover page.
    """

    generated_at = datetime.now().strftime("%B %d, %Y %I:%M %p")

    story.append(Spacer(1, 1.5 * inch))

    story.append(
        Paragraph(
            report_title,
            styles["CoverTitle"],
        )
    )

    story.append(
        Paragraph(
            "Executive analytics report generated from the latest Gold-layer survey intelligence data.",
            styles["BodyTextCustom"],
        )
    )

    story.append(Spacer(1, 0.4 * inch))

    cover_info = [
        ["Report Type", "Weekly Intelligence Summary"],
        ["Generated At", generated_at],
        ["Data Source", "Gold Layer / MinIO"],
        ["Platform", "AfroSurvey Intelligence Platform"],
        ["Prepared For", "Stakeholders"],
    ]

    table = Table(cover_info, colWidths=[1.8 * inch, 4.2 * inch])

    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), BRAND_COLORS["light_bg"]),
                ("TEXTCOLOR", (0, 0), (-1, -1), BRAND_COLORS["primary"]),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (1, 0), (1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, BRAND_COLORS["border"]),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )

    story.append(table)

    story.append(Spacer(1, 0.6 * inch))

    story.append(
        Paragraph(
            "Built by Jimmy Ukaba and Chidimma.",
            styles["SmallMuted"],
        )
    )

    story.append(PageBreak())


def build_kpi_table(kpis):
    """
    Build a reusable KPI table.
    """

    table_data = [["Metric", "Value"]]

    for metric, value in kpis.items():
        table_data.append([metric, str(value)])

    table = Table(table_data, colWidths=[3.2 * inch, 2.5 * inch])

    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), BRAND_COLORS["primary"]),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, BRAND_COLORS["border"]),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("TEXTCOLOR", (0, 1), (-1, -1), BRAND_COLORS["secondary"]),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )

    return table


def build_findings_section(story, styles, title, findings):
    """
    Add findings and recommendations to the PDF.
    """

    story.append(Paragraph(title, styles["SectionTitle"]))

    if not findings:
        story.append(
            Paragraph(
                "No findings were generated for this section.",
                styles["BodyTextCustom"],
            )
        )
        return

    for finding in findings:
        severity = finding.get("severity", "Low")
        category = finding.get("category", "General")
        finding_title = finding.get("title", "Untitled Finding")
        message = finding.get("message", "")
        recommendation = finding.get("recommendation", "")

        severity_color = BRAND_COLORS["success"]

        if severity == "High":
            severity_color = BRAND_COLORS["danger"]
        elif severity == "Medium":
            severity_color = BRAND_COLORS["warning"]

        finding_table = Table(
            [
                [
                    Paragraph(f"<b>{severity}</b>", styles["BodyTextCustom"]),
                    Paragraph(f"<b>{finding_title}</b>", styles["BodyTextCustom"]),
                ],
                [
                    Paragraph("Category", styles["SmallMuted"]),
                    Paragraph(category, styles["BodyTextCustom"]),
                ],
                [
                    Paragraph("Finding", styles["SmallMuted"]),
                    Paragraph(message, styles["BodyTextCustom"]),
                ],
                [
                    Paragraph("Recommendation", styles["SmallMuted"]),
                    Paragraph(recommendation, styles["BodyTextCustom"]),
                ],
            ],
            colWidths=[1.3 * inch, 5.0 * inch],
        )

        finding_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (0, 0), severity_color),
                    ("TEXTCOLOR", (0, 0), (0, 0), colors.white),
                    ("BACKGROUND", (1, 0), (1, 0), BRAND_COLORS["light_bg"]),
                    ("GRID", (0, 0), (-1, -1), 0.4, BRAND_COLORS["border"]),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ("TOPPADDING", (0, 0), (-1, -1), 7),
                ]
            )
        )

        story.append(finding_table)
        story.append(Spacer(1, 0.15 * inch))


def add_chart_image(story, styles, chart_path, title):
    """
    Add a chart image to the PDF if it exists.
    """

    if not chart_path or not os.path.exists(chart_path):
        return

    story.append(Paragraph(title, styles["SubSectionTitle"]))

    img = Image(chart_path)
    img._restrictSize(6.8 * inch, 4.2 * inch)

    story.append(img)
    story.append(Spacer(1, 0.25 * inch))


def generate_pdf_report(
    output_path,
    report_summary,
    business_kpis,
    platform_kpis,
    chart_paths=None,
):
    """
    Generate the full AfroSurvey PDF intelligence report.
    """

    if chart_paths is None:
        chart_paths = {}

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=36,
        leftMargin=36,
        topMargin=36,
        bottomMargin=36,
    )

    styles = get_report_styles()
    story = []

    # =========================
    # Cover Page
    # =========================

    build_cover_page(story, styles)

    # =========================
    # Executive Summary
    # =========================

    story.append(Paragraph("Executive Summary", styles["SectionTitle"]))

    risk_summary = report_summary["risk_summary"]

    executive_text = (
        f"This report contains {risk_summary['total_findings']} automated findings "
        f"generated from the latest AfroSurvey Gold-layer analytics. "
        f"The system identified {risk_summary['high_risk_count']} high-risk findings, "
        f"{risk_summary['medium_risk_count']} medium-risk findings, and "
        f"{risk_summary['low_risk_count']} low-risk findings."
    )

    story.append(Paragraph(executive_text, styles["BodyTextCustom"]))
    story.append(Spacer(1, 0.2 * inch))

    # =========================
    # Business Analytics
    # =========================

    story.append(Paragraph("Business Analytics", styles["SectionTitle"]))
    story.append(Paragraph("KPI Summary", styles["SubSectionTitle"]))
    story.append(build_kpi_table(business_kpis))
    story.append(Spacer(1, 0.25 * inch))

    add_chart_image(
        story,
        styles,
        chart_paths.get("responses_by_country"),
        "Survey Responses by Country",
    )

    add_chart_image(
        story,
        styles,
        chart_paths.get("governance_trust"),
        "Governance Trust Score by Country",
    )

    add_chart_image(
        story,
        styles,
        chart_paths.get("civic_perception"),
        "Democracy vs Election Fairness Index",
    )

    add_chart_image(
        story,
        styles,
        chart_paths.get("corruption_perception"),
        "Corruption Perception Index",
    )

    build_findings_section(
        story,
        styles,
        "Business Findings and Recommendations",
        report_summary["business_findings"],
    )

    story.append(PageBreak())

    # =========================
    # Platform Monitoring
    # =========================

    story.append(Paragraph("Platform Monitoring", styles["SectionTitle"]))
    story.append(Paragraph("Platform KPI Summary", styles["SubSectionTitle"]))
    story.append(build_kpi_table(platform_kpis))
    story.append(Spacer(1, 0.25 * inch))

    add_chart_image(
        story,
        styles,
        chart_paths.get("data_quality_scores"),
        "Data Quality Scores",
    )

    add_chart_image(
        story,
        styles,
        chart_paths.get("reliability_breakdown"),
        "Reliability Score Breakdown",
    )

    build_findings_section(
        story,
        styles,
        "Platform Findings and Recommendations",
        report_summary["platform_findings"],
    )

    # =========================
    # Appendix
    # =========================

    story.append(PageBreak())
    story.append(Paragraph("Appendix", styles["SectionTitle"]))

    story.append(
        Paragraph(
            "This report was generated automatically from Gold-layer Parquet datasets stored in MinIO. "
            "Findings are rule-based and designed to support stakeholder review, not replace human decision-making.",
            styles["BodyTextCustom"],
        )
    )

    doc.build(story)

    return output_path