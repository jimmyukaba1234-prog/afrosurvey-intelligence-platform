"""
Smart report summary generator for AfroSurvey.

This module reads prepared dashboard data and generates
rule-based findings, risks, and recommendations for reports.
"""


def add_finding(findings, severity, category, title, message, recommendation):
    """
    Helper function to append a structured report finding.
    """

    findings.append(
        {
            "severity": severity,
            "category": category,
            "title": title,
            "message": message,
            "recommendation": recommendation,
        }
    )


def generate_business_findings(country_kpis, governance, democracy, corruption, election, population):
    """
    Generate rule-based business analytics findings.
    """

    findings = []

    # =========================
    # Completion Rate Check
    # =========================

    avg_completion_rate = country_kpis["completion_rate"].mean()

    if avg_completion_rate < 70:
        add_finding(
            findings,
            severity="High",
            category="Survey Participation",
            title="Low Average Completion Rate",
            message=f"The average survey completion rate is {avg_completion_rate:.2f}%, which is below the recommended 70% threshold.",
            recommendation="Review survey length, respondent experience, and possible technical barriers affecting completion.",
        )
    elif avg_completion_rate < 85:
        add_finding(
            findings,
            severity="Medium",
            category="Survey Participation",
            title="Moderate Completion Rate",
            message=f"The average survey completion rate is {avg_completion_rate:.2f}%. This is acceptable but still has room for improvement.",
            recommendation="Monitor completion trends and identify countries or regions with lower participation quality.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Survey Participation",
            title="Strong Completion Rate",
            message=f"The average survey completion rate is {avg_completion_rate:.2f}%, indicating strong respondent completion behavior.",
            recommendation="Maintain current survey design and continue monitoring for regional variation.",
        )

    # =========================
    # Governance Trust Check
    # =========================

    avg_trust_score = governance["average_trust_score"].mean()

    if avg_trust_score < 2.5:
        add_finding(
            findings,
            severity="High",
            category="Governance Trust",
            title="Low Governance Trust",
            message=f"The average governance trust score is {avg_trust_score:.2f}, suggesting weak public confidence.",
            recommendation="Prioritize deeper review of countries and regions with the lowest trust scores.",
        )
    elif avg_trust_score < 3.5:
        add_finding(
            findings,
            severity="Medium",
            category="Governance Trust",
            title="Moderate Governance Trust",
            message=f"The average governance trust score is {avg_trust_score:.2f}, indicating mixed citizen sentiment.",
            recommendation="Track trust indicators over time and compare with democracy and corruption perception metrics.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Governance Trust",
            title="Healthy Governance Trust",
            message=f"The average governance trust score is {avg_trust_score:.2f}, indicating relatively positive public trust.",
            recommendation="Maintain monitoring and investigate regional differences behind the aggregate score.",
        )

    #return findings


    # =========================
    # Democracy Perception Check
    # =========================

    avg_democracy_index = democracy["democracy_index"].mean()

    if avg_democracy_index < 40:
        add_finding(
            findings,
            severity="High",
            category="Democracy Perception",
            title="Weak Democracy Perception",
            message=f"The average democracy index is {avg_democracy_index:.2f}%, indicating weak perception of democratic quality.",
            recommendation="Identify the countries with the weakest democracy index and compare with governance trust and election fairness metrics.",
        )
    elif avg_democracy_index < 60:
        add_finding(
            findings,
            severity="Medium",
            category="Democracy Perception",
            title="Moderate Democracy Perception",
            message=f"The average democracy index is {avg_democracy_index:.2f}%, suggesting mixed perceptions of democratic performance.",
            recommendation="Monitor democracy perception trends and investigate countries with declining sentiment.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Democracy Perception",
            title="Positive Democracy Perception",
            message=f"The average democracy index is {avg_democracy_index:.2f}%, suggesting generally positive democratic perception.",
            recommendation="Continue monitoring democracy perception alongside election fairness and governance trust.",
        )

    # =========================
    # Corruption Perception Check
    # =========================

    avg_corruption_index = corruption["corruption_index"].mean()

    if avg_corruption_index >= 70:
        add_finding(
            findings,
            severity="High",
            category="Corruption Perception",
            title="High Corruption Perception",
            message=f"The average corruption perception index is {avg_corruption_index:.2f}%, indicating elevated perceived corruption.",
            recommendation="Prioritize deeper investigation into countries and regions with the highest corruption perception levels.",
        )
    elif avg_corruption_index >= 40:
        add_finding(
            findings,
            severity="Medium",
            category="Corruption Perception",
            title="Moderate Corruption Perception",
            message=f"The average corruption perception index is {avg_corruption_index:.2f}%, suggesting corruption perception requires monitoring.",
            recommendation="Compare corruption perception with trust and democracy scores to identify possible governance risk clusters.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Corruption Perception",
            title="Low Corruption Perception",
            message=f"The average corruption perception index is {avg_corruption_index:.2f}%, suggesting relatively lower perceived corruption.",
            recommendation="Maintain periodic monitoring and watch for regional spikes.",
        )

    # =========================
    # Election Fairness Check
    # =========================

    avg_fairness_index = election["fairness_index"].mean()

    if avg_fairness_index < 40:
        add_finding(
            findings,
            severity="High",
            category="Election Fairness",
            title="Low Election Fairness Perception",
            message=f"The average election fairness index is {avg_fairness_index:.2f}%, indicating weak confidence in election fairness.",
            recommendation="Review countries with low fairness scores and compare them with democracy perception metrics.",
        )
    elif avg_fairness_index < 60:
        add_finding(
            findings,
            severity="Medium",
            category="Election Fairness",
            title="Moderate Election Fairness Perception",
            message=f"The average election fairness index is {avg_fairness_index:.2f}%, suggesting mixed citizen confidence.",
            recommendation="Track fairness perception over time and identify countries with persistent concerns.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Election Fairness",
            title="Positive Election Fairness Perception",
            message=f"The average election fairness index is {avg_fairness_index:.2f}%, suggesting relatively positive election fairness perception.",
            recommendation="Maintain monitoring and compare results with governance trust indicators.",
        )

    # =========================
    # Population Coverage Check
    # =========================

    avg_coverage = population["responses_per_million"].mean()

    if avg_coverage < 10:
        add_finding(
            findings,
            severity="High",
            category="Population Coverage",
            title="Very Low Population Coverage",
            message=f"The average survey coverage is {avg_coverage:.2f} responses per million people, which is very low.",
            recommendation="Increase respondent outreach and improve geographic sampling coverage.",
        )
    elif avg_coverage < 50:
        add_finding(
            findings,
            severity="Medium",
            category="Population Coverage",
            title="Limited Population Coverage",
            message=f"The average survey coverage is {avg_coverage:.2f} responses per million people.",
            recommendation="Expand survey distribution to improve representativeness across countries.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Population Coverage",
            title="Acceptable Population Coverage",
            message=f"The average survey coverage is {avg_coverage:.2f} responses per million people.",
            recommendation="Maintain survey coverage and compare country-level sampling gaps.",
        )
    return findings




def generate_platform_findings(data_quality, reliability, pipeline_runtime, pipeline_status, data_freshness):
    """
    Generate rule-based platform monitoring findings.
    """

    findings = []

    dq = data_quality.iloc[0]
    rel = reliability.iloc[0]
    runtime = pipeline_runtime.iloc[0]
    status = pipeline_status.iloc[0]
    freshness = data_freshness.iloc[0]

    # =========================
    # Pipeline Status Check
    # =========================

    pipeline_state = str(status["pipeline_status"]).upper()

    if pipeline_state != "SUCCESS":
        add_finding(
            findings,
            severity="High",
            category="Pipeline Health",
            title="Pipeline Execution Issue",
            message=f"The latest pipeline status is {pipeline_state}, meaning the ETL process did not complete successfully.",
            recommendation="Review Airflow logs, Spark job logs, and failed task dependencies immediately.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Pipeline Health",
            title="Pipeline Execution Successful",
            message="The latest pipeline execution completed successfully.",
            recommendation="Continue monitoring runtime, freshness, and data quality indicators.",
        )

    # =========================
    # Reliability Index Check
    # =========================

    reliability_index = rel["overall_reliability_index"]

    if reliability_index < 70:
        add_finding(
            findings,
            severity="High",
            category="Reliability",
            title="Low Reliability Index",
            message=f"The overall reliability index is {reliability_index:.2f}%, which is below the safe threshold.",
            recommendation="Investigate validation failures, duplicate records, and missing values in the Silver and Gold layers.",
        )
    elif reliability_index < 85:
        add_finding(
            findings,
            severity="Medium",
            category="Reliability",
            title="Moderate Reliability Index",
            message=f"The overall reliability index is {reliability_index:.2f}%. The platform is usable but should be monitored.",
            recommendation="Review data validation rules and monitor duplicate or invalid record trends.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Reliability",
            title="Strong Reliability Index",
            message=f"The overall reliability index is {reliability_index:.2f}%, indicating strong data reliability.",
            recommendation="Maintain the current validation and deduplication checks.",
        )

    # =========================
    # Freshness Check
    # =========================

    freshness_status = str(freshness["freshness_status"])

    if freshness_status.lower() == "very stale":
        add_finding(
            findings,
            severity="High",
            category="Data Freshness",
            title="Very Stale Dataset",
            message="The dataset is marked as very stale, meaning the dashboard may not reflect recent survey activity.",
            recommendation="Trigger a new ingestion and transformation run through Airflow.",
        )
    elif freshness_status.lower() == "stale":
        add_finding(
            findings,
            severity="Medium",
            category="Data Freshness",
            title="Stale Dataset",
            message="The dataset is stale and may require refresh soon.",
            recommendation="Confirm whether the scheduled pipeline has run successfully within the expected refresh window.",
        )
    else:
        add_finding(
            findings,
            severity="Low",
            category="Data Freshness",
            title="Fresh Dataset",
            message="The dataset freshness status is healthy.",
            recommendation="Continue scheduled refresh monitoring.",
        )

    return findings



def generate_full_report_summary(
    country_kpis,
    governance,
    democracy,
    corruption,
    election,
    population,
    data_quality,
    reliability,
    pipeline_runtime,
    pipeline_status,
    data_freshness,
):
    """
    Generate a full smart report summary combining business and platform findings.
    """

    business_findings = generate_business_findings(
        country_kpis=country_kpis,
        governance=governance,
        democracy=democracy,
        corruption=corruption,
        election=election,
        population=population,
    )

    platform_findings = generate_platform_findings(
        data_quality=data_quality,
        reliability=reliability,
        pipeline_runtime=pipeline_runtime,
        pipeline_status=pipeline_status,
        data_freshness=data_freshness,
    )

    all_findings = business_findings + platform_findings

    high_risk_count = sum(
        1 for finding in all_findings if finding["severity"] == "High"
    )

    medium_risk_count = sum(
        1 for finding in all_findings if finding["severity"] == "Medium"
    )

    low_risk_count = sum(
        1 for finding in all_findings if finding["severity"] == "Low"
    )

    return {
        "business_findings": business_findings,
        "platform_findings": platform_findings,
        "all_findings": all_findings,
        "risk_summary": {
            "high_risk_count": high_risk_count,
            "medium_risk_count": medium_risk_count,
            "low_risk_count": low_risk_count,
            "total_findings": len(all_findings),
        },
    }