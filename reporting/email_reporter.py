"""
Email reporter for AfroSurvey Intelligence Platform.

This module sends generated PDF reports to stakeholders.
"""

import os
import smtplib
from pathlib import Path
from email.message import EmailMessage

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=True)


SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REPORT_SENDER = os.getenv("REPORT_SENDER", SMTP_USERNAME)

def send_email_report(
    recipients,
    subject,
    body,
    attachment_path=None,
):
    """
    Send stakeholder email report with optional PDF attachment.
    """

    if not SMTP_USERNAME or not SMTP_PASSWORD:
        raise ValueError(
            "SMTP_USERNAME and SMTP_PASSWORD must be set in environment variables."
        )

    if isinstance(recipients, str):
        recipients = [recipients]

    message = EmailMessage()
    message["From"] = REPORT_SENDER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject

    message.set_content(body)

    if attachment_path:
        with open(attachment_path, "rb") as file:
            attachment_data = file.read()

        filename = os.path.basename(attachment_path)

        message.add_attachment(
            attachment_data,
            maintype="application",
            subtype="pdf",
            filename=filename,
        )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(message)

    return True