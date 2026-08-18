import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(
    dotenv_path=PROJECT_ROOT / ".env",
    override=False,
)

def send_email(
    *,
    subject: str,
    body: str,
) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = os.environ["ALERT_EMAIL_FROM"]
    message["To"] = os.environ["ALERT_EMAIL_TO"]
    message.set_content(body)

    host = os.environ["MAILTRAP_HOST"]
    port = int(os.environ["MAILTRAP_PORT"])

    username = os.environ["MAILTRAP_USERNAME"]
    password = os.environ["MAILTRAP_PASSWORD"]

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(
            username,
            password,
        )
        smtp.send_message(message)


def send_incident_alert(
    *,
    incident_type: str,
    severity: str,
    details: str,
) -> None:
    send_email(
        subject=(f"[RetailPulse] {severity}: " f"{incident_type}"),
        body=(
            "RetailPulse pipeline incident detected.\n\n"
            f"Incident: {incident_type}\n"
            f"Severity: {severity}\n\n"
            f"Details:\n{details}\n"
        ),
    )


def send_recovery_alert(
    *,
    incident_type: str,
) -> None:
    send_email(
        subject=(f"[RetailPulse] RECOVERED: " f"{incident_type}"),
        body=(
            "RetailPulse pipeline incident recovered.\n\n"
            f"Incident: {incident_type}\n"
        ),
    )
