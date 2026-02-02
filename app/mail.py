import smtplib
from email.message import EmailMessage
import os
from app.helpers import df_to_csv_bytes, df_to_json_bytes, normalize_recipients
from app.logger import logger


def send_email(
    *,
    subject: str,
    body: str,
    to: list[str],
    sender: str,
    df=None,
    attachment_format: str = "csv",
):
    recipients = normalize_recipients(to)
    logger.info("Preparing email | subject=%s | to=%s", subject, recipients)

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipients
    msg["Subject"] = subject
    msg.set_content(body)

    if df is not None and not df.empty:
        if attachment_format == "json":
            data_bytes = df_to_json_bytes(df)
            msg.add_attachment(
                data_bytes,
                maintype="application",
                subtype="json",
                filename="results.json",
            )
        else:
            csv_bytes = df_to_csv_bytes(df)
            msg.add_attachment(
                csv_bytes,
                maintype="text",
                subtype="csv",
                filename="deduped_results.csv",
            )

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(
                sender,
                os.environ["GOOGLE_APP_PW"],
            )
            smtp.send_message(msg)
        logger.info("Email sent successfully | to=%s", recipients)
    except Exception as e:
        logger.error("Email failed | to=%s | error=%s", recipients, e)
        raise
