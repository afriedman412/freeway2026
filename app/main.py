from app.query import fetch, FetchRun
from app.ingestion import ingest_jsonl
from app.mail import send_email
from app.logger import logger
from app.db import get_engine, create_tables, get_latest_expenditure
from app.helpers import get_now, format_results, get_today
from app.config import CYCLE, TARGET_EMAILS
import os
import time


def run(variant, key, cycle, send_notifications=True):
    start = time.perf_counter()

    fetch_run = FetchRun(
        variant=variant,
        key=key,
        cycle=cycle,
    )

    new_data = fetch(fetch_run)
    if new_data:
        engine = get_engine()

        result = ingest_jsonl(
            fetch_run.output_path,
            fetch_run.schema,
            engine
        )

        if result is None:
            logger.info("No new data after deduplication!")
            return

        new_data_df, new_committees_df = result

        if new_data_df.empty:
            logger.info("No new data after deduplication!")
            return

        if not send_notifications:
            logger.info(f"Ingested {len(new_data_df)} records (notifications disabled)")
            return

        runtime_seconds = round(time.perf_counter() - start, 2)

        ts = get_now()
        subject = f"[sludgewire] New {variant.title()}s, {ts}"

        results = {
            f"new_{variant}s": len(new_data_df),
            "new_committees": len(new_committees_df),
            "runtime_seconds": runtime_seconds,
        }

        body = f"""
            New data for {ts}!

            Results:
            --------
            {format_results(results)}
        """

        send_email(
            subject=subject,
            body=body,
            to=TARGET_EMAILS,
            sender="steadynappin@gmail.com",
            df=new_data_df
        )
    else:
        logger.info("No new data! ")


def run_test_mode():
    """Send a test email with the most recent expenditure from the database."""
    logger.info("Running in TEST MODE...")
    engine = get_engine()

    latest_df = get_latest_expenditure(engine)

    if latest_df.empty:
        logger.info("No expenditures found in database!")
        return

    ts = get_now()
    row = latest_df.iloc[0]
    recipients_list = "\n".join(f"  - {email}" for email in TARGET_EMAILS)

    body = f"""[TEST MODE] Most recent expenditure as of {ts}

Recipients:
{recipients_list}

Latest Record:
--------------
Date:          {row['expenditure_date']}
Amount:        ${row['expenditure_amount']:,.2f}
Committee:     {row['committee_name']} ({row['committee_id']})
Payee:         {row['payee_name']}
Description:   {row['expenditure_description']}
"""

    send_email(
        subject=f"[sludgewire] TEST - Last Update Check, {ts}",
        body=body,
        to=TARGET_EMAILS,
        sender="steadynappin@gmail.com",
        df=latest_df,
        attachment_format="json"
    )
    logger.info("Test email sent!")


if __name__ == "__main__":
    create_tables()

    if os.environ.get("PRODUCTION"):
        today = get_today()
        logger.info("Running production mode...")
        run(
            variant="expenditure",
            key=today,
            cycle=CYCLE
        )
        logger.info("Run complete.")
    else:
        run_test_mode()
