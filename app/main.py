from app.query import fetch, FetchRun
from app.ingestion import ingest_jsonl
from app.mail import send_email
from app.logger import logger
from app.db import get_engine, create_tables
from app.helpers import get_now, format_results, get_today
from app.config import CYCLE, TARGET_EMAILS
import os
import time


def run(variant, key, cycle):
    start = time.perf_counter()

    run = FetchRun(
        variant=variant,
        key=key,
        cycle=cycle,
    )

    new_data = fetch(run)
    if new_data:
        engine = get_engine()

        new_data_df, new_committees_df = ingest_jsonl(
            run.output_path,
            run.schema,
            engine
        )

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


if __name__ == "__main__":
    today = get_today()
    create_tables()
    logger.info("Running...")
    run(
        variant="expenditure",
        key=today,
        cycle=CYCLE
    )
    logger.info("Run complete.")
