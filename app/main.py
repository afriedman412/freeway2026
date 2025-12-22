from app.query import fetch, FetchRun
from app.ingestion import ingest_jsonl
from app.mail import send_email
from app.db import get_engine
from app.helpers import get_now, format_results
import os
import time


def run(variant, key, cycle):
    start = time.perf_counter()

    run = FetchRun(
        variant=variant,
        key=key,
        cycle=cycle,
    )

    fetch(run)
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
        to=os.getenv('TEST_TARGETS'),
        sender="steadynappin@gmail.com",
        df=new_data_df
    )
