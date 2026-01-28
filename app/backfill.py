"""
Backfill script - loops through dates or committee IDs.

Usage:
    python -m app.backfill --variant expenditure --start 2025-01-01 --end 2025-01-31
    python -m app.backfill --variant contribution --committees C00799031,C00710848,C00804823
"""
import argparse
import time
import gc
from datetime import datetime, timedelta
from app.main import run
from app.db import create_tables
from app.config import CYCLE, DT_FORMAT
from app.logger import logger


def daterange(start: datetime, end: datetime):
    """Yield dates from start to end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def backfill_dates(variant: str, start_date: str, end_date: str, cycle: int = CYCLE):
    create_tables()

    start = datetime.strptime(start_date, DT_FORMAT)
    end = datetime.strptime(end_date, DT_FORMAT)

    total_days = (end - start).days + 1
    logger.info(f"Backfilling {variant} from {start_date} to {end_date} ({total_days} days)")

    for i, date in enumerate(daterange(start, end), 1):
        date_str = date.strftime(DT_FORMAT)
        logger.info(f"[{i}/{total_days}] Fetching {variant} for {date_str}")
        try:
            run(variant=variant, key=date_str, cycle=cycle, send_notifications=False)
        except Exception as e:
            logger.error(f"Error on {date_str}: {e}")

        gc.collect()
        time.sleep(1)

    logger.info("Backfill complete!")


def backfill_committees(variant: str, committee_ids: list[str], cycle: int = CYCLE):
    create_tables()

    total = len(committee_ids)
    logger.info(f"Backfilling {variant} for {total} committees")

    for i, committee_id in enumerate(committee_ids, 1):
        logger.info(f"[{i}/{total}] Fetching {variant} for {committee_id}")
        try:
            run(variant=variant, key=committee_id, cycle=cycle, send_notifications=False)
        except Exception as e:
            logger.error(f"Error on {committee_id}: {e}")

        gc.collect()
        time.sleep(1)

    logger.info("Backfill complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill FEC data")
    parser.add_argument("--variant", required=True, choices=["expenditure", "contribution"])
    parser.add_argument("--start", help="Start date (YYYY-MM-DD) for date-based backfill")
    parser.add_argument("--end", help="End date (YYYY-MM-DD) for date-based backfill")
    parser.add_argument("--committees", help="Comma-separated committee IDs for committee-based backfill")
    parser.add_argument("--cycle", type=int, default=CYCLE, help=f"Election cycle (default: {CYCLE})")

    args = parser.parse_args()

    if args.committees:
        committee_ids = [c.strip() for c in args.committees.split(",") if c.strip()]
        backfill_committees(args.variant, committee_ids, args.cycle)
    elif args.start and args.end:
        backfill_dates(args.variant, args.start, args.end, args.cycle)
    else:
        parser.error("Provide either --start/--end for dates or --committees for committee IDs")
