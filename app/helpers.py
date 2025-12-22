from sqlalchemy import select
from sqlmodel import Session, SQLModel
from typing import Dict, Any, List
import json
from app.logger import logger
from pathlib import Path
from datetime import datetime as dt
from typing import Optional
from requests import HTTPError
from tenacity import RetryCallState
import pandas as pd
from pydantic import ValidationError
from datetime import UTC
from zoneinfo import ZoneInfo
from io import StringIO


def normalize_recipients(to) -> list[str]:
    if isinstance(to, str):
        return [to]
    return list(to)


def df_to_csv_bytes(df) -> bytes:
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def format_results(results: dict) -> str:
    return "\n".join(
        f"{k:25} {results[k]}"
        for k in sorted(results)
    )


def get_now():
    now_hour = (
        dt.now(ZoneInfo("America/New_York"))
        .replace(minute=0, second=0, microsecond=0)
    )

    ts = now_hour.strftime("%Y-%m-%d %H:00")
    return ts

# ------------------------------------------------------------------------------
# retry helpers
# ------------------------------------------------------------------------------


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, HTTPError) and exc.response is not None:
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return False


def on_retry(retry_state: RetryCallState):
    exc = retry_state.outcome.exception()
    if isinstance(exc, HTTPError) and exc.response is not None:
        status = exc.response.status_code
        logger.warning(
            f"[{dt.now().isoformat()}] retrying after HTTP {status} "
            f"(attempt {retry_state.attempt_number})"
        )


# ------------------------------------------------------------------------------
# jsonl + checkpoint helpers
# ------------------------------------------------------------------------------


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def load_jsonl(path: Path) -> pd.DataFrame:
    records = []

    with open(path) as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Bad JSON on line {i}: {e}")

    if not records:
        raise ValueError("No valid JSON objects found in file")

    return pd.DataFrame(records)


def load_checkpoint(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def save_checkpoint(path: Path, *, page: int) -> None:
    path.write_text(
        json.dumps(
            {
                "page": page,
                "updated_at": dt.now(UTC),
            },
            indent=2,
        )
    )


def validate_df(df: pd.DataFrame, model) -> pd.DataFrame:
    valid_rows = []

    for _, row in df.iterrows():
        data = row.dropna().to_dict()
        try:
            model(**data)
            valid_rows.append(data)
        except ValidationError:
            continue

    return pd.DataFrame(valid_rows)


def query_table(
    *,
    session: Session,
    table_name: str,
    filters: Dict[str, Any],
    limit: int | None = None,
) -> List[dict]:
    # ------------------------------------------------------------
    # Lookup table from SQLModel metadata
    # ------------------------------------------------------------
    table = SQLModel.metadata.tables.get(table_name)

    if table is None:
        raise ValueError(f"Table '{table_name}' not found")

    # ------------------------------------------------------------
    # Build SELECT
    # ------------------------------------------------------------
    stmt = select(table)

    for column, value in filters.items():
        if column not in table.c:
            raise ValueError(
                f"Column '{column}' not found in table '{table_name}'"
            )
        stmt = stmt.where(table.c[column] == value)

    if limit is not None:
        stmt = stmt.limit(limit)

    # ------------------------------------------------------------
    # Execute and return rows as dicts
    # ------------------------------------------------------------
    result = session.exec(stmt)
    return [dict(row._mapping) for row in result.all()]
