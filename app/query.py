from pathlib import Path
import os
import requests
from requests import HTTPError
from typing import Literal, Optional, Type
from time import sleep
from urllib.parse import urljoin
from sqlmodel import SQLModel
from app.config import VARIANTS, DATA_DIR, FEC_URL
from app.logger import logger
from app.helpers import (
    write_jsonl, load_checkpoint, save_checkpoint, is_retryable, on_retry
)
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)


def build_query(*, variant: str, key: str, cycle: int) -> dict:
    spec = VARIANTS[variant]

    query = {
        "form_type": spec.form,
        "cycle": cycle,
    }

    if spec.use_date_range:
        query["min_date"] = key
        query["max_date"] = key
    else:
        query[spec.key_name] = key

    if spec.extra_params:
        query |= spec.extra_params

    return query


class FetchRun:
    def __init__(
        self,
        *,
        variant: Literal["expenditure", "contribution"],
        key: str,
        cycle: int,
        per_page: int = 100,
        max_results: int | None = None,
        sleep_every: int = 3,
        sleep_seconds: float = 3.0,
    ):
        if variant not in VARIANTS:
            raise ValueError(f"unknown variant: {variant}")

        self.variant = variant
        self.key = key
        self.cycle = cycle
        self.per_page = per_page
        self.max_results = max_results
        self.sleep_every = sleep_every
        self.sleep_seconds = sleep_seconds

        self.spec = VARIANTS[variant]

    # -------------------------
    # Derived properties
    # -------------------------

    @property
    def url(self) -> str:
        return urljoin(FEC_URL, self.spec.form)

    @property
    def base_params(self) -> dict:
        return self.spec.build_base_params(
            key=self.key,
            cycle=self.cycle,
        )

    @property
    def sort_field(self) -> str:
        return self.spec.sort_field

    @property
    def out_dir(self) -> Path:
        return DATA_DIR / self.spec.form

    @property
    def output_path(self) -> Path:
        return self.out_dir / f"{self.key}.jsonl"

    @property
    def checkpoint_path(self) -> Path:
        return self.out_dir / f"{self.key}.checkpoint.json"

    @property
    def schema(self) -> Type[SQLModel]:
        return self.spec.schema


def fetch(run: FetchRun) -> None:
    run.out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "starting fetch | variant=%s | cycle=%s | target=%s",
        run.variant,
        run.cycle,
        run.key,
    )

    run_fec_query(
        url=run.url,
        base_params=run.base_params,
        sort_field=run.sort_field,
        per_page=run.per_page,
        output_path=run.output_path,
        checkpoint_path=run.checkpoint_path,
        max_results=run.max_results,
        sleep_every=run.sleep_every,
        sleep_seconds=run.sleep_seconds,
    )


def run_fec_query(
    *,
    url: str,
    base_params: dict,
    sort_field: str,
    per_page: int,
    output_path: Path,
    checkpoint_path: Path,
    max_results: Optional[int] = None,
    sleep_every: int = 3,
    sleep_seconds: float = 3.0,
) -> None:

    checkpoint = load_checkpoint(checkpoint_path) or {}
    page = checkpoint.get("page", 1)

    written = 0
    request_count = 0

    while True:
        params = {
            **base_params,
            "api_key": os.environ["GOV_API_KEY"],
            "per_page": per_page,
            "page": page,
            "sort": sort_field,
            "sort_hide_null": "true",
        }
        try:
            r = _request(url, params=params)
        except Exception as e:
            save_checkpoint(
                checkpoint_path,
                page=page,
            )
            logger.error(
                "request failed on page %s — checkpoint saved: %s",
                page,
                e,
            )
            raise
        request_count += 1

        if r is None:
            save_checkpoint(checkpoint_path, page=page)
            logger.warning("429 hit — checkpoint saved at page %s", page)
            return

        payload = r.json()
        results = payload.get("results", [])
        if not results:
            break

        # ------------------------------------------------------------
        # ✂️ apply max_results cap
        # ------------------------------------------------------------
        if max_results is not None:
            remaining = max_results - written
            if remaining <= 0:
                logger.info("max_results=%s reached — stopping", max_results)
                return
            results = results[:remaining]

        write_jsonl(output_path, results)
        written += len(results)

        logger.info("wrote %s rows (total=%s)", len(results), written)

        if max_results is not None and written >= max_results:
            logger.info("max_results=%s reached — stopping", max_results)
            return

        pages = payload.get("pagination", {}).get("pages", 0)
        if page >= pages:
            break

        if request_count % sleep_every == 0:
            sleep(sleep_seconds)

        page += 1

    if checkpoint_path.exists():
        checkpoint_path.unlink()


@retry(
    retry=retry_if_exception(is_retryable),
    wait=wait_exponential_jitter(initial=1, max=10),
    stop=stop_after_attempt(5),
    before_sleep=on_retry,
)
def _request(url: str, *, params: dict, timeout: int = 60
             ) -> requests.Response | None:
    r = requests.get(url, params=params, timeout=timeout)

    # hard stop on rate limit
    if r.status_code == 429:
        return None

    if r.status_code != 200:
        raise HTTPError(
            f"{r.status_code}: {r.text}",
            response=r,
        )

    return r
