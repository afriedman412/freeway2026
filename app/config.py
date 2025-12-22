from dataclasses import dataclass
import os
from typing import Type
from pathlib import Path
from sqlmodel import SQLModel
from app.schemas import Expenditure, Contribution

FEC_URL = "https://api.open.fec.gov/v1/schedules/{}"

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+psycopg2://localhost/postgres",
)


@dataclass(frozen=True)
class VariantSpec:
    form: str
    sort_field: str
    schema: Type[SQLModel]
    build_base_params: callable


VARIANTS = {
    "expenditure": VariantSpec(
        form="schedule_e",
        sort_field="-expenditure_date",
        schema=Expenditure,
        build_base_params=lambda *, key, cycle: {
            "two_year_transaction_period": cycle,
            "min_date": key,
            "max_date": key,
        },
    ),
    "contribution": VariantSpec(
        form="schedule_a",
        sort_field="-contribution_receipt_date",
        schema=Contribution,
        build_base_params=lambda *, key, cycle: {
            "committee_id": key,
            "two_year_transaction_period": cycle,
        },
    ),
}
