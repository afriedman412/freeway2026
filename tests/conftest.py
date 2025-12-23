import pytest
from sqlmodel import SQLModel
import os
from app.db import get_engine
from app.config import DATA_DIR
from tests.helpers import reset_data_dir


@pytest.fixture(scope="session")
def engine():
    engine = get_engine()

    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)

    yield engine

    SQLModel.metadata.drop_all(engine)


@pytest.fixture(scope="session", autouse=True)
def set_test_postgres_url():
    # this is hard-wired in start_test_db.sh
    os.environ["POSTGRES_URL"] = (
        "postgresql+psycopg2://postgres:postgres@localhost:5433/test_db"
    )
    yield
    os.environ.pop("POSTGRES_URL", None)


@pytest.fixture
def clean_data_dir():
    reset_data_dir(DATA_DIR)
    yield
    reset_data_dir(DATA_DIR)


@pytest.fixture
def fake_email(monkeypatch):
    sent = []

    def _fake_send_email(*, subject, body, to, sender, df=None):
        sent.append(
            {
                "subject": subject,
                "body": body,
                "to": to,
                "df": df,
            }
        )

    monkeypatch.setattr("app.main.send_email", _fake_send_email)
    return sent
