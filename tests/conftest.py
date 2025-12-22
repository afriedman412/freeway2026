import pytest
from sqlmodel import SQLModel
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
