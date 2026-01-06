import pytest
from sqlmodel import SQLModel
import os
import subprocess
import time
from app.db import get_engine
from app.config import DATA_DIR
from tests.helpers import reset_data_dir, _postgres_ready


POSTGRES_CONTAINER = "pytest-postgres"
POSTGRES_PORT = 5433


@pytest.fixture(scope="session", autouse=True)
def docker_postgres():
    subprocess.run(
        ["docker", "rm", "-f", POSTGRES_CONTAINER],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", POSTGRES_CONTAINER,
            "-e", "POSTGRES_USER=postgres",
            "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_DB=test_db",
            "-p", f"{POSTGRES_PORT}:5432",
            "postgres:16",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )

    # 🔑 wait for Postgres, not just the port
    for _ in range(30):
        if _postgres_ready(POSTGRES_CONTAINER):
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Postgres did not become ready")

    os.environ["POSTGRES_URL"] = (
        f"postgresql+psycopg2://postgres:postgres@localhost:{POSTGRES_PORT}/test_db"
    )

    import app.config as config
    import app.db as db

    config.POSTGRES_URL = os.environ["POSTGRES_URL"]
    db.POSTGRES_URL = os.environ["POSTGRES_URL"]

    yield

    subprocess.run(
        ["docker", "rm", "-f", POSTGRES_CONTAINER],
        stdout=subprocess.DEVNULL,
    )
    os.environ.pop("POSTGRES_URL", None)


@pytest.fixture(scope="session")
def engine():
    engine = get_engine()

    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)

    yield engine

    SQLModel.metadata.drop_all(engine)


# @pytest.fixture(scope="session", autouse=True)
# def set_test_postgres_url():
#     # this is hard-wired in start_test_db.sh
#     os.environ["POSTGRES_URL"] = (
#         "postgresql+psycopg2://postgres:postgres@localhost:5433/test_db"
#     )
#     yield
#     os.environ.pop("POSTGRES_URL", None)


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
