from sqlmodel import SQLModel, Session, create_engine, text, select
import pandas as pd
from datetime import datetime
import json
from app.schemas import Committee, Contribution, Expenditure, AppConfig
from app.config import POSTGRES_URL


def get_engine():
    return create_engine(
        POSTGRES_URL,
        pool_pre_ping=True,
    )


def create_tables(database_url: str = POSTGRES_URL, echo: bool = False) -> None:
    engine = create_engine(database_url, echo=echo)
    SQLModel.metadata.create_all(engine)


def reset_tables(database_url: str, echo: bool = False) -> None:
    engine = create_engine(database_url, echo=echo)
    with engine.connect() as conn:
        db = conn.execute(text("select current_database()")).scalar()
        schema = conn.execute(text("select current_schema()")).scalar()
        print("CREATING IN DB:", db, "SCHEMA:", schema)
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)


def get_latest_expenditure(engine) -> pd.DataFrame:
    """Get the most recent expenditure from the database, with committee name."""
    with Session(engine) as session:
        stmt = select(Expenditure).order_by(
            Expenditure.expenditure_date.desc()
        ).limit(1)
        result = session.exec(stmt).first()
        if result:
            data = result.model_dump()
            # Join committee name
            if result.committee_id:
                committee = session.get(Committee, result.committee_id)
                data["committee_name"] = committee.name if committee else None
            else:
                data["committee_name"] = None
            return pd.DataFrame([data])
        return pd.DataFrame()


def get_large_expenditures(engine, min_amount: float = 50000) -> pd.DataFrame:
    """Get all expenditures >= min_amount with committee names."""
    with Session(engine) as session:
        stmt = select(Expenditure).where(
            Expenditure.expenditure_amount >= min_amount
        ).order_by(Expenditure.expenditure_date.desc())
        results = session.exec(stmt).all()

        if not results:
            return pd.DataFrame()

        rows = []
        for exp in results:
            data = exp.model_dump()
            if exp.committee_id:
                committee = session.get(Committee, exp.committee_id)
                data["committee_name"] = committee.name if committee else None
            else:
                data["committee_name"] = None
            rows.append(data)

        return pd.DataFrame(rows)


def get_config(engine, key: str, default=None):
    """Get a config value from the database."""
    with Session(engine) as session:
        config = session.get(AppConfig, key)
        if config:
            return json.loads(config.value)
        return default


def set_config(engine, key: str, value) -> None:
    """Set a config value in the database."""
    with Session(engine) as session:
        config = session.get(AppConfig, key)
        if config:
            config.value = json.dumps(value)
            config.updated_at = datetime.now()
        else:
            config = AppConfig(
                key=key,
                value=json.dumps(value),
                updated_at=datetime.now()
            )
            session.add(config)
        session.commit()
