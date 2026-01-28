from sqlmodel import SQLModel, Session, create_engine, text, select
import pandas as pd
from app.schemas import Committee, Contribution, Expenditure
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
