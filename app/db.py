from sqlmodel import SQLModel, create_engine, text
from app.schemas import Committee, Contribution, Expenditure
from app.config import POSTGRES_URL


def get_engine():
    return create_engine(
        POSTGRES_URL,
        pool_pre_ping=True,
    )


def create_tables(database_url: str, echo: bool = False) -> None:
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
