import pandas as pd
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from app.helpers import validate_df, load_jsonl
from app.logger import logger
from app.schemas import Committee, Expenditure, Contribution


def ingest_jsonl(
    path: Path,
    schema,
    engine,
):
    # --------------------------------------------------
    # 1. Load JSONL
    # --------------------------------------------------
    df = load_jsonl(path)
    logger.info("raw rows: %s", len(df))
    raw_len = len(df)
    df = validate_df(df, schema)
    logger.info("validated %s / %s rows", len(df), raw_len)

    if df.empty:
        return

    df = validate_key(df)

    # --------------------------------------------------
    # 2. Extract committee dimension
    # --------------------------------------------------
    committee_df = (
        df.get("committee")
        .dropna()
        .apply(pd.Series)
        .drop_duplicates(subset=["committee_id"])
    )

    # --------------------------------------------------
    # 3. Drop nested objects from fact rows
    # --------------------------------------------------
    df = df.drop(
        columns=["committee", "candidate", "contributor"],
        errors="ignore",
    )

    logger.info("after dropping nested objects: %s", len(df))

    # --------------------------------------------------
    # 4. Fact-specific filtering
    # --------------------------------------------------
    if schema is Expenditure:
        df = df.dropna(
            subset=["expenditure_date", "expenditure_amount"]
        )
        logger.info("after expenditure filter: %s", len(df))
    elif schema is Contribution:
        df = df.dropna(
            subset=["contribution_receipt_date", "contribution_receipt_amount"]
        )
        logger.info("after contribution filter: %s", len(df))

    # --------------------------------------------------
    # 5. Schema validation (pure validation)
    # --------------------------------------------------
    committee_df = validate_df(committee_df, Committee)
    df = validate_df(df, schema)

    logger.info("fact rows to insert: %s", len(df))

    if df.empty and committee_df.empty:
        return

    # --------------------------------------------------
    # 6. Insert (idempotent, PK-backed)
    # --------------------------------------------------
    committee_added = insert_df(committee_df, Committee, engine)
    data_added = insert_df(df, schema, engine)
    return data_added, committee_added


def insert_df(
    df,
    schema,
    engine,
):
    if df.empty:
        return df.iloc[0:0]  # empty DF, same columns

    records = df.to_dict("records")

    pk_cols = [col.name for col in schema.__table__.primary_key.columns]

    stmt = (
        insert(schema)
        .on_conflict_do_nothing()
        .returning(*[schema.__table__.c[c] for c in pk_cols])
    )

    with engine.begin() as conn:
        result = conn.execute(stmt, records)
        inserted_pks = result.fetchall()

    attempted = len(records)
    inserted = len(inserted_pks)
    skipped = attempted - inserted

    logger.info(
        "target=%s inserted=%s skipped=%s attempted=%s",
        schema.__tablename__,
        inserted,
        skipped,
        attempted,
    )

    if not inserted_pks:
        return df.iloc[0:0]

    # --------------------------------------------------
    # Step 2: filter df down to only inserted rows
    # --------------------------------------------------
    inserted_pk_set = set(inserted_pks)

    mask = df[pk_cols].apply(tuple, axis=1).isin(inserted_pk_set)
    inserted_df = df[mask]

    return inserted_df


def validate_key(df, keys=["sub_id", "transaction_id"]):
    """
    Drop rows where any key column is null.
    """
    missing = df[keys].isna().any(axis=1)

    if missing.any():
        logger.warning(
            "Dropping %d rows with null keys (%s)",
            missing.sum(),
            ", ".join(keys),
        )

    return df.loc[~missing].copy()
