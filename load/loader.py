"""
load/duckdb_loader.py

Core DuckDB utility — handles connection, schema initialization, and upserts.
All ingestion scripts import from here rather than managing DB connections directly.
"""

import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_SQL_PATH = Path(__file__).parent.parent / "schema.sql"
DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in .env")
    return create_engine(DATABASE_URL)


def init_db() -> None:
    sql = SCHEMA_SQL_PATH.read_text()
    engine = get_engine()

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.connect() as conn:
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.warning(f"Statement skipped ({e}): {stmt[:80]}...")
        conn.commit()

    logger.info("Database initialized successfully")


def load_dataframe(df, table: str, mode: str = "append") -> int:
    """
    Load a pandas DataFrame into a DuckDB table.

    Args:
        df:     pandas DataFrame to load
        table:  Fully qualified table name e.g. 'raw.census_acs'
        mode:   'append' (default) or 'replace'

    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.warning(f"Empty DataFrame passed to load_dataframe for {table}, skipping.")
        return 0

    # pandas to_sql expects schema and table name separately
    schema, table_name = table.split(".")
    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append" if mode == "append" else "replace",
        index=False,
    )

    logger.info(f"Loaded {len(df)} rows into {table}")
    return len(df)


def upsert_dataframe(df, table: str, primary_keys: list) -> int:
    """
    Upsert a DataFrame into a Postgres table.
    Inserts new rows, updates existing ones on primary key conflict.

    Args:
        df:           pandas DataFrame
        table:        Fully qualified table name e.g. 'staging.stg_census_acs'
        primary_keys: List of column names that form the primary key

    Returns:
        Number of rows upserted
    """
    if df.empty:
        logger.warning(f"Empty DataFrame passed for {table}, skipping.")
        return 0

    schema, table_name = table.split(".")
    engine = get_engine()

    # Build the ON CONFLICT upsert statement
    cols = list(df.columns)
    conflict_cols = ", ".join(primary_keys)
    update_cols = [c for c in cols if c not in primary_keys]
    update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    # Write to a temp table first, then upsert into target
    with engine.connect() as conn:
        # Load into a temporary table
        df.to_sql(
            name=f"_temp_{table_name}",
            con=conn,
            schema=schema,
            if_exists="replace",
            index=False,
        )

        # Upsert from temp into target
        cols_str = ", ".join(cols)
        conn.execute(text(f"""
            INSERT INTO {schema}.{table_name} ({cols_str})
            SELECT {cols_str} FROM {schema}._temp_{table_name}
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_clause}
        """))

        # Clean up temp table
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}._temp_{table_name}"))
        conn.commit()

    logger.info(f"Upserted {len(df)} rows into {table}")
    return len(df)


def row_count(table: str) -> int:
    """Return the current row count of a table."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()


def query(sql: str):
    """Run an arbitrary SELECT and return a pandas DataFrame."""
    import pandas as pd
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    init_db()
    print("Database initialized successfully.")