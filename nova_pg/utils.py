from contextlib import contextmanager
from typing import List, Tuple, Optional
import psycopg2


def connect_to_db(
    database_url: str,
) -> psycopg2.extensions.connection:
    """Open a database connection.

    Args:
        database_url: PostgreSQL connection string.

    Returns:
        psycopg2 connection object with autocommit disabled.

    Raises:
        ConnectionError: If connection cannot be established.
    """
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        return conn

    except psycopg2.Error as e:
        raise ConnectionError(f"Connection to db failed: {e}")


@contextmanager
def get_cursor(
    database_url: str,
):
    """Context manager yielding a cursor with automatic transaction handling.

    Args:
        database_url: PostgreSQL connection string.

    Yields:
        psycopg2 cursor.

    Raises:
        Propagates any exception from query execution.
    """
    conn = connect_to_db(database_url)
    cur = conn.cursor()

    try:
        yield cur
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


def execute_query(
    *,
    cur,
    query: str,
) -> None:
    """Execute a generic SQL statement.

    Args:
        cur: psycopg2 cursor.
        query: SQL statement.

    Raises:
        Exception: On execution errors.
    """
    try:
        cur.execute(query)
    except Exception as e:
        raise Exception(f"An error occurred during query execution: {e}")


def fetch_one(
    *,
    cur,
    query: str,
) -> Tuple[List[str], Optional[Tuple]]:
    """Execute a SELECT query and fetch the first row.

    Args:
        cur: psycopg2 cursor.
        query: SQL SELECT query.

    Returns:
        Tuple of:
            - List of column names.
            - First row or None.

    Raises:
        Exception: On execution errors.
    """
    try:
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        result = cur.fetchone()
        return column_names, result

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_many(
    *,
    cur,
    query: str,
    batch_size: int,
) -> Tuple[List[str], List[Tuple]]:
    """Execute a SELECT query and fetch up to batch_size rows.

    Args:
        cur: psycopg2 cursor.
        query: SQL SELECT query.
        batch_size: Max number of rows to fetch.

    Returns:
        Tuple of:
            - List of column names.
            - List of rows.

    Raises:
        Exception: On execution errors.
    """
    try:
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        results = cur.fetchmany(batch_size)
        return column_names, results

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_all(
    *,
    cur,
    query: str,
) -> Tuple[List[str], List[Tuple]]:
    """Execute a SELECT query and fetch the entire result set.

    Args:
        cur: psycopg2 cursor.
        query: SQL SELECT query.

    Returns:
        Tuple of:
            - List of column names.
            - List of all rows.

    Raises:
        Exception: On execution errors.
    """
    try:
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        return column_names, results

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def create_schema(
    *,
    cur,
    schema_name: str,
) -> None:
    """Create a schema if it does not already exist.

    Args:
        cur: psycopg2 cursor.
        schema_name: Schema name.

    Raises:
        Exception: On execution errors.
    """
    try:
        query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
        cur.execute(query)

    except Exception as e:
        raise Exception(f"Error creating schema '{schema_name}': {e}")


def estimate_table_rows(
    *,
    cur,
    table_name: str,
) -> int:
    """Estimate row count of a table via pg_class.reltuples.

    Args:
        cur: psycopg2 cursor.
        table_name: Name of the table.

    Returns:
        Estimated number of rows. Defaults to 100_000 if unavailable.

    Raises:
        Exception: On execution errors.
    """
    try:
        cur.execute(
            """
            SELECT reltuples::BIGINT
            FROM pg_class
            WHERE relname = %s;
            """,
            (table_name,),
        )

        row = cur.fetchone()
        return int(row[0]) if row else 100_000

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_in_chunks(
    *,
    cur,
    query: str,
    table_name: str,
    batch_size: int = 1000,
) -> Tuple[List[str], List[Tuple]]:
    """Execute a SELECT query and fetch results incrementally.

    Args:
        cur: psycopg2 cursor (supports named cursors for large datasets).
        query: SQL SELECT query.
        table_name: Table used for row estimation.
        batch_size: Number of rows per batch.

    Returns:
        Tuple of:
            - List of column names.
            - List of all fetched rows.

    Raises:
        Exception: On execution or fetch errors.
    """
    try:
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        results: List[Tuple] = []

        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break

            results.extend(batch)

        return column_names, results

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")
