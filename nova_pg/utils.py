from contextlib import contextmanager
from rich.progress import Progress
from typing import List
import pandas as pd
import psycopg2
import io


def connect_to_db(
    database_url: str, 
):
    """Open a database connection for the specified environment.

    Args:
        database_url: url string builded using `build_connection_string`.

    Returns:
        psycopg2.connection: An active database connection object.
    """
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        return conn
    
    except psycopg2.Error as e:
        raise ConnectionError(f"Connection to db failed: {e}")
    

@contextmanager
def get_cursor(
    database_url: str
):
    """Context manager that provides a database cursor with automatic
    commit/rollback handling.

    Args:
        database_url (str): A valid PostgreSQL connection string.

    Yields:
        psycopg2.cursor: A cursor object ready for executing queries.
    """
    conn = connect_to_db(database_url)
    cur = conn.cursor()

    try:
        yield cur
        conn.commit()  # Confirm modifies

    except Exception as e:
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
    """Execute a single SQL query (INSERT, UPDATE, DELETE, etc.).

    Args:
        cur: psycopg2.cursor: A cursor object ready for executing queries.
        query (str): The SQL statement to be executed. 
    """
    try:
        cur.execute(query)
    except Exception as e:
        raise Exception(f"An error occurred during query execution: {e}")


def fetch_one(
    *,
    cur,
    query: str,
) -> tuple:
    """Execute a SQL SELECT query and return a single row.

    Executes the provided SQL query using the given database cursor and
    retrieves exactly one row from the result set.

    Args:
        cur: A database cursor object (e.g., from psycopg2).
        query (str): The SQL SELECT query to execute.

    Returns:
        tuple: The first row returned by the query.
            Returns None if the result set is empty.

    Raises:
        Exception: If an error occurs while executing the query.
    """
    try:
        cur.execute(query)
        results = cur.fetchone()
        return results
    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_many(
    *,
    cur,
    query: str,
    batch_size: int,
) -> list:
    """Execute a SELECT query and return the fetched results.

    Args:
        cur: psycopg2.cursor: A cursor object ready for executing queries.
        query (str): The SQL SELECT query.

    Returns:
        list: The result set returned by the query.
    """
    try:
        cur.execute(query)
        results = cur.fetchmany(batch_size)

        return results
    
    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_all(
    *,
    cur,
    query: str,
) -> list:
    """Execute a SELECT query and return the fetched results.

    Args:
        cur: psycopg2.cursor: A cursor object ready for executing queries.
        query (str): The SQL SELECT query.

    Returns:
        list: The result set returned by the query.
    """
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")
    

def create_schema(
    *,
    cur,
    schema_name: str
) -> None:
    """Create a new schema (namespace) in the database if it does not already
    exist.

    Args:
        cur: psycopg2.cursor: A cursor object ready for executing queries.
        schema_name (str): The name of the schema to create.  
    """
    try:
        query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
        cur.execute(query)

    except Exception as e:
        raise Exception(f"Error creating schema '{schema_name}': {e}")


def insert_dataframe(
    *,
    cur, df: pd.DataFrame,
    table_name: str,
    schema: str
):
    """Insert a pandas DataFrame into a target database table."""
    if df.empty:
        raise ValueError("The provided DataFrame is empty and cannot be inserted.")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    try:
        sql = f"""
        COPY {schema}.{table_name} ({', '.join(df.columns)})
        FROM STDIN WITH CSV
        """
        cur.copy_expert(sql=sql, file=buffer)

    except Exception as e:
        raise RuntimeError(
            f"Error inserting DataFrame into {schema}.{table_name}: {e}"
        ) from e

    finally:
        buffer.close()


def estimate_table_rows(
    *,
    cur,
    table_name: str
):
    """
    Estimate the number of rows in a PostgreSQL table using pg_class.reltuples.

    Fast, approximate row count (no full COUNT(*)).
    Returns 100_000 if the estimate isn't available.
    """
    try:
        cur.execute("""
            SELECT reltuples::BIGINT, relpages
            FROM pg_class
            WHERE relname = %s;
        """, (table_name,))

        row = cur.fetchone()
        estimated_rows = int(row[0]) if row is not None else 100_000
        return estimated_rows

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")


def fetch_in_chunks(
    *,
    cur,
    query: str,
    table_name: str,
    batch_size: int = 1000
) -> List:
    """Execute a SELECT query and return the fetched results with a progress bar.

    Args:
        cur: psycopg2.cursor (should be a *named cursor* for large queries).
        query (str): SQL SELECT query.
        table_name (str): Name of the table to estimate row count.
        batch_size (int): Number of rows to fetch per batch.

    Returns:
        list: The result set.
    """
    try:
        rows_estimate = estimate_table_rows(
            cur=cur,
            table_name=table_name
        )

        cur.execute(query)
        results = []

        with Progress() as progress:
            task = progress.add_task(
                f"[cyan]Fetching from {table_name}...", total=rows_estimate
            )

            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break

                progress.update(task, advance=len(batch))
                results.extend(batch)

        return results

    except Exception as e:
        raise Exception(f"An error occurred during fetch: {e}")
