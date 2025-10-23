from contextlib import contextmanager
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

    Example:
        with get_cursor("dev") as cur:
            cur.execute("SELECT * FROM raw.ohlcv;")

    Args:
        env_name (str): The target environment (default: "dev").

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
        query (str): The SQL statement to be executed.
        cur: psycopg2.cursor: A cursor object ready for executing queries.
    """
    try:
        cur.execute(query)
    except Exception as e:
        raise Exception(f"An error occurred during query execution: {e}")


def fetch_query(
    *,
    cur,
    query: str,
) -> list:
    """Execute a SELECT query and return the fetched results.

    Args:
        query (str): The SQL SELECT query.
        env_name (str): The target environment (default: "dev").

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
        schema_name (str): The name of the schema to create.
        env_name (str): The target environment (default: "dev").
    """
    try:
        query = f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'
        cur.execute(query)

    except Exception as e:
        raise Exception(f"Error creating schema '{schema_name}': {e}")


def insert_dataframe(
    *,
    cur,
    df: pd.DataFrame,
    table_name: str
):
    """Insert a pandas DataFrame into a target database table.

    Args:
        df (pandas.DataFrame): The DataFrame to insert.
        table_name (str): The name of the target table.
        env_name (str): The target environment (default: "dev").
    """
    if df.empty:
        raise ValueError(
            "The provided DataFrame is empty and cannot be inserted."
        )

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    try:
        cur.copy_from(buffer, table_name, sep=",", columns=df.columns)
        
    except Exception as e:
        raise Exception(
            f"Error inserting DataFrame into table '{table_name}': {e}"
        )
    
    finally:
        buffer.close()
