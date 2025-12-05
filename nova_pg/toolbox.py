import pandas as pd
import nova_pg
import io


def insert_dataframe(
    *,
    cur,
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    chunksize: int = 5000
):
    """Insert a pandas DataFrame into a target database table."""
    if df.empty:
        raise ValueError(
            "The provided DataFrame is empty and cannot be inserted."
        )
    
    start = 0
    end = min(start + chunksize, len(df))
    while start < len(df):
        chunk = df.iloc[start:end]
        
        buffer = io.StringIO()
        chunk.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        try:
            sql = f"""
            COPY {schema}.{table_name} ({', '.join(chunk.columns)})
            FROM STDIN WITH CSV
            """
            cur.copy_expert(sql=sql, file=buffer)

        except Exception as e:
            raise RuntimeError(
                f"Error inserting DataFrame into {schema}.{table_name}: {e}"
            ) from e

        finally:
            buffer.close()

        start = end
        end += chunksize


def schema_exists(
    *,
    cur,
    schema_name: str
) -> bool:
    """
    Check whether a PostgreSQL schema exists.

    Parameters
    ----------
    cur : psycopg cursor
        Database cursor used to execute the query.
    schema_name : str
        Name of the schema to check. The function escapes single quotes
        to avoid breaking the query, since execute_query does not support
        params.

    Returns
    -------
    bool
        True if the schema exists, otherwise False.
    """
    
    safe_schema = schema_name.replace("'", "''")

    query = f"""
    SELECT EXISTS (
        SELECT FROM pg_namespace
        WHERE nspname = '{safe_schema}'
    );
    """

    row = nova_pg.utils.fetch_one(cur=cur, query=query)

    if row is None:
        return False

    return bool(row[0])


def table_exists(
    *,
    cur,
    schema_name: str,
    table_name: str
) -> bool:
    """
    Check whether a table exists within a given PostgreSQL schema.

    Parameters
    ----------
    cur : psycopg cursor
        Database cursor used to execute the query.
    schema_name : str
        Schema where the table should exist.
    table_name : str
        Name of the table to check. The function escapes single quotes
        because execute_query does not support parameter binding.

    Returns
    -------
    bool
        True if the table exists in the specified schema, otherwise False.
    """

    safe_schema = schema_name.replace("'", "''")
    safe_table_name = table_name.replace("'", "''")

    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{safe_schema}'
        AND table_name = '{safe_table_name}'
    );
    """

    row = nova_pg.utils.fetch_one(cur=cur, query=query)

    if row is None:
        return False

    return bool(row[0])


def create_table(
    *,
    cur,
    schema_name: str,
    table_name: str,
    columns_map: dict,
):
    """
    Create a PostgreSQL table in the specified schema.

    Parameters
    ----------
    cur : psycopg cursor
        Active database cursor.
    schema_name : str
        Name of the target schema.
    table_name : str
        Name of the table to create.
    columns_map : dict
        Mapping of column_name -> python_dtypename.

    Notes
    -----
    - No param binding is used because execute_query does not support it.
    - Single quotes in schema/table names are escaped manually.
    - Unsupported Python types raise ValueError.
    """

    safe_schema = schema_name.replace("'", "''")
    safe_table_name = table_name.replace("'", "''")

    pg_map = {
        "int": "BIGINT",
        "float": "DOUBLE PRECISION",
        "decimal": "NUMERIC",
        "bool": "BOOLEAN",
        "str": "TEXT",
        "bytes": "BYTEA",
        "datetime": "TIMESTAMPTZ",
        "date": "DATE",
        "time": "TIME",
        "timedelta": "INTERVAL",
    }

    for col, dtype in columns_map.items():
        if dtype not in pg_map:
            raise ValueError(
                f"Unsupported dtype '{dtype}' for column '{col}'"
            )

    column_defs = ", ".join(
        f"{col} {pg_map[dtype]}"
        for col, dtype in columns_map.items()
    )

    query = (
        f"CREATE TABLE {safe_schema}.{safe_table_name} (\n"
        f"    {column_defs}\n"
        f");"
    )

    if not schema_exists(cur=cur, schema_name=safe_schema):
        raise ValueError(f"Schema '{safe_schema}' does not exist.")

    if table_exists(
        cur=cur,
        schema_name=safe_schema,
        table_name=safe_table_name,
    ):
        raise ValueError(
            f"Table '{safe_schema}.{safe_table_name}' already exists."
        )

    try:
        nova_pg.utils.execute_query(cur=cur, query=query)
    except Exception as e:
        raise RuntimeError(
            f"Error creating table '{safe_schema}.{safe_table_name}': {e}"
        )
