from unittest.mock import patch, MagicMock
from nova_pg import utils
import pandas as pd
import pytest


@patch("psycopg2.connect")
def test_connect_to_db(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    conn = utils.connect_to_db("fake_url")

    mock_connect.assert_called_once_with("fake_url")
    assert conn == mock_conn
    assert conn.autocommit is False


@patch("psycopg2.connect")
def test_get_cursor_success(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    with utils.get_cursor("fake_url") as cur:
        cur.execute("SELECT 1")

    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT 1")
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("psycopg2.connect")
def test_get_cursor_exception_triggers_rollback(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(Exception):
        with utils.get_cursor("fake_url") as cur:
            raise Exception("boom")

    mock_conn.rollback.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_execute_query_success():
    mock_cursor = MagicMock()
    utils.execute_query(cur=mock_cursor, query="SELECT 1",)
    mock_cursor.execute.assert_called_once_with("SELECT 1")


def test_execute_query_failure():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("sql error")

    with pytest.raises(Exception) as e:
        utils.execute_query(cur=mock_cursor, query="SELECT 1")

    assert "An error occurred during query execution" in str(e.value)


def test_fetch_query_success():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("row1",), ("row2",)]

    result = utils.fetch_query(
        cur=mock_cursor,
        query="SELECT * FROM table"
    )

    mock_cursor.execute.assert_called_once_with("SELECT * FROM table")
    mock_cursor.fetchall.assert_called_once()
    assert result == [("row1",), ("row2",)]


def test_fetch_query_failure():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("db error")

    with pytest.raises(Exception) as e:
        utils.fetch_query(
            cur=mock_cursor,
            query="SELECT * FROM table"
        )

    assert "An error occurred during fetch" in str(e.value)


def test_create_schema_success():
    mock_cursor = MagicMock()

    utils.create_schema(
        cur=mock_cursor,
        schema_name="myschema"
    )

    mock_cursor.execute.assert_called_once_with(
        'CREATE SCHEMA IF NOT EXISTS "myschema";'
    )


def test_create_schema_failure():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("create error")

    with pytest.raises(Exception) as e:
        utils.create_schema(
            cur=mock_cursor,
            schema_name="myschema"
        )

    assert "Error creating schema 'myschema'" in str(e.value)


def test_insert_dataframe_success():
    mock_cursor = MagicMock()

    df = pd.DataFrame(
        {
            "ticker": ["AAPL", "TSLA", "USO"],
            "price": [350, 400, 25]
        }
    )

    utils.insert_dataframe(
        cur=mock_cursor,
        df=df,
        table_name="mock_prices"
    )

    # Check if copy_from has been called only once
    mock_cursor.copy_from.assert_called_once()

    args, kwargs = mock_cursor.copy_from.call_args
    assert args[1] == "mock_prices"
    assert kwargs["sep"] == ","
    assert list(kwargs["columns"]) == ["ticker", "price"]


def test_insert_dataframe_failure():
    mock_cursor = MagicMock()

    df = pd.DataFrame(
        {
            "ticker": ["AAPL", "TSLA", "USO"],
            "price": [350, 400, 25]
        }
    )

    mock_cursor.copy_from.side_effect = Exception("db copy error")

    with pytest.raises(Exception) as e:
        utils.insert_dataframe(
            cur=mock_cursor,
            df=df,
            table_name="prices"
        )

    assert "Error inserting DataFrame" in str(e.value)


def test_insert_empty_dataframe():
    mock_cursor = MagicMock()
    df = pd.DataFrame()

    with pytest.raises(ValueError) as e:
        utils.insert_dataframe(
            cur=mock_cursor,
            df=df,
            table_name="mock_prices"
        )

    assert "empty" in str(e.value).lower()