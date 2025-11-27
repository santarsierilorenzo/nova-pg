from unittest.mock import patch, MagicMock
from nova_pg import utils
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


def test_fetch_all_success():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("row1",), ("row2",)]

    cols, result = utils.fetch_all(
        cur=mock_cursor,
        query="SELECT * FROM table"
    )

    mock_cursor.execute.assert_called_once_with("SELECT * FROM table")
    mock_cursor.fetchall.assert_called_once()
    assert result == [("row1",), ("row2",)]


def test_fetch_all_failure():
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("db error")

    with pytest.raises(Exception) as e:
        utils.fetch_all(
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
