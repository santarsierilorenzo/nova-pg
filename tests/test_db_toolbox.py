from unittest.mock import MagicMock
from nova_pg import toolbox
import pandas as pd
import pytest

def test_insert_dataframe_success():
    mock_cursor = MagicMock()

    df = pd.DataFrame(
        {
            "ticker": ["AAPL", "TSLA", "USO"],
            "price": [350, 400, 25]
        }
    )

    toolbox.insert_dataframe(
        cur=mock_cursor,
        df=df,
        table_name="mock_prices",
        schema="mock_schema"
    )

    mock_cursor.copy_expert.assert_called_once()

    _, kwargs = mock_cursor.copy_expert.call_args
    sql_arg = kwargs["sql"]
    file_arg = kwargs["file"]

    assert "COPY mock_schema.mock_prices" in sql_arg
    assert "ticker" in sql_arg
    assert "price" in sql_arg
    assert "FROM STDIN WITH CSV" in sql_arg

    import io
    assert isinstance(file_arg, io.StringIO)


def test_insert_dataframe_failure():
    mock_cursor = MagicMock()

    df = pd.DataFrame(
        {
            "ticker": ["AAPL", "TSLA", "USO"],
            "price": [350, 400, 25]
        }
    )

    mock_cursor.copy_expert.side_effect = Exception("db copy error")

    with pytest.raises(Exception) as e:
        toolbox.insert_dataframe(
            cur=mock_cursor,
            df=df,
            table_name="prices",
            schema="mock_schema"
        )

    assert "Error inserting DataFrame" in str(e.value)


def test_insert_empty_dataframe():
    mock_cursor = MagicMock()
    df = pd.DataFrame()

    with pytest.raises(ValueError) as e:
        toolbox.insert_dataframe(
            cur=mock_cursor,
            df=df,
            table_name="mock_prices",
            schema="mock_schema"
        )

    assert "empty" in str(e.value).lower()
