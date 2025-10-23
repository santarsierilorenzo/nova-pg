from nova_pg import config
from pathlib import Path
import pytest
import json


@pytest.fixture
def valid_config(tmp_path):
    """Create a temporary valid JSON config file for testing"""
    config_data = {
        "dev": {
            "host": "localhost",
            "port": 5432,
            "dbname": "test_db",
            "user": "postgres",
            "password": "secret"
        }
    }
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(config_data))

    return file_path


def test_valid_cred_dict():
    """Given a valid dictionary containing all required keys for a database
    connection, verify that the returned tuple matches the expected result.
    """
    proper_config_data = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test_db",
        "user": "postgres",
        "password": "secret"
    }

    all_keys_present, missing_keys = config._db_keys_check(
        db_cred_dict=proper_config_data
    )

    assert all_keys_present is True
    assert missing_keys == set()


def test_missing_keys_cred_dict():
    """Given a dictionary missing some of the required keys for a database
    connection, verify that the returned tuple matches the expected result.
    """
    incomplete_config_data = {
        "port": 5432,
        "dbname": "test_db",
        "user": "postgres",
    }

    all_keys_present, missing_keys = config._db_keys_check(
        db_cred_dict=incomplete_config_data
    )

    assert all_keys_present is False
    assert missing_keys == {"host", "password"}


def test_valid_config_loads_correctly(valid_config):
    """Should correctly load valid configuration"""
    result = config.load_db_config(
        config_file_path=valid_config,
        env_name="dev"
    )
    assert result["host"] == "localhost"
    assert result["port"] == 5432
    assert set(result.keys()) == {
        "host", "port", "dbname", "user", "password"
    }


def test_missing_file_raises_error():
    """Should raise error when file does not exist"""
    fake_path = Path("non_existent_config.json")
    with pytest.raises(Exception):
        config.load_db_config(config_file_path=fake_path, env_name="dev")


def test_invalid_env_raises_exception(valid_config):
    """Should raise KeyError when env not found"""
    with pytest.raises(Exception):
        config.load_db_config(config_file_path=valid_config, env_name="prod")


def test_missing_keys_raises_exception(tmp_path):
    """Should raise KeyError if required keys are missing"""
    bad_data = {
        "dev": {
            "host": "localhost",
            "port": 5432
        }
    }
    bad_file = tmp_path / "config.json"
    bad_file.write_text(json.dumps(bad_data))

    with pytest.raises(Exception) as exc:
        config.load_db_config(config_file_path=bad_file, env_name="dev")

    assert "Missing required keys" in str(exc.value)


def test_invalid_json_format(tmp_path):
    """Should raise error for malformed JSON"""
    bad_file = tmp_path / "config.json"
    bad_file.write_text("{invalid_json: true,}")  # broken JSON

    with pytest.raises(Exception):
        config.load_db_config(config_file_path=bad_file, env_name="dev")
        
    
def test_missing_keys_raises_exception_build_string():
    """Test that `build_connection_string` raises an Exception
    when required database keys are missing.
    """
    bad_data = {
        "host": "localhost",
        "port": 5432
    }
    with pytest.raises(Exception):
        config.build_connection_string(bad_data)


def test_valid_connection_string_format():
    """Given proper database credentials, check that the connection string 
    is correctly formatted.
    """
    proper_config_data = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test_db",
        "user": "postgres",
        "password": "secret"
    }

    expected_string = (
        "postgresql://postgres:secret@localhost:5432/"
        "test_db?sslmode=require"
    )

    connection_string = config.build_connection_string(
        db_cred_dict=proper_config_data
    )

    assert connection_string == expected_string
    