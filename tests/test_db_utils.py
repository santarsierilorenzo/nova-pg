from db_utils import load_db_config
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


def test_valid_config_loads_correctly(valid_config):
    """Should correctly load valid configuration"""
    result = load_db_config(valid_config, "dev")
    assert result["host"] == "localhost"
    assert result["port"] == 5432
    assert set(result.keys()) == {
        "host", "port", "dbname", "user", "password"
    }


def test_missing_file_raises_error():
    """Should raise error when file does not exist"""
    fake_path = Path("non_existent_config.json")
    with pytest.raises(Exception):
        load_db_config(fake_path, "dev")


def test_invalid_env_raises_key_error(valid_config):
    """Should raise KeyError when env not found"""
    with pytest.raises(Exception):
        load_db_config(valid_config, "prod")


def test_missing_keys_raises_key_error(tmp_path):
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
        load_db_config(bad_file, "dev")

    assert "Missing required keys" in str(exc.value)


def test_invalid_json_format(tmp_path):
    """Should raise error for malformed JSON"""
    bad_file = tmp_path / "config.json"
    bad_file.write_text("{invalid_json: true,}")  # broken JSON

    with pytest.raises(Exception):
        load_db_config(bad_file, "dev")
        