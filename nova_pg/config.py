from contextlib import contextmanager
from typing import Tuple, Dict
from pathlib import Path
import json


def _db_keys_check(
    *,
    db_cred_dict: Dict
) -> Tuple[bool, set]:
    """Validate that all required database credential keys are present in the
    provided dictionary.

    Checks whether the input dictionary contains all standard keys required for
    a database
    connection: {"host", "port", "dbname", "user", "password"}.
    Returns a tuple indicating whether all keys are present and, if not, which
    ones are missing.

    Args:
        db_cred_dict (Dict): Dictionary containing database connection
        credentials.

    Returns:
        Tuple[bool, set]:
            - A boolean indicating whether all required keys are present.
            - A set of missing keys (empty if none are missing).
    """
    standard_keys = {"host", "port", "dbname", "user", "password"}
    dict_keys = db_cred_dict.keys()
    all_keys_present = standard_keys.issubset(dict_keys)
    missing_keys = standard_keys - dict_keys

    return all_keys_present, missing_keys


def load_db_config(
    *,
    config_file_path: Path,
    env_name: str = "dev"
) -> Dict:
    """Load database connection parameters from a JSON configuration file.

    Args:
        config_file_path (Path): Path to the JSON configuration file.
        env_name (str, optional): Environment name to load. Defaults to "dev".

    Returns:
        Dict: Dictionary containing the database credentials.

    Raises:
        Exception: If the environment section is not found.
        Exception: If any required key is missing.
        Exception: If an error occurs while loading or parsing the
        configuration file.
    """
    try:
        with open(config_file_path, "r") as f:
            config = json.load(f)

        db_cred = config.get(env_name)
        if not db_cred:
            raise Exception(
                f"Environment '{env_name}' not found in {config_file_path}"
            )

        all_keys_present, missing_keys = _db_keys_check(db_cred_dict=db_cred)
        if not all_keys_present:
            raise Exception(f"Missing required keys: {missing_keys}")

        return db_cred

    except Exception as e:
        raise Exception(f"Error loading DB configuration: {e}")


def build_connection_string(
    *,
    db_cred_dict: Dict
) -> str:
    """
    Build a PostgreSQL connection string from a configuration dictionary.

    Args:
        db_cred_dict (Dict): Database configuration containing 'host', 'port',
            'dbname', 'user', and 'password'.

    Returns:
        str: A valid PostgreSQL connection string.
    """
    all_keys_present, missing_keys = _db_keys_check(db_cred_dict=db_cred_dict)
    
    if not all_keys_present:
        raise Exception(f"Missing required keys: {missing_keys}")

    db_user = db_cred_dict["user"]
    db_password = db_cred_dict["password"]
    db_host = db_cred_dict["host"]
    db_port = db_cred_dict["port"]
    db_name = db_cred_dict["dbname"]

    return (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/"
        f"{db_name}?sslmode=require"
    )
