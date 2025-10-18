from pathlib import Path
from typing import Dict
import json


def load_db_config(
    config_file_path: Path,
    env_name: str = "dev"
) -> Dict:
    """Load database connection parameters from a JSON configuration file.

    Args:
        config_file_path (Path): 
            Path to the JSON configuration file.
        env_name (str, optional): 
            Environment name to load. Defaults to "dev".

    Returns:
        Dict: 
            Dictionary containing the database credentials.

    Raises:
        Exception: 
            If the environment section is not found.
        Exception: 
            If any required key is missing.
        Exception: 
            If an error occurs while loading or parsing the configuration file.
    """
    standard_keys = {"host", "port", "dbname", "user", "password"}

    try:
        with open(config_file_path, "r") as f:
            config = json.load(f)

        db_cred = config.get(env_name)
        if not db_cred:
            raise Exception(
                f"Environment '{env_name}' not found in {config_file_path}"
            )

        if not standard_keys.issubset(db_cred.keys()):
            missing_keys = standard_keys - db_cred.keys()
            raise Exception(f"Missing required keys: {missing_keys}")

        return db_cred

    except Exception as e:
        raise Exception(f"Error loading DB configuration: {e}")
