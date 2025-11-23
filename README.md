# nova-pg

nova-pg is a Python package designed to simplify and streamline interactions with PostgreSQL databases.  
It provides a set of helper functions that make it easier to connect, query, and manage data in a scalable and reusable way.

The main goal of this project is to accelerate development, enable scalability, and offer a portable package that can be easily integrated wherever database access is required.

## Installation
You can install **Nova PG** directly from [PyPI](https://pypi.org) using `pip`:

```bash
pip install nova-pg
```

Or, if you want to test the latest version from **TestPyPI**, use:

```bash
pip install -i https://test.pypi.org/simple --extra-index-url https://pypi.org/simple nova-pg
```

## Repository Structure
This repository includes:
- Example configuration files for environment setup.
- A dedicated test folder to validate the functionality of the helper functions.
- A Docker-based development environment for consistent and isolated builds.


## Development Setup
Follow these steps to set up your local development environment:

1. Clone the repository:
   ```bash
   git clone https://github.com/santarsierilorenzo/nova-pg.git
   ```

2. Copy the example dev container configuration file:
   ```bash
   cp .devcontainer/devcontainer.json.example .devcontainer/devcontainer.json
   ```

3. Open VS Code and rebuild the container without cache:
   ```bash
   Ctrl + Shift + P → Rebuild Container Without Cache and Reopen in Container
   ```

4. Once the Docker container is running, you’ll have access to all required dependencies and tools pre-installed.


## Get Started

1. Copy `db_config.json.example`, rename it to `db_config.json`, and fill it with your database credentials.

2. Import the package:

```python
import nova_pg
```

3. Load your database credentials from `db_config.json`:

```python
db_cred = nova_pg.config.load_db_config(
    config_file_path="path_to_your_json",
    env_name="dev"
)
```

4. Build your connection string:

```python
url_db = nova_pg.config.build_connection_string(
    db_cred_dict=db_cred
)
```

5. Use functions in the `utils` module:

```python
QUERY = """
SELECT *
FROM prices.nq_1_min
"""

with nova_pg.utils.get_cursor(url_db) as cur:
    cols, data = nova_pg.utils.fetch_in_chunks(
        cur=cur,
        query=QUERY,
        table_name="nq_1_min",
        batch_size=10000            
    )
```



