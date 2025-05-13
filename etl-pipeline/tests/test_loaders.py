import pytest
from src.loaders.postgres_loader import PostgresLoader

@pytest.mark.skip("Integration test: requires running Postgres")
def test_postgres_loader_validate_config():
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "table": "test_table"
    }
    loader = PostgresLoader(config)
    assert loader.validate_config()
