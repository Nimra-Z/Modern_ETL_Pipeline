import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any, Dict, List
from .base import BaseLoader
from loguru import logger

class PostgresLoader(BaseLoader):
    """Loader for PostgreSQL database."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the PostgreSQL loader.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the PostgreSQL loader
        """
        super().__init__(config)
        self.connection_string = self._build_connection_string()
        self.schema = config.get('schema', 'public')
        self.table = config.get('table')
        self.engine = None
    
    def _build_connection_string(self) -> str:
        """Build the PostgreSQL connection string from config."""
        return (
            f"postgresql://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
    
    def load(self, data: List[Dict[str, Any]]) -> bool:
        """
        Load data into PostgreSQL database.
        
        Args:
            data (List[Dict[str, Any]]): List of records to load
            
        Returns:
            bool: True if loading was successful, False otherwise
        """
        try:
            self.logger.info(f"Loading {len(data)} records into PostgreSQL")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Create SQLAlchemy engine
            self.engine = create_engine(self.connection_string)
            
            # Load data into PostgreSQL
            df.to_sql(
                name=self.table,
                schema=self.schema,
                con=self.engine,
                if_exists='append',
                index=False
            )
            
            self.logger.info("Successfully loaded data into PostgreSQL")
            return True
            
        except Exception as e:
            self.handle_error(e)
            return False
        
        finally:
            if self.engine:
                self.engine.dispose()
    
    def validate_config(self) -> bool:
        """
        Validate the PostgreSQL loader configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_fields = ['host', 'port', 'database', 'user', 'password', 'table']
        return all(field in self.config for field in required_fields)
    
    def create_table_if_not_exists(self, columns: Dict[str, str]) -> None:
        """
        Create the target table if it doesn't exist.
        
        Args:
            columns (Dict[str, str]): Dictionary of column names and their SQL types
        """
        try:
            self.engine = create_engine(self.connection_string)
            
            # Build CREATE TABLE statement
            column_defs = [f"{col} {dtype}" for col, dtype in columns.items()]
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                    {', '.join(column_defs)}
                )
            """
            
            # Execute CREATE TABLE statement
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            self.logger.info(f"Successfully created table {self.schema}.{self.table}")
            
        except Exception as e:
            self.handle_error(e)
        
        finally:
            if self.engine:
                self.engine.dispose() 