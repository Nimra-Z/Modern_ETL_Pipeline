from typing import Any, Dict, List
from loguru import logger
import yaml
from pathlib import Path

from extractors.csv_extractor import CSVExtractor
from transformers.data_cleaner import DataCleaner
from loaders.postgres_loader import PostgresLoader

class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, config_path: str):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path (str): Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = logger.bind(name=self.__class__.__name__)
        self._setup_logging()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self) -> None:
        """Configure logging based on settings."""
        log_config = self.config.get('logging', {})
        logger.add(
            log_config.get('file', 'logs/etl_pipeline.log'),
            level=log_config.get('level', 'INFO'),
            format=log_config.get('format'),
            rotation=log_config.get('max_size'),
            retention=log_config.get('backup_count')
        )
    
    def run(self) -> bool:
        """
        Run the ETL pipeline.
        
        Returns:
            bool: True if pipeline execution was successful, False otherwise
        """
        try:
            self.logger.info("Starting ETL pipeline execution")
            
            # Extract
            extractor = CSVExtractor(self.config['sources']['csv'])
            if not extractor.validate_config():
                raise ValueError("Invalid extractor configuration")
            data = extractor.extract()
            
            # Transform
            for transform_config in self.config['transformations']:
                if transform_config['type'] == 'data_cleaning':
                    # Create a dictionary of rules from the list
                    rules = {}
                    for rule in transform_config.get('rules', []):
                        if isinstance(rule, dict):
                            rules.update(rule)
                    
                    # Create transformer with the rules dictionary
                    transformer = DataCleaner({'rules': rules})
                    if not transformer.validate_config():
                        raise ValueError("Invalid transformer configuration")
                    data = transformer.transform(data)
            
            # Load
            loader = PostgresLoader(self.config['target'])
            if not loader.validate_config():
                raise ValueError("Invalid loader configuration")
            
            # Create table if it doesn't exist
            # Note: In a real implementation, you'd want to define your schema
            sample_columns = {
                'id': 'SERIAL PRIMARY KEY',
                'name': 'VARCHAR(255)',
                'age': 'INTEGER',
                'email': 'VARCHAR(255)',
                'created_at': 'TIMESTAMP',
                'updated_at': 'TIMESTAMP'
            }
            loader.create_table_if_not_exists(sample_columns)
            
            # Load the data
            success = loader.load(data)
            
            if success:
                self.logger.info("ETL pipeline completed successfully")
            else:
                self.logger.error("ETL pipeline failed during loading")
            
            return success
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            return False 