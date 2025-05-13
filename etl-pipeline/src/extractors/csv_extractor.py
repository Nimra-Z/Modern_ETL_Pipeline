import pandas as pd
from typing import Any, Dict, List
from .base import BaseExtractor
from loguru import logger

class CSVExtractor(BaseExtractor):
    """Extractor for CSV data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the CSV extractor.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the CSV extractor
        """
        super().__init__(config)
        self.path = config['path']
        self.delimiter = config.get('delimiter', ',')
        self.encoding = config.get('encoding', 'utf-8')
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from CSV file.
        
        Returns:
            List[Dict[str, Any]]: List of extracted records
        """
        try:
            self.logger.info(f"Extracting data from CSV file: {self.path}")
            
            # Read CSV file
            df = pd.read_csv(
                self.path,
                delimiter=self.delimiter,
                encoding=self.encoding
            )
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            self.logger.info(f"Successfully extracted {len(records)} records")
            return records
            
        except Exception as e:
            self.handle_error(e)
    
    def validate_config(self) -> bool:
        """
        Validate the CSV extractor configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_fields = ['path']
        return all(field in self.config for field in required_fields) 