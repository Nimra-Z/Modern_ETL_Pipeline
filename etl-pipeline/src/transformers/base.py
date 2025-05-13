from abc import ABC, abstractmethod
from typing import Any, Dict, List
from loguru import logger

class BaseTransformer(ABC):
    """Base class for all data transformers in the ETL pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer with configuration.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the transformer
        """
        self.config = config
        self.logger = logger.bind(name=self.__class__.__name__)
    
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the input data.
        
        Args:
            data (List[Dict[str, Any]]): List of records to transform
            
        Returns:
            List[Dict[str, Any]]: Transformed records
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate the transformer configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        return True
    
    def handle_error(self, error: Exception) -> None:
        """
        Handle transformation errors.
        
        Args:
            error (Exception): The error that occurred
        """
        self.logger.error(f"Transformation error: {str(error)}")
        raise error 