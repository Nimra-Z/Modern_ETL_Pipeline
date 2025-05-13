from abc import ABC, abstractmethod
from typing import Any, Dict, List
from loguru import logger

class BaseLoader(ABC):
    """Base class for all data loaders in the ETL pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the loader with configuration.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the loader
        """
        self.config = config
        self.logger = logger.bind(name=self.__class__.__name__)
    
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> bool:
        """
        Load the data into the target system.
        
        Args:
            data (List[Dict[str, Any]]): List of records to load
            
        Returns:
            bool: True if loading was successful, False otherwise
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate the loader configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        return True
    
    def handle_error(self, error: Exception) -> None:
        """
        Handle loading errors.
        
        Args:
            error (Exception): The error that occurred
        """
        self.logger.error(f"Loading error: {str(error)}")
        raise error 