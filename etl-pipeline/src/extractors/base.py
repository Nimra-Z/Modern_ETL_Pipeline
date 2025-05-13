from abc import ABC, abstractmethod
from typing import Any, Dict, List
import logging
from loguru import logger

class BaseExtractor(ABC):
    """Base class for all data extractors in the ETL pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor with configuration.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the extractor
        """
        self.config = config
        self.logger = logger.bind(name=self.__class__.__name__)
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the source.
        
        Returns:
            List[Dict[str, Any]]: List of extracted records
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate the extractor configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        return True
    
    def handle_error(self, error: Exception) -> None:
        """
        Handle extraction errors.
        
        Args:
            error (Exception): The error that occurred
        """
        self.logger.error(f"Extraction error: {str(error)}")
        raise error 