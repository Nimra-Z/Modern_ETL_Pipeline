import pandas as pd
from typing import Any, Dict, List
from .base import BaseTransformer
from loguru import logger

class DataCleaner(BaseTransformer):
    """Transformer for cleaning and standardizing data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data cleaner.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary for the data cleaner
        """
        super().__init__(config)
        self.rules = config.get('rules', {})
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and standardize the input data.
        
        Args:
            data (List[Dict[str, Any]]): List of records to clean
            
        Returns:
            List[Dict[str, Any]]: Cleaned records
        """
        try:
            self.logger.info("Starting data cleaning process")
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data)
            
            # Apply cleaning rules
            if self.rules.get('remove_duplicates'):
                df = self._remove_duplicates(df)
            
            if self.rules.get('handle_missing_values'):
                df = self._handle_missing_values(df)
            
            if self.rules.get('standardize_dates'):
                df = self._standardize_dates(df)
            
            # Convert back to list of dictionaries
            cleaned_data = df.to_dict('records')
            
            self.logger.info(f"Successfully cleaned {len(cleaned_data)} records")
            return cleaned_data
            
        except Exception as e:
            self.handle_error(e)
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows from the DataFrame."""
        original_len = len(df)
        df = df.drop_duplicates()
        removed = original_len - len(df)
        self.logger.info(f"Removed {removed} duplicate records")
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the DataFrame."""
        strategy = self.rules.get('handle_missing_values')
        
        if strategy == 'mean':
            numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
        elif strategy == 'median':
            numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
        elif strategy == 'mode':
            categorical_columns = df.select_dtypes(include=['object']).columns
            df[categorical_columns] = df[categorical_columns].fillna(df[categorical_columns].mode().iloc[0])
        
        return df
    
    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize date columns in the DataFrame."""
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])
        return df 