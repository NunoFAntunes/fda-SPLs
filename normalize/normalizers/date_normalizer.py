"""
Date normalization utilities for SPL documents.
Converts various date formats to ISO-8601 standard format.
"""

import re
from datetime import datetime
from typing import Optional, Union


class DateNormalizer:
    """Normalizes dates to ISO-8601 format."""
    
    # Common SPL date formats
    SPL_DATE_FORMATS = [
        '%Y%m%d',           # 20240901 (most common in SPL)
        '%Y-%m-%d',         # 2024-09-01
        '%m/%d/%Y',         # 09/01/2024
        '%m-%d-%Y',         # 09-01-2024
        '%d/%m/%Y',         # 01/09/2024 (less common)
        '%Y/%m/%d',         # 2024/09/01
        '%Y.%m.%d',         # 2024.09.01
        '%B %d, %Y',        # September 1, 2024
        '%b %d, %Y',        # Sep 1, 2024
        '%d %B %Y',         # 1 September 2024
        '%d %b %Y',         # 1 Sep 2024
        # Enhanced: datetime formats
        '%Y-%m-%dT%H:%M:%S',    # 2024-03-15T10:30:00 (ISO 8601 datetime)
        '%Y%m%d%H%M%S',         # 20240315103000 (compact datetime)
        '%Y-%m-%dT%H:%M:%S.%f', # 2024-03-15T10:30:00.123456 (with microseconds)
        '%Y-%m-%d %H:%M:%S',    # 2024-03-15 10:30:00 (space separated)
    ]
    
    # Regex patterns for date extraction
    DATE_PATTERNS = {
        'yyyymmdd': re.compile(r'\b(\d{8})\b'),                    # 20240901
        'yyyy-mm-dd': re.compile(r'\b(\d{4}-\d{2}-\d{2})\b'),      # 2024-09-01
        'mm/dd/yyyy': re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b'),  # 9/1/2024 or 09/01/2024
        'mm-dd-yyyy': re.compile(r'\b(\d{1,2}-\d{1,2}-\d{4})\b'),  # 9-1-2024 or 09-01-2024
        'text_dates': re.compile(r'\b((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4})\b', re.IGNORECASE),
    }
    
    @classmethod
    def normalize_date(cls, date_value: Union[str, int, None]) -> Optional[str]:
        """
        Normalize a date value to ISO-8601 format (YYYY-MM-DD).
        
        Args:
            date_value: Date in various formats (string, int, or None)
            
        Returns:
            str: ISO-8601 formatted date string or None if invalid
        """
        if not date_value:
            return None
        
        # Convert to string for processing
        date_str = str(date_value).strip()
        
        if not date_str:
            return None
        
        # Try parsing with known formats
        for date_format in cls.SPL_DATE_FORMATS:
            try:
                parsed_date = datetime.strptime(date_str, date_format)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # Try extracting date from longer text
        extracted_date = cls._extract_date_from_text(date_str)
        if extracted_date:
            return extracted_date
        
        return None
    
    @classmethod
    def _extract_date_from_text(cls, text: str) -> Optional[str]:
        """Extract and normalize the first valid date found in text."""
        for pattern_name, pattern in cls.DATE_PATTERNS.items():
            match = pattern.search(text)
            if match:
                date_str = match.group(1)
                
                # Try to parse the extracted date
                for date_format in cls.SPL_DATE_FORMATS:
                    try:
                        parsed_date = datetime.strptime(date_str, date_format)
                        return parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
        
        return None
    
    @classmethod
    def validate_iso_date(cls, date_str: str) -> bool:
        """
        Validate if a string is a valid ISO-8601 date.
        
        Args:
            date_str: Date string to validate
            
        Returns:
            bool: True if valid ISO-8601 date format
        """
        if not date_str:
            return False
        
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    @classmethod
    def normalize_effective_time(cls, effective_time: Optional[str]) -> Optional[str]:
        """
        Normalize SPL effective time values to ISO-8601 format.
        
        Args:
            effective_time: SPL effective time value
            
        Returns:
            str: Normalized ISO-8601 date or None
        """
        return cls.normalize_date(effective_time)
    
    @classmethod
    def get_current_date_iso(cls) -> str:
        """Get current date in ISO-8601 format."""
        return datetime.now().strftime('%Y-%m-%d')
    
    @classmethod
    def is_future_date(cls, date_str: str) -> bool:
        """
        Check if a normalized date is in the future.
        
        Args:
            date_str: ISO-8601 formatted date string
            
        Returns:
            bool: True if date is in the future
        """
        if not cls.validate_iso_date(date_str):
            return False
        
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            return date_obj.date() > datetime.now().date()
        except ValueError:
            return False
    
    @classmethod
    def is_reasonable_drug_date(cls, date_str: str) -> bool:
        """
        Check if date is reasonable for pharmaceutical data (not too old/new).
        
        Args:
            date_str: ISO-8601 formatted date string
            
        Returns:
            bool: True if date seems reasonable for drug data
        """
        if not cls.validate_iso_date(date_str):
            return False
        
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            current_year = datetime.now().year
            
            # Reasonable range: 1950 to 5 years in the future
            return 1950 <= date_obj.year <= (current_year + 5)
        except ValueError:
            return False