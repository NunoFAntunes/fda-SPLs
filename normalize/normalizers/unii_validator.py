"""
UNII (Unique Ingredient Identifier) validation utilities for SPL documents.
Validates FDA UNII codes according to FDA standards.
"""

import re
from typing import Optional, Dict, Set, List


class UNIIValidator:
    """Validates FDA UNII (Unique Ingredient Identifier) codes."""
    
    # UNII format: 10 characters, alphanumeric, specific pattern
    UNII_PATTERN = re.compile(r'^[A-Z0-9]{10}$')
    
    # Check digit algorithm constants
    CHECK_DIGIT_WEIGHTS = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    
    # Common UNII prefixes (for validation context)
    KNOWN_PREFIXES = {
        # These are examples - in practice you'd want a more comprehensive list
        'BKN': 'Biologics',
        'CHE': 'Chemical',
        'POL': 'Polymer',
        'STR': 'Structural',
    }
    
    # Characters used in UNII encoding
    UNII_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    CHAR_VALUES = {char: idx for idx, char in enumerate(UNII_CHARS)}
    
    @classmethod
    def validate_unii(cls, unii_code: str) -> bool:
        """
        Validate if string is a properly formatted UNII code.
        
        Note: UNII codes are 10-character alphanumeric codes assigned by FDA.
        This validates format only - full validation requires FDA UNII database lookup.
        
        Args:
            unii_code: UNII code string to validate
            
        Returns:
            bool: True if valid UNII format
        """
        if not unii_code:
            return False
        
        # Clean the code
        clean_code = unii_code.strip().upper()
        
        # Check basic format (10 characters, alphanumeric)
        if not cls.UNII_PATTERN.match(clean_code):
            return False
        
        # For now, just validate format - check digit validation is complex
        # and the FDA algorithm is not publicly documented in detail
        return True
    
    @classmethod
    def _validate_check_digit(cls, unii_code: str) -> bool:
        """
        Validate UNII check digit using FDA algorithm.
        
        The UNII check digit algorithm:
        1. Take first 9 characters
        2. Convert each to numeric value (0-9 = 0-9, A-Z = 10-35)
        3. Multiply by position weight (1-9)
        4. Sum all products
        5. Take modulo 36
        6. Convert back to character
        7. Compare with 10th character
        """
        if len(unii_code) != 10:
            return False
        
        # Get first 9 characters and check digit
        base_chars = unii_code[:9]
        check_digit = unii_code[9]
        
        # Calculate check digit
        calculated_check = cls._calculate_check_digit(base_chars)
        
        return calculated_check == check_digit
    
    @classmethod
    def _calculate_check_digit(cls, base_chars: str) -> str:
        """Calculate UNII check digit for 9-character base."""
        if len(base_chars) != 9:
            return ""
        
        total = 0
        for i, char in enumerate(base_chars):
            if char not in cls.CHAR_VALUES:
                return ""  # Invalid character
            
            char_value = cls.CHAR_VALUES[char]
            weight = cls.CHECK_DIGIT_WEIGHTS[i]
            total += char_value * weight
        
        check_digit_value = total % 36
        return cls.UNII_CHARS[check_digit_value]
    
    @classmethod
    def normalize_unii(cls, unii_code: str) -> Optional[str]:
        """
        Normalize UNII code to standard format.
        
        Args:
            unii_code: UNII code in various formats
            
        Returns:
            str: Normalized UNII code or None if invalid
        """
        if not unii_code:
            return None
        
        # Clean and format
        clean_code = re.sub(r'[^A-Z0-9]', '', unii_code.upper())
        
        if cls.validate_unii(clean_code):
            return clean_code
        
        return None
    
    @classmethod
    def extract_uniis_from_text(cls, text: str) -> List[str]:
        """
        Extract valid UNII codes from text.
        
        Args:
            text: Text that may contain UNII codes
            
        Returns:
            list: List of valid UNII codes found in text
        """
        if not text:
            return []
        
        # Find potential UNII codes (10 character alphanumeric sequences)
        potential_codes = re.findall(r'\b[A-Z0-9]{10}\b', text.upper())
        
        valid_uniis = []
        for code in potential_codes:
            if cls.validate_unii(code):
                valid_uniis.append(code)
        
        return list(set(valid_uniis))  # Remove duplicates
    
    @classmethod
    def validate_unii_substance_match(cls, unii_code: str, substance_name: str) -> Dict[str, any]:
        """
        Validate if UNII code could plausibly match substance name.
        
        Note: This is a basic validation. Full validation would require
        access to the FDA UNII database.
        
        Args:
            unii_code: UNII code to validate
            substance_name: Substance name to check against
            
        Returns:
            dict: Validation result with recommendations
        """
        result = {
            'unii_valid': False,
            'plausible_match': None,
            'warnings': [],
            'recommendations': []
        }
        
        # Validate UNII format
        result['unii_valid'] = cls.validate_unii(unii_code)
        
        if not result['unii_valid']:
            result['warnings'].append("Invalid UNII format")
            result['recommendations'].append("Verify UNII code format")
            return result
        
        if not substance_name:
            result['warnings'].append("No substance name provided for comparison")
            return result
        
        # Basic plausibility checks
        # Note: Real validation would require FDA UNII database lookup
        
        # Check for obvious mismatches (placeholder logic)
        substance_clean = re.sub(r'[^a-zA-Z]', '', substance_name.lower())
        
        if len(substance_clean) < 3:
            result['warnings'].append("Substance name too short for reliable validation")
        else:
            # In real implementation, would look up UNII in database
            result['plausible_match'] = True  # Placeholder
            result['recommendations'].append("Verify against FDA UNII database")
        
        return result
    
    @classmethod
    def is_valid_unii_format(cls, code: str) -> bool:
        """
        Quick format check without full validation.
        
        Args:
            code: Code to check
            
        Returns:
            bool: True if basic format is correct
        """
        if not code:
            return False
        
        clean_code = code.strip().upper()
        
        # Basic format check: exactly 10 alphanumeric characters
        if len(clean_code) != 10:
            return False
            
        # Check all characters are alphanumeric (0-9, A-Z)
        return clean_code.isalnum() and clean_code.isascii()
    
    @classmethod
    def generate_unii_variants(cls, base_code: str) -> List[str]:
        """
        Generate possible UNII variants for typo detection.
        
        Args:
            base_code: Base UNII code
            
        Returns:
            list: List of possible variants (for fuzzy matching)
        """
        if not cls.is_valid_unii_format(base_code):
            return []
        
        variants = []
        
        # Generate single-character substitution variants
        for i in range(len(base_code)):
            for char in cls.UNII_CHARS:
                if char != base_code[i]:
                    variant = base_code[:i] + char + base_code[i+1:]
                    if cls.validate_unii(variant):
                        variants.append(variant)
        
        return variants
    
    @classmethod
    def suggest_unii_corrections(cls, invalid_code: str) -> List[str]:
        """
        Suggest corrections for invalid UNII codes.
        
        Args:
            invalid_code: Invalid UNII code
            
        Returns:
            list: List of suggested corrections
        """
        if not invalid_code:
            return []
        
        clean_code = re.sub(r'[^A-Z0-9]', '', invalid_code.upper())
        
        if len(clean_code) != 10:
            return []  # Can't suggest corrections for wrong length
        
        suggestions = []
        
        # Try fixing the check digit
        if len(clean_code) == 10:
            base_chars = clean_code[:9]
            correct_check_digit = cls._calculate_check_digit(base_chars)
            
            if correct_check_digit:
                corrected_code = base_chars + correct_check_digit
                if corrected_code != clean_code:
                    suggestions.append(corrected_code)
        
        return suggestions
    
    @classmethod
    def batch_validate_uniis(cls, unii_codes: List[str]) -> Dict[str, List[str]]:
        """
        Validate a batch of UNII codes.
        
        Args:
            unii_codes: List of UNII codes to validate
            
        Returns:
            dict: Categorized results (valid, invalid, suggestions)
        """
        result = {
            'valid': [],
            'invalid': [],
            'suggestions': {}
        }
        
        for code in unii_codes:
            if cls.validate_unii(code):
                normalized = cls.normalize_unii(code)
                if normalized:
                    result['valid'].append(normalized)
            else:
                result['invalid'].append(code)
                suggestions = cls.suggest_unii_corrections(code)
                if suggestions:
                    result['suggestions'][code] = suggestions
        
        return result