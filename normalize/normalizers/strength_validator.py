"""
Strength and concentration validation utilities for SPL documents.
Validates that drug strengths are reasonable for their dosage forms and routes.
"""

from typing import Dict, Optional, Tuple, List
import re
from decimal import Decimal, InvalidOperation


class StrengthValidator:
    """Validates pharmaceutical strength and concentration values."""
    
    # Reasonable strength ranges by dosage form category (in mg unless specified)
    STRENGTH_RANGES = {
        'tablet': {
            'min': 0.001,      # 1 mcg (levothyroxine)
            'max': 2000,       # 2000 mg (calcium carbonate)
            'common_min': 0.1,
            'common_max': 1000,
        },
        'capsule': {
            'min': 0.001,
            'max': 1500,       # 1500 mg (fish oil)
            'common_min': 0.5,
            'common_max': 800,
        },
        'injection': {
            'min': 0.000001,   # 1 ng/mL (some biologics)
            'max': 500,        # High concentration preparations
            'common_min': 0.001,
            'common_max': 100,
        },
        'solution': {
            'min': 0.001,
            'max': 1000,       # Concentrated solutions
            'common_min': 0.1,
            'common_max': 200,
        },
        'cream': {
            'min': 0.001,     # 0.001% (potent steroids)
            'max': 50,         # 50% (urea creams)
            'common_min': 0.01,
            'common_max': 10,
        },
        'ointment': {
            'min': 0.001,
            'max': 50,
            'common_min': 0.01,
            'common_max': 10,
        },
    }
    
    # Suspicious patterns that may indicate data errors
    SUSPICIOUS_PATTERNS = {
        'too_many_decimals': re.compile(r'\d+\.\d{4,}'),  # More than 3 decimal places
        'scientific_notation': re.compile(r'\d+\.?\d*[eE][+-]?\d+'),
        'very_small': 0.000001,    # Less than 1 microgram
        'very_large': 10000,       # More than 10 grams
        'zero_strength': 0,
        'negative': lambda x: x < 0,
    }
    
    # Common unit conversions to mg
    UNIT_TO_MG = {
        'mg': 1,
        'g': 1000,
        'ug': 0.001,
        'mcg': 0.001,
        'kg': 1000000,
        'ng': 0.000001,
    }
    
    # Percentage to mg/mL conversion (assuming 1g/mL density)
    PERCENT_TO_MG_ML = 10  # 1% = 10 mg/mL
    
    @classmethod
    def validate_strength(cls, value: str, unit: str, dosage_form: str = None, 
                         route: str = None) -> Dict[str, any]:
        """
        Validate pharmaceutical strength value.
        
        Args:
            value: Strength value as string
            unit: Unit of measurement
            dosage_form: Dosage form (optional, for context)
            route: Route of administration (optional, for context)
            
        Returns:
            dict: Validation result with status, normalized_value, warnings, errors
        """
        result = {
            'is_valid': False,
            'normalized_value': None,
            'normalized_unit': None,
            'warnings': [],
            'errors': [],
            'severity': 'info'
        }
        
        if not value or not unit:
            result['errors'].append("Missing strength value or unit")
            result['severity'] = 'error'
            return result
        
        # Parse numeric value
        try:
            numeric_value = cls._parse_numeric_value(value)
            if numeric_value is None:
                result['errors'].append(f"Cannot parse numeric value: {value}")
                result['severity'] = 'error'
                return result
        except Exception as e:
            result['errors'].append(f"Error parsing value '{value}': {str(e)}")
            result['severity'] = 'error'
            return result
        
        # Normalize unit
        normalized_unit = cls._normalize_unit(unit)
        if not normalized_unit:
            result['warnings'].append(f"Unrecognized unit: {unit}")
            normalized_unit = unit
        
        # Convert to standard units (mg) for validation
        mg_value = cls._convert_to_mg(numeric_value, normalized_unit)
        
        # Check for suspicious patterns
        cls._check_suspicious_patterns(value, numeric_value, result)
        
        # Validate range based on dosage form
        if dosage_form:
            cls._validate_dosage_form_range(mg_value, dosage_form, result)
        
        # Additional validations based on route
        if route:
            cls._validate_route_compatibility(mg_value, normalized_unit, route, result)
        
        # Set final validation status
        result['is_valid'] = len(result['errors']) == 0
        result['normalized_value'] = numeric_value
        result['normalized_unit'] = normalized_unit
        
        if result['warnings'] and not result['errors']:
            result['severity'] = 'warning'
        elif result['errors']:
            result['severity'] = 'error'
        
        return result
    
    @classmethod
    def _parse_numeric_value(cls, value_str: str) -> Optional[float]:
        """Parse numeric value from string, handling various formats."""
        if not value_str:
            return None
        
        # Clean the string
        clean_value = value_str.strip()
        
        # Handle fractions like "1/2"
        if '/' in clean_value:
            parts = clean_value.split('/')
            if len(parts) == 2:
                try:
                    numerator = float(parts[0])
                    denominator = float(parts[1])
                    return numerator / denominator if denominator != 0 else None
                except (ValueError, ZeroDivisionError):
                    pass
        
        # Handle ranges like "5-10" (take first value)
        if '-' in clean_value and not clean_value.startswith('-'):
            range_parts = clean_value.split('-')
            if len(range_parts) >= 2:
                try:
                    return float(range_parts[0])
                except ValueError:
                    pass
        
        # Standard numeric parsing
        try:
            return float(clean_value)
        except ValueError:
            # Try to extract first number from string
            number_match = re.search(r'(\d+\.?\d*)', clean_value)
            if number_match:
                try:
                    return float(number_match.group(1))
                except ValueError:
                    pass
        
        return None
    
    @classmethod
    def _normalize_unit(cls, unit: str) -> str:
        """Normalize unit to standard form."""
        if not unit:
            return ""
        
        unit_mappings = {
            'milligram': 'mg',
            'milligrams': 'mg',
            'gram': 'g',
            'grams': 'g',
            'microgram': 'ug',
            'micrograms': 'ug',
            'mcg': 'ug',
            'μg': 'ug',
            'nanogram': 'ng',
            'nanograms': 'ng',
            'kilogram': 'kg',
            'kilograms': 'kg',
            'percent': '%',
            'percentage': '%',
        }
        
        unit_clean = unit.strip().lower()
        return unit_mappings.get(unit_clean, unit)
    
    @classmethod
    def _convert_to_mg(cls, value: float, unit: str) -> float:
        """Convert value to milligrams for standardized comparison."""
        if unit in cls.UNIT_TO_MG:
            return value * cls.UNIT_TO_MG[unit]
        elif unit == '%':
            return value * cls.PERCENT_TO_MG_ML
        else:
            # Unknown unit, return as-is
            return value
    
    @classmethod
    def _check_suspicious_patterns(cls, value_str: str, numeric_value: float, result: Dict):
        """Check for suspicious patterns in strength values."""
        # Too many decimal places
        if cls.SUSPICIOUS_PATTERNS['too_many_decimals'].search(value_str):
            result['warnings'].append("Unusually high precision (many decimal places)")
        
        # Scientific notation
        if cls.SUSPICIOUS_PATTERNS['scientific_notation'].search(value_str):
            result['warnings'].append("Scientific notation detected - verify value")
        
        # Zero or negative values
        if numeric_value <= 0:
            result['errors'].append("Strength cannot be zero or negative")
        
        # Extremely small values
        elif numeric_value < cls.SUSPICIOUS_PATTERNS['very_small']:
            result['warnings'].append("Extremely small strength value - verify units")
        
        # Extremely large values
        elif numeric_value > cls.SUSPICIOUS_PATTERNS['very_large']:
            result['warnings'].append("Extremely large strength value - verify units")
    
    @classmethod
    def _validate_dosage_form_range(cls, mg_value: float, dosage_form: str, result: Dict):
        """Validate strength against reasonable ranges for dosage form."""
        # Get the base dosage form (remove modifiers)
        base_form = dosage_form.split('_')[0] if dosage_form else None
        
        if base_form not in cls.STRENGTH_RANGES:
            return  # No range data for this form
        
        ranges = cls.STRENGTH_RANGES[base_form]
        
        # Check absolute limits
        if mg_value < ranges['min']:
            result['errors'].append(f"Strength too low for {dosage_form} (< {ranges['min']} mg)")
        elif mg_value > ranges['max']:
            result['errors'].append(f"Strength too high for {dosage_form} (> {ranges['max']} mg)")
        
        # Check common ranges (warnings only)
        elif mg_value < ranges['common_min']:
            result['warnings'].append(f"Unusually low strength for {dosage_form}")
        elif mg_value > ranges['common_max']:
            result['warnings'].append(f"Unusually high strength for {dosage_form}")
    
    @classmethod
    def _validate_route_compatibility(cls, mg_value: float, unit: str, route: str, result: Dict):
        """Validate strength based on route of administration."""
        if route == 'intravenous':
            # IV solutions should generally be in reasonable concentration ranges
            if unit in ['mg/mL', 'mg/ml'] and mg_value > 200:
                result['warnings'].append("Very high concentration for IV administration")
        
        elif route == 'topical':
            # Topical preparations often in percentages
            if unit == '%' and mg_value > 20:
                result['warnings'].append("Very high percentage for topical use")
        
        elif route == 'ophthalmic':
            # Eye preparations should be low concentration
            if unit == '%' and mg_value > 5:
                result['warnings'].append("High concentration for ophthalmic use")
    
    @classmethod
    def validate_strength_ratio(cls, numerator_value: str, numerator_unit: str,
                               denominator_value: str, denominator_unit: str) -> Dict[str, any]:
        """
        Validate strength expressed as a ratio (e.g., mg/mL).
        
        Args:
            numerator_value: Numerator value
            numerator_unit: Numerator unit
            denominator_value: Denominator value  
            denominator_unit: Denominator unit
            
        Returns:
            dict: Validation result
        """
        result = {
            'is_valid': False,
            'concentration': None,
            'warnings': [],
            'errors': []
        }
        
        # Validate numerator
        num_val = cls._parse_numeric_value(numerator_value)
        if num_val is None or num_val <= 0:
            result['errors'].append("Invalid numerator value")
            return result
        
        # Validate denominator
        denom_val = cls._parse_numeric_value(denominator_value)
        if denom_val is None or denom_val <= 0:
            result['errors'].append("Invalid denominator value")
            return result
        
        # Calculate concentration
        concentration = num_val / denom_val
        result['concentration'] = concentration
        
        # Validate reasonableness
        if concentration > 1000:  # More than 1000 mg/mL equivalent
            result['warnings'].append("Very high concentration")
        elif concentration < 0.001:  # Less than 1 mcg/mL equivalent
            result['warnings'].append("Very low concentration")
        
        result['is_valid'] = len(result['errors']) == 0
        return result