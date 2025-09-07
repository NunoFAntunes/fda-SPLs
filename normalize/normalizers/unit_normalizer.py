"""
Unit normalization utilities for SPL documents.
Converts common pharmaceutical units to UCUM (Unified Code for Units of Measure) standard.
"""

from typing import Dict, Optional, Tuple
import re


class UnitNormalizer:
    """Normalizes pharmaceutical units to UCUM standard format."""
    
    # Common SPL units to UCUM mappings
    UNIT_MAPPINGS = {
        # Mass units
        'g': 'g',           # gram
        'gram': 'g',
        'grams': 'g',
        'gm': 'g',
        'mg': 'mg',         # milligram  
        'milligram': 'mg',
        'milligrams': 'mg',
        'mcg': 'ug',        # microgram (UCUM uses 'ug')
        'μg': 'ug',
        'microgram': 'ug',
        'micrograms': 'ug',
        'kg': 'kg',         # kilogram
        'kilogram': 'kg',
        'kilograms': 'kg',
        'ug': 'ug',         # microgram
        
        # Volume units
        'l': 'L',           # liter
        'liter': 'L',
        'liters': 'L',
        'litre': 'L',
        'litres': 'L',
        'ml': 'mL',         # milliliter
        'mL': 'mL',
        'milliliter': 'mL',
        'milliliters': 'mL',
        'millilitre': 'mL',
        'millilitres': 'mL',
        'ul': 'uL',         # microliter
        'μl': 'uL',
        'microliter': 'uL',
        'microliters': 'uL',
        'microlitre': 'uL',
        'microlitres': 'uL',
        
        # Dose form units
        'tablet': '{tablet}',
        'tablets': '{tablet}',
        'capsule': '{capsule}',
        'capsules': '{capsule}',
        'unit': 'U',        # international unit
        'units': 'U',
        'iu': '[IU]',       # international unit (alternative)
        'international unit': '[IU]',
        'international units': '[IU]',
        
        # Time units (for dosing)
        'hour': 'h',
        'hours': 'h',
        'hr': 'h',
        'hrs': 'h',
        'h': 'h',
        'day': 'd',
        'days': 'd',
        'd': 'd',
        'week': 'wk',
        'weeks': 'wk',
        'wk': 'wk',
        'month': 'mo',
        'months': 'mo',
        'mo': 'mo',
        'year': 'a',        # annum in UCUM
        'years': 'a',
        'yr': 'a',
        'yrs': 'a',
        
        # Special pharmaceutical units
        'drop': 'gtt',      # drops
        'drops': 'gtt',
        'gtt': 'gtt',
        'spray': '{spray}',
        'sprays': '{spray}',
        'puff': '{puff}',
        'puffs': '{puff}',
        'application': '{application}',
        'applications': '{application}',
        
        # Concentration expressions
        'percent': '%',
        '%': '%',
        'ppm': '[ppm]',     # parts per million
        'ppb': '[ppb]',     # parts per billion
        
        # Area units (for topical drugs)
        'cm2': 'cm2',
        'cm²': 'cm2',
        'square cm': 'cm2',
        'square centimeter': 'cm2',
    }
    
    # Common unit combinations and their UCUM equivalents
    COMBINATION_MAPPINGS = {
        'mg/ml': 'mg/mL',
        'mg/mL': 'mg/mL',
        'g/l': 'g/L',
        'g/L': 'g/L',
        'mcg/ml': 'ug/mL',
        'ug/ml': 'ug/mL',
        'ug/mL': 'ug/mL',
        'mg/kg': 'mg/kg',
        'mg/day': 'mg/d',
        'mg/hr': 'mg/h',
        'units/ml': 'U/mL',
        'units/mL': 'U/mL',
        'iu/ml': '[IU]/mL',
        'iu/mL': '[IU]/mL',
    }
    
    # Regex patterns for unit extraction
    UNIT_PATTERN = re.compile(r'\b(\d*\.?\d+)\s*([a-zA-Zμ%]+(?:/[a-zA-Zμ%]+)?)\b')
    
    @classmethod
    def normalize_unit(cls, unit: str) -> Optional[str]:
        """
        Normalize a single unit to UCUM format.
        
        Args:
            unit: Unit string to normalize
            
        Returns:
            str: UCUM normalized unit or None if not recognized
        """
        if not unit:
            return None
        
        # Clean and normalize input
        unit_clean = unit.strip().lower()
        
        # Check direct mappings first
        if unit_clean in cls.UNIT_MAPPINGS:
            return cls.UNIT_MAPPINGS[unit_clean]
        
        # Check combination mappings
        if unit_clean in cls.COMBINATION_MAPPINGS:
            return cls.COMBINATION_MAPPINGS[unit_clean]
        
        # Try case-sensitive lookup for abbreviations
        unit_original = unit.strip()
        if unit_original in cls.UNIT_MAPPINGS:
            return cls.UNIT_MAPPINGS[unit_original]
        
        if unit_original in cls.COMBINATION_MAPPINGS:
            return cls.COMBINATION_MAPPINGS[unit_original]
        
        return None
    
    @classmethod
    def normalize_quantity_string(cls, quantity_str: str) -> Optional[Tuple[float, str]]:
        """
        Extract and normalize quantity with unit from string.
        
        Args:
            quantity_str: String containing quantity and unit (e.g., "250 mg")
            
        Returns:
            tuple: (normalized_value, normalized_unit) or None if parsing fails
        """
        if not quantity_str:
            return None
        
        # Try to extract number and unit
        match = cls.UNIT_PATTERN.search(quantity_str.strip())
        if not match:
            return None
        
        try:
            value = float(match.group(1))
            unit = match.group(2)
            
            normalized_unit = cls.normalize_unit(unit)
            if normalized_unit:
                return (value, normalized_unit)
        except (ValueError, IndexError):
            pass
        
        return None
    
    @classmethod
    def create_ucum_expression(cls, numerator_value: float, numerator_unit: str, 
                             denominator_value: float = 1.0, denominator_unit: str = "") -> str:
        """
        Create a UCUM expression from numerator and denominator components.
        
        Args:
            numerator_value: Numeric value for numerator
            numerator_unit: Unit for numerator
            denominator_value: Numeric value for denominator (default 1.0)
            denominator_unit: Unit for denominator (default empty)
            
        Returns:
            str: UCUM formatted expression
        """
        # Normalize units
        norm_num_unit = cls.normalize_unit(numerator_unit) or numerator_unit
        norm_denom_unit = cls.normalize_unit(denominator_unit) or denominator_unit
        
        # Format the expression
        if denominator_value == 1.0 and not denominator_unit:
            # Simple unit: "250 mg"
            return f"{numerator_value} {norm_num_unit}"
        else:
            # Ratio unit: "5 mg/mL" or "10 mg/2 mL"
            if denominator_value == 1.0:
                return f"{numerator_value} {norm_num_unit}/{norm_denom_unit}"
            else:
                return f"{numerator_value} {norm_num_unit}/{denominator_value} {norm_denom_unit}"
    
    @classmethod
    def is_valid_ucum_unit(cls, unit: str) -> bool:
        """
        Check if a unit string appears to be valid UCUM format.
        
        Args:
            unit: Unit string to validate
            
        Returns:
            bool: True if unit appears to be valid UCUM format
        """
        if not unit:
            return False
        
        # Check if it's one of our known UCUM mappings
        return unit in cls.UNIT_MAPPINGS.values() or unit in cls.COMBINATION_MAPPINGS.values()
    
    @classmethod
    def get_unit_category(cls, unit: str) -> Optional[str]:
        """
        Get the category of a normalized unit (mass, volume, time, etc.).
        
        Args:
            unit: UCUM normalized unit
            
        Returns:
            str: Unit category or None if not recognized
        """
        mass_units = {'g', 'mg', 'ug', 'kg'}
        volume_units = {'L', 'mL', 'uL'}
        time_units = {'h', 'd', 'wk', 'mo', 'a'}
        dose_form_units = {'{tablet}', '{capsule}', 'U', '[IU]'}
        special_units = {'gtt', '{spray}', '{puff}', '{application}'}
        
        if unit in mass_units:
            return 'mass'
        elif unit in volume_units:
            return 'volume'  
        elif unit in time_units:
            return 'time'
        elif unit in dose_form_units:
            return 'dose_form'
        elif unit in special_units:
            return 'special'
        elif '/' in unit:
            return 'ratio'
        elif unit in ['%', '[ppm]', '[ppb]']:
            return 'concentration'
        elif unit == 'cm2':
            return 'area'
        else:
            return None