"""
Ingredient Processing Parser.
Handles complex ingredient hierarchies from manufactured products.
"""

import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Set
import re

from base_parser import BaseParser, XMLUtils
from models import Ingredient, CodedConcept, Quantity, IngredientType


class IngredientParser(BaseParser):
    """Parser for ingredient information from manufactured products."""
    
    def __init__(self):
        super().__init__()
        self.substance_cache: Dict[str, str] = {}  # Cache for substance code to name mapping
        self.known_units = self._initialize_known_units()
    
    def parse(self, source: ET.Element) -> List[Ingredient]:
        """Required abstract method implementation - delegates to parse_ingredients."""
        return self.parse_ingredients(source)
    
    def parse_ingredients(self, product_element: ET.Element) -> List[Ingredient]:
        """
        Parse all ingredients from a manufactured product element.
        
        Args:
            product_element: XML element containing ingredient information
            
        Returns:
            List[Ingredient]: List of parsed ingredients (active and inactive)
        """
        print(f"[DEBUG] IngredientParser: Starting to parse ingredients")
        ingredients = []
        ingredient_elements = XMLUtils.find_all_elements(product_element, "hl7:ingredient")
        print(f"[DEBUG] IngredientParser: Found {len(ingredient_elements)} ingredient elements")
        
        for i, ingredient_element in enumerate(ingredient_elements):
            print(f"[DEBUG] IngredientParser: Processing ingredient {i+1}")
            try:
                ingredient = self.parse_single_ingredient(ingredient_element)
                if ingredient:
                    print(f"[DEBUG] IngredientParser: Successfully parsed ingredient: {ingredient.type.value} - {ingredient.substance_name}")
                    ingredients.append(ingredient)
                else:
                    print(f"[DEBUG] IngredientParser: Failed to parse ingredient {i+1}")
            except Exception as e:
                print(f"[DEBUG] IngredientParser: Exception parsing ingredient {i+1}: {str(e)}")
                self.add_error(f"Failed to parse ingredient: {str(e)}")
                continue
        
        print(f"[DEBUG] IngredientParser: Completed parsing, found {len(ingredients)} valid ingredients")
        return ingredients
    
    def parse_single_ingredient(self, ingredient_element: ET.Element) -> Optional[Ingredient]:
        """
        Parse a single ingredient element.
        
        Args:
            ingredient_element: <ingredient> XML element
            
        Returns:
            Ingredient: Parsed ingredient or None if parsing fails
        """
        print(f"[DEBUG] IngredientParser: Starting to parse single ingredient")
        
        # Determine ingredient type from classCode attribute - use direct access
        try:
            class_code = ingredient_element.get("classCode")
            print(f"[DEBUG] IngredientParser: Got classCode directly: '{class_code}'")
        except Exception as e:
            print(f"[DEBUG] IngredientParser: Exception getting classCode: {e}")
            class_code = None
            
        ingredient_type = self._parse_ingredient_type(class_code)
        
        if ingredient_type is None:
            print(f"[DEBUG] IngredientParser: Could not determine ingredient type")
            self.add_error(f"Unknown ingredient class code: {class_code}")
            return None
        
        ingredient = Ingredient(type=ingredient_type)
        print(f"[DEBUG] IngredientParser: Created ingredient with type: {ingredient_type}")
        
        # Parse quantity (mainly for active ingredients)
        try:
            quantity_element = XMLUtils.find_element(ingredient_element, "hl7:quantity")
            print(f"[DEBUG] IngredientParser: Found quantity element: {quantity_element is not None}")
            if quantity_element is not None:
                ingredient.quantity = self._parse_quantity(quantity_element)
                print(f"[DEBUG] IngredientParser: Parsed quantity: {ingredient.quantity}")
        except Exception as e:
            print(f"[DEBUG] IngredientParser: Exception parsing quantity: {e}")
        
        # Parse ingredient substance
        try:
            substance_element = XMLUtils.find_element(ingredient_element, "hl7:ingredientSubstance")
            print(f"[DEBUG] IngredientParser: Found substance element: {substance_element is not None}")
            if substance_element is not None:
                ingredient.substance_code, ingredient.substance_name = self._parse_substance(substance_element)
                print(f"[DEBUG] IngredientParser: Parsed substance: {ingredient.substance_name}")
                
                # Parse active moiety if present
                if ingredient_type == IngredientType.ACTIVE:
                    ingredient.active_moiety = self._parse_active_moiety(substance_element)
        except Exception as e:
            print(f"[DEBUG] IngredientParser: Exception parsing substance: {e}")
        
        # Validate ingredient
        if not ingredient.substance_name and not ingredient.substance_code:
            print(f"[DEBUG] IngredientParser: Ingredient validation failed - no substance info")
            self.add_error("Ingredient missing substance information")
            return None
        
        print(f"[DEBUG] IngredientParser: Successfully parsed ingredient: {ingredient.substance_name}")
        return ingredient
    
    def _parse_ingredient_type(self, class_code: str) -> Optional[IngredientType]:
        """Parse ingredient type from class code."""
        print(f"[DEBUG] IngredientParser: Parsing ingredient type from class_code: '{class_code}'")
        if not class_code:
            print(f"[DEBUG] IngredientParser: class_code is empty")
            return None
        
        class_code = class_code.upper()
        if class_code == "ACTIM":
            print(f"[DEBUG] IngredientParser: Recognized as ACTIVE ingredient")
            return IngredientType.ACTIVE
        elif class_code == "IACT":
            print(f"[DEBUG] IngredientParser: Recognized as INACTIVE ingredient")
            return IngredientType.INACTIVE
        else:
            print(f"[DEBUG] IngredientParser: Unknown class_code: '{class_code}'")
            return None
    
    def _parse_substance(self, substance_element: ET.Element) -> tuple[Optional[CodedConcept], Optional[str]]:
        """Parse substance code and name."""
        print(f"[DEBUG] IngredientParser: Starting to parse substance")
        
        # Parse substance code (typically UNII) - use direct access
        substance_code = None
        try:
            code_element = substance_element.find("{urn:hl7-org:v3}code")
            if code_element is not None:
                code_value = code_element.get("code")
                code_system = code_element.get("codeSystem")
                if code_value and code_system:
                    from models import CodedConcept
                    substance_code = CodedConcept(
                        code=code_value,
                        code_system=code_system,
                        display_name=None
                    )
                    print(f"[DEBUG] IngredientParser: Found substance code: {code_value}")
        except Exception as e:
            print(f"[DEBUG] IngredientParser: Exception parsing substance code: {e}")
        
        # Parse substance name - use direct access
        substance_name = None
        try:
            name_element = substance_element.find("{urn:hl7-org:v3}name")
            if name_element is not None and name_element.text:
                substance_name = name_element.text.strip()
                print(f"[DEBUG] IngredientParser: Found substance name: '{substance_name}'")
                
                # Clean and normalize substance name
                substance_name = self._normalize_substance_name(substance_name)
                print(f"[DEBUG] IngredientParser: Normalized substance name: '{substance_name}'")
        except Exception as e:
            print(f"[DEBUG] IngredientParser: Exception parsing substance name: {e}")
        
        # Cache substance information for future reference
        if substance_code and substance_name:
            self.substance_cache[substance_code.code] = substance_name
        
        print(f"[DEBUG] IngredientParser: Returning substance_code: {substance_code}, substance_name: '{substance_name}'")
        return substance_code, substance_name
    
    def _parse_active_moiety(self, substance_element: ET.Element) -> Optional[Ingredient]:
        """Parse active moiety information for active ingredients."""
        moiety_element = XMLUtils.find_element(substance_element, "hl7:activeMoiety")
        if moiety_element is None:
            return None
        
        inner_moiety = XMLUtils.find_element(moiety_element, "hl7:activeMoiety")
        if inner_moiety is None:
            return None
        
        # Create moiety as an ingredient without quantity
        moiety_ingredient = Ingredient(type=IngredientType.ACTIVE)
        
        # Parse moiety substance information
        moiety_ingredient.substance_code, moiety_ingredient.substance_name = self._parse_substance(inner_moiety)
        
        return moiety_ingredient if moiety_ingredient.substance_name or moiety_ingredient.substance_code else None
    
    def _parse_quantity(self, quantity_element: ET.Element) -> Optional[Quantity]:
        """Parse ingredient quantity with numerator and denominator."""
        numerator_element = XMLUtils.find_element(quantity_element, "hl7:numerator")
        denominator_element = XMLUtils.find_element(quantity_element, "hl7:denominator")
        
        if numerator_element is None:
            self.add_error("Quantity missing numerator")
            return None
        
        # Extract numerator
        numerator_value_str = XMLUtils.get_attribute(numerator_element, "value")
        numerator_unit = XMLUtils.get_attribute(numerator_element, "unit") or ""
        
        if not numerator_value_str:
            self.add_error("Numerator missing value")
            return None
        
        try:
            numerator_value = float(numerator_value_str)
        except ValueError:
            self.add_error(f"Invalid numerator value: {numerator_value_str}")
            return None
        
        # Extract denominator (defaults to 1 if not present)
        denominator_value = 1.0
        denominator_unit = ""
        
        if denominator_element is not None:
            denominator_value_str = XMLUtils.get_attribute(denominator_element, "value")
            denominator_unit = XMLUtils.get_attribute(denominator_element, "unit") or ""
            
            if denominator_value_str:
                try:
                    denominator_value = float(denominator_value_str)
                except ValueError:
                    self.add_error(f"Invalid denominator value: {denominator_value_str}")
                    denominator_value = 1.0
        
        # Normalize units
        numerator_unit = self._normalize_unit(numerator_unit)
        denominator_unit = self._normalize_unit(denominator_unit)
        
        return Quantity(
            numerator_value=numerator_value,
            numerator_unit=numerator_unit,
            denominator_value=denominator_value,
            denominator_unit=denominator_unit
        )
    
    def _normalize_substance_name(self, name: str) -> str:
        """Normalize substance name by cleaning and standardizing."""
        if not name:
            return ""
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name.strip())
        
        # Convert to uppercase for consistency (common practice in pharma)
        name = name.upper()
        
        # Remove common suffixes that don't add value
        suffixes_to_remove = [
            r'\s+\(UNII:\s*[A-Z0-9]+\)$',  # Remove UNII codes in parentheses
            r'\s+\[INN\]$',                 # Remove INN designation
            r'\s+\[USP\]$',                 # Remove USP designation
        ]
        
        for suffix_pattern in suffixes_to_remove:
            name = re.sub(suffix_pattern, '', name)
        
        return name.strip()
    
    def _normalize_unit(self, unit: str) -> str:
        """Normalize units to standard forms."""
        if not unit:
            return ""
        
        unit = unit.lower().strip()
        
        # Common unit normalizations
        unit_mappings = {
            'mg': 'mg',
            'milligram': 'mg',
            'milligrams': 'mg',
            'g': 'g',
            'gram': 'g', 
            'grams': 'g',
            'kg': 'kg',
            'kilogram': 'kg',
            'kilograms': 'kg',
            'ml': 'mL',
            'milliliter': 'mL',
            'milliliters': 'mL',
            'l': 'L',
            'liter': 'L',
            'liters': 'L',
            'mcg': 'mcg',
            'microgram': 'mcg',
            'micrograms': 'mcg',
            'ug': 'mcg',  # Alternative notation for microgram
            'units': 'units',
            'unit': 'units',
            'iu': 'IU',
            'international unit': 'IU',
            'international units': 'IU',
            '%': '%',
            'percent': '%',
            'percentage': '%'
        }
        
        return unit_mappings.get(unit, unit)
    
    def _initialize_known_units(self) -> Set[str]:
        """Initialize set of known pharmaceutical units."""
        return {
            'mg', 'g', 'kg', 'mcg', 'ng', 'pg',  # Mass
            'mL', 'L', 'dL', 'uL',               # Volume
            'IU', 'units',                        # Activity
            '%', 'ppm', 'ppb',                   # Concentration
            'mol', 'mmol', 'umol',               # Amount
            'tablet', 'capsule', 'dose',         # Count
            'mg/mL', 'mg/g', 'mcg/mL',          # Common ratios
        }
    
    def get_ingredient_summary(self, ingredients: List[Ingredient]) -> Dict[str, any]:
        """Generate summary statistics for a list of ingredients."""
        active_ingredients = [ing for ing in ingredients if ing.type == IngredientType.ACTIVE]
        inactive_ingredients = [ing for ing in ingredients if ing.type == IngredientType.INACTIVE]
        
        # Collect unique substances
        active_substances = {ing.substance_name for ing in active_ingredients if ing.substance_name}
        inactive_substances = {ing.substance_name for ing in inactive_ingredients if ing.substance_name}
        
        # Collect units used
        units_used = set()
        for ing in ingredients:
            if ing.quantity:
                if ing.quantity.numerator_unit:
                    units_used.add(ing.quantity.numerator_unit)
                if ing.quantity.denominator_unit:
                    units_used.add(ing.quantity.denominator_unit)
        
        return {
            'total_ingredients': len(ingredients),
            'active_count': len(active_ingredients),
            'inactive_count': len(inactive_ingredients),
            'active_substances': list(active_substances),
            'inactive_substances': list(inactive_substances),
            'units_used': list(units_used),
            'has_quantities': sum(1 for ing in ingredients if ing.quantity),
            'has_moieties': sum(1 for ing in active_ingredients if ing.active_moiety)
        }


class SubstanceValidator:
    """Utility for validating substance codes and names."""
    
    # UNII (FDA Unique Ingredient Identifier) pattern: 10 characters, alphanumeric
    UNII_PATTERN = re.compile(r'^[A-Z0-9]{10}$')
    
    @classmethod
    def is_valid_unii(cls, unii_code: str) -> bool:
        """Check if UNII code format is valid."""
        if not unii_code:
            return False
        return bool(cls.UNII_PATTERN.match(unii_code.upper().strip()))
    
    @classmethod
    def normalize_unii(cls, unii_code: str) -> Optional[str]:
        """Normalize UNII code to standard format."""
        if not unii_code:
            return None
        
        normalized = unii_code.upper().strip()
        return normalized if cls.is_valid_unii(normalized) else None
    
    @classmethod
    def validate_substance_name(cls, name: str) -> bool:
        """Basic validation for substance names."""
        if not name or not name.strip():
            return False
        
        # Check for minimum length
        if len(name.strip()) < 2:
            return False
        
        # Check for obviously invalid patterns
        invalid_patterns = [
            r'^\s*$',           # Empty or whitespace only
            r'^[\d\s]*$',       # Only digits and spaces
            r'^[^\w\s]*$',      # Only special characters
        ]
        
        for pattern in invalid_patterns:
            if re.match(pattern, name):
                return False
        
        return True