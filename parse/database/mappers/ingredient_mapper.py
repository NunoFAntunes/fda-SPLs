"""
Ingredient mapper for inserting ingredient data into ingredients table.
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal

from .base_mapper import BaseMapper, MappingError
from ..db_connection import DatabaseConnection
from ...models import SPLDocument, Ingredient


class IngredientMapper(BaseMapper):
    """Maps ingredient data from SPLDocument to ingredients table."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
    
    def insert_ingredients(self, document: SPLDocument) -> bool:
        """Insert all ingredients from SPL document."""
        try:
            # First, clean existing ingredients for this document
            self.delete_related_records('ingredients', 'spl_id', document.document_id)
            
            # Extract and insert all ingredients
            all_ingredients = self._extract_all_ingredients(document)
            
            if all_ingredients:
                count = self.insert_batch('ingredients', all_ingredients)
                self.logger.info(f"Inserted {count} ingredients for document {document.document_id}")
            else:
                self.logger.info(f"No ingredients found for document {document.document_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert ingredients for {document.document_id}: {str(e)}")
            raise MappingError(f"Ingredient mapping failed: {str(e)}") from e
    
    def _extract_all_ingredients(self, document: SPLDocument) -> List[Dict[str, Any]]:
        """Extract all ingredients from document sections."""
        ingredients = []
        
        # Look for SPL Listing sections (LOINC 48780-1) with manufactured products
        for section in document.sections:
            if (section.section_code and section.section_code.code == "48780-1" and 
                section.manufactured_product is not None):
                
                product = section.manufactured_product
                
                # Process all ingredients in this product
                for ingredient in product.ingredients:
                    ingredient_data = self._map_ingredient_to_db(document.document_id, ingredient)
                    if ingredient_data:
                        ingredients.append(ingredient_data)
        
        return ingredients
    
    def _map_ingredient_to_db(self, spl_id: str, ingredient: Ingredient) -> Optional[Dict[str, Any]]:
        """Map a single ingredient to database record."""
        try:
            # Determine ingredient type
            ingredient_type = 'active' if ingredient.type == 'ACTIM' else 'inactive'
            
            # Build ingredient record
            ingredient_data = {
                'spl_id': spl_id,
                'ingredient_type': ingredient_type,
                'substance_name': ingredient.substance_name,
                'unii_code': self._extract_unii_code(ingredient),
                
                # Strength/quantity information
                'strength_numerator': self._extract_strength_numerator(ingredient),
                'strength_numerator_unit': self._extract_strength_numerator_unit(ingredient),
                'strength_denominator': self._extract_strength_denominator(ingredient),
                'strength_denominator_unit': self._extract_strength_denominator_unit(ingredient),
                
                # Active moiety information (for active ingredients)
                'active_moiety_name': self._extract_active_moiety_name(ingredient),
                'active_moiety_unii': self._extract_active_moiety_unii(ingredient),
            }
            
            return ingredient_data
            
        except Exception as e:
            self.logger.warning(f"Failed to map ingredient {ingredient.substance_name}: {str(e)}")
            return None
    
    def _extract_unii_code(self, ingredient: Ingredient) -> Optional[str]:
        """Extract UNII code from ingredient substance code."""
        if (ingredient.substance_code and 
            ingredient.substance_code.code_system == "2.16.840.1.113883.4.9"):
            return ingredient.substance_code.code
        return None
    
    def _extract_strength_numerator(self, ingredient: Ingredient) -> Optional[Decimal]:
        """Extract strength numerator value."""
        if ingredient.quantity and ingredient.quantity.numerator_value is not None:
            try:
                return Decimal(str(ingredient.quantity.numerator_value))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid numerator value: {ingredient.quantity.numerator_value}")
        return None
    
    def _extract_strength_numerator_unit(self, ingredient: Ingredient) -> Optional[str]:
        """Extract strength numerator unit."""
        if ingredient.quantity:
            return ingredient.quantity.numerator_unit
        return None
    
    def _extract_strength_denominator(self, ingredient: Ingredient) -> Optional[Decimal]:
        """Extract strength denominator value."""
        if ingredient.quantity and ingredient.quantity.denominator_value is not None:
            try:
                return Decimal(str(ingredient.quantity.denominator_value))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid denominator value: {ingredient.quantity.denominator_value}")
        return None
    
    def _extract_strength_denominator_unit(self, ingredient: Ingredient) -> Optional[str]:
        """Extract strength denominator unit."""
        if ingredient.quantity:
            return ingredient.quantity.denominator_unit
        return None
    
    def _extract_active_moiety_name(self, ingredient: Ingredient) -> Optional[str]:
        """Extract active moiety substance name."""
        if ingredient.active_moiety:
            return ingredient.active_moiety.substance_name
        return None
    
    def _extract_active_moiety_unii(self, ingredient: Ingredient) -> Optional[str]:
        """Extract active moiety UNII code."""
        if (ingredient.active_moiety and 
            ingredient.active_moiety.substance_code and
            ingredient.active_moiety.substance_code.code_system == "2.16.840.1.113883.4.9"):
            return ingredient.active_moiety.substance_code.code
        return None
    
    def get_ingredients_for_medication(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get all ingredients for a specific medication."""
        try:
            query = "SELECT * FROM ingredients WHERE spl_id = %(spl_id)s ORDER BY ingredient_type, substance_name"
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'spl_id': spl_id})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get ingredients for {spl_id}: {str(e)}")
            raise
    
    def get_active_ingredients_for_medication(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get only active ingredients for a specific medication."""
        try:
            query = """
            SELECT * FROM ingredients 
            WHERE spl_id = %(spl_id)s AND ingredient_type = 'active'
            ORDER BY substance_name
            """
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'spl_id': spl_id})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get active ingredients for {spl_id}: {str(e)}")
            raise