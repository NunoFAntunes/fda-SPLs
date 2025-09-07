"""
Indication mapper for extracting and inserting indication data into indications table.
"""

from typing import List, Dict, Any, Optional
import re

from .base_mapper import BaseMapper, MappingError
from ..db_connection import DatabaseConnection
from ...models import SPLDocument, SPLSection


class IndicationMapper(BaseMapper):
    """Maps indication text from SPL sections to indications table."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
    
    def insert_indications(self, document: SPLDocument) -> bool:
        """Extract and insert indications from SPL document."""
        try:
            # First, clean existing indications for this document
            self.delete_related_records('indications', 'spl_id', document.document_id)
            
            # Extract indications from relevant sections
            indications = self._extract_indications_from_document(document)
            
            if indications:
                count = self.insert_batch('indications', indications)
                self.logger.info(f"Inserted {count} indications for document {document.document_id}")
            else:
                self.logger.info(f"No indications found for document {document.document_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert indications for {document.document_id}: {str(e)}")
            raise MappingError(f"Indication mapping failed: {str(e)}") from e
    
    def _extract_indications_from_document(self, document: SPLDocument) -> List[Dict[str, Any]]:
        """Extract indication text from relevant SPL sections."""
        indications = []
        
        # Look for indication sections (LOINC code 34067-9)
        indication_sections = self._find_indication_sections(document.sections)
        
        for section in indication_sections:
            if section.text_content:
                # Extract individual indications from text
                indication_texts = self._parse_indication_text(section.text_content)
                
                for idx, indication_text in enumerate(indication_texts):
                    indication_data = {
                        'spl_id': document.document_id,
                        'indication_text': indication_text,
                        'indication_type': self._classify_indication_type(indication_text),
                        'condition_name': self._extract_condition_name(indication_text),
                        'population_restriction': self._extract_population_restriction(indication_text),
                    }
                    indications.append(indication_data)
        
        # If no dedicated indication sections, try to extract from other clinical sections
        if not indications:
            indications.extend(self._extract_from_other_sections(document))
        
        return indications
    
    def _find_indication_sections(self, sections: List[SPLSection]) -> List[SPLSection]:
        """Find sections that contain indication information."""
        indication_sections = []
        
        def search_sections(section_list):
            for section in section_list:
                # Primary indication section (LOINC 34067-9)
                if section.section_type == "34067-9":
                    indication_sections.append(section)
                
                # Also search subsections
                if section.subsections:
                    search_sections(section.subsections)
        
        search_sections(sections)
        return indication_sections
    
    def _parse_indication_text(self, text_content: str) -> List[str]:
        """Parse indication text into individual indication statements."""
        if not text_content:
            return []
        
        # Clean the text
        text = text_content.strip()
        
        # For simple cases, treat the entire text as one indication
        # More sophisticated parsing could be added here for complex texts
        if text:
            # Split on common delimiters but keep it simple for now
            # Could be enhanced to split on "and", bullet points, etc.
            return [text]
        
        return []
    
    def _classify_indication_type(self, indication_text: str) -> str:
        """Classify the type of indication based on text content."""
        text_lower = indication_text.lower()
        
        # Simple classification based on keywords
        prevention_keywords = ['prevent', 'prevention', 'prophylaxis', 'prophylactic']
        if any(keyword in text_lower for keyword in prevention_keywords):
            return 'prevention'
        
        # Default to primary for most cases
        return 'primary'
    
    def _extract_condition_name(self, indication_text: str) -> Optional[str]:
        """Extract condition/disease name from indication text."""
        # Simple extraction - could be enhanced with medical NLP
        text = indication_text.strip()
        
        # Remove common prefixes
        prefixes_to_remove = [
            'prevents and treats', 'treats', 'for the treatment of',
            'indicated for', 'used for', 'prevention and treatment of'
        ]
        
        for prefix in prefixes_to_remove:
            if text.lower().startswith(prefix):
                text = text[len(prefix):].strip()
                break
        
        # Take first 100 characters as condition name
        condition = text[:100].strip() if text else None
        return condition if condition else None
    
    def _extract_population_restriction(self, indication_text: str) -> Optional[str]:
        """Extract population restrictions from indication text."""
        text_lower = indication_text.lower()
        
        # Look for age restrictions
        age_patterns = [
            r'children (\d+) years? of age and older',
            r'adults and children (\d+) years? and older',
            r'children under (\d+) years?',
            r'ages? (\d+) and (?:older|above)',
            r'(\d+) years? and older',
        ]
        
        for pattern in age_patterns:
            match = re.search(pattern, text_lower)
            if match:
                return f"Age restriction found: {match.group(0)}"
        
        # Look for other population restrictions
        if 'children' in text_lower and 'adult' in text_lower:
            return 'Children and adults'
        elif 'children' in text_lower:
            return 'Children'
        elif 'adult' in text_lower:
            return 'Adults'
        
        return None
    
    def _extract_from_other_sections(self, document: SPLDocument) -> List[Dict[str, Any]]:
        """Extract indications from other sections if no dedicated indication section found."""
        indications = []
        
        # Look in other sections that might contain indication-like information
        for section in document.sections:
            # Skip sections we already processed
            if section.section_type == "34067-9":
                continue
            
            # Look for text that might indicate usage
            if section.text_content and len(section.text_content.strip()) > 10:
                text_lower = section.text_content.lower()
                
                # Simple heuristic: if section contains usage-related keywords
                usage_keywords = ['treats', 'treatment', 'used for', 'indicated', 'relief']
                if any(keyword in text_lower for keyword in usage_keywords):
                    indication_data = {
                        'spl_id': document.document_id,
                        'indication_text': section.text_content.strip(),
                        'indication_type': 'secondary',
                        'condition_name': self._extract_condition_name(section.text_content),
                        'population_restriction': None,
                    }
                    indications.append(indication_data)
                    break  # Only take the first one to avoid duplicates
        
        return indications
    
    def get_indications_for_medication(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get all indications for a specific medication."""
        try:
            query = """
            SELECT * FROM indications 
            WHERE spl_id = %(spl_id)s 
            ORDER BY indication_type, id
            """
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'spl_id': spl_id})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get indications for {spl_id}: {str(e)}")
            raise
    
    def search_indications_by_condition(self, condition_name: str) -> List[Dict[str, Any]]:
        """Search indications by condition name."""
        try:
            query = """
            SELECT i.*, m.brand_name, m.generic_name 
            FROM indications i
            JOIN medications m ON i.spl_id = m.spl_id
            WHERE i.condition_name ILIKE %(condition)s 
            OR i.indication_text ILIKE %(condition)s
            ORDER BY m.brand_name, m.generic_name
            """
            
            condition_pattern = f"%{condition_name}%"
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'condition': condition_pattern})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to search indications for condition {condition_name}: {str(e)}")
            raise