"""
Section mapper for inserting SPL section data into spl_sections table.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from .base_mapper import BaseMapper, MappingError
from ..db_connection import DatabaseConnection
from ...models import SPLDocument, SPLSection


class SectionMapper(BaseMapper):
    """Maps SPL sections to spl_sections table."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
    
    def insert_sections(self, document: SPLDocument) -> bool:
        """Insert all sections from SPL document."""
        try:
            # First, clean existing sections for this document
            self.delete_related_records('spl_sections', 'spl_id', document.document_id)
            
            # Extract and insert all sections (including subsections)
            all_sections = []
            section_order = 1
            
            for section in document.sections:
                section_data = self._map_section_to_db(
                    document.document_id, section, section_order, None
                )
                if section_data:
                    all_sections.append(section_data)
                    section_order += 1
                    
                    # Process subsections recursively
                    subsection_data = self._extract_subsections(
                        document.document_id, section, section_order
                    )
                    all_sections.extend(subsection_data[0])
                    section_order = subsection_data[1]
            
            if all_sections:
                count = self.insert_batch('spl_sections', all_sections)
                self.logger.info(f"Inserted {count} sections for document {document.document_id}")
            else:
                self.logger.info(f"No sections found for document {document.document_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert sections for {document.document_id}: {str(e)}")
            raise MappingError(f"Section mapping failed: {str(e)}") from e
    
    def _extract_subsections(self, spl_id: str, parent_section: SPLSection, 
                           start_order: int, parent_section_uuid: Optional[str] = None) -> tuple:
        """Recursively extract subsections."""
        subsections = []
        current_order = start_order
        
        for subsection in parent_section.subsections:
            subsection_data = self._map_section_to_db(
                spl_id, subsection, current_order, parent_section_uuid
            )
            if subsection_data:
                subsections.append(subsection_data)
                current_order += 1
                
                # Recursively process nested subsections
                nested_data = self._extract_subsections(
                    spl_id, subsection, current_order, subsection_data.get('id')
                )
                subsections.extend(nested_data[0])
                current_order = nested_data[1]
        
        return subsections, current_order
    
    def _map_section_to_db(self, spl_id: str, section: SPLSection, 
                          section_order: int, parent_section_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Map a single SPL section to database record."""
        try:
            section_data = {
                'spl_id': spl_id,
                'section_id': section.section_id,
                'loinc_code': self._extract_loinc_code(section),
                'section_title': section.title,
                'section_text': self._clean_section_text(section.text_content),
                'section_order': section_order,
                'effective_time': self._parse_date(section.effective_time),
                'parent_section_id': parent_section_id,
            }
            
            return section_data
            
        except Exception as e:
            self.logger.warning(f"Failed to map section {section.section_id}: {str(e)}")
            return None
    
    def _extract_loinc_code(self, section: SPLSection) -> Optional[str]:
        """Extract LOINC code from section."""
        # Try section_type first (this is the mapped LOINC code)
        if section.section_type:
            return section.section_type
        
        # Fall back to section_code if available
        if section.section_code and section.section_code.code_system == "2.16.840.1.113883.6.1":
            return section.section_code.code
        
        return None
    
    def _clean_section_text(self, text_content: Optional[str]) -> Optional[str]:
        """Clean and normalize section text content."""
        if not text_content:
            return None
        
        # Basic text cleaning
        cleaned = text_content.strip()
        
        # Remove excessive whitespace
        import re
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Return None for empty strings after cleaning
        return cleaned if cleaned else None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse various date formats from SPL data."""
        if not date_str:
            return None
        
        # Remove whitespace
        date_str = date_str.strip()
        
        # Try different date formats
        formats = [
            '%Y%m%d',           # 20240315
            '%Y-%m-%d',         # 2024-03-15
            '%Y%m%d%H%M%S',     # 20240315103000
            '%Y-%m-%dT%H:%M:%S', # 2024-03-15T10:30:00
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        self.logger.warning(f"Could not parse section date: {date_str}")
        return None
    
    def get_sections_for_medication(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get all sections for a specific medication."""
        try:
            query = """
            SELECT * FROM spl_sections 
            WHERE spl_id = %(spl_id)s 
            ORDER BY section_order, section_id
            """
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'spl_id': spl_id})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get sections for {spl_id}: {str(e)}")
            raise
    
    def get_sections_by_loinc_code(self, spl_id: str, loinc_codes: List[str]) -> List[Dict[str, Any]]:
        """Get sections with specific LOINC codes for a medication."""
        try:
            placeholders = ', '.join(['%s'] * len(loinc_codes))
            query = f"""
            SELECT * FROM spl_sections 
            WHERE spl_id = %s AND loinc_code IN ({placeholders})
            ORDER BY section_order
            """
            
            params = [spl_id] + loinc_codes
            
            with self.db.transaction() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get sections by LOINC for {spl_id}: {str(e)}")
            raise
    
    def get_clinical_sections(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get clinical sections that contain important medical information."""
        clinical_loinc_codes = [
            '34067-9',  # Indications and Usage
            '34071-1',  # Warnings
            '34068-7',  # Dosage and Administration
            '50570-1',  # Do Not Use
            '50569-3',  # Ask Doctor Before Use
            '50567-7',  # When Using This Product
            '50565-1',  # Keep Out of Reach of Children
            '55106-9',  # Active Ingredients
            '51727-6',  # Inactive Ingredients
            '44425-7',  # Storage and Handling
        ]
        
        return self.get_sections_by_loinc_code(spl_id, clinical_loinc_codes)
    
    def get_sections_with_text(self, spl_id: str) -> List[Dict[str, Any]]:
        """Get sections that have text content (for LLM processing)."""
        try:
            query = """
            SELECT * FROM spl_sections 
            WHERE spl_id = %(spl_id)s 
            AND section_text IS NOT NULL 
            AND LENGTH(TRIM(section_text)) > 0
            ORDER BY section_order
            """
            
            with self.db.transaction() as cursor:
                cursor.execute(query, {'spl_id': spl_id})
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"Failed to get text sections for {spl_id}: {str(e)}")
            raise