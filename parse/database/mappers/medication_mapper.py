"""
Medication mapper for inserting core SPL document metadata into medications table.
"""

from typing import Optional, Dict, Any
from datetime import datetime

from .base_mapper import BaseMapper, MappingError
from ..db_connection import DatabaseConnection
from ...models import SPLDocument, ManufacturedProduct


class MedicationMapper(BaseMapper):
    """Maps SPLDocument objects to medications table."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
    
    def insert(self, document: SPLDocument) -> bool:
        """Insert SPL document metadata into medications table."""
        try:
            # Extract manufactured product info (if available)
            product_info = self._extract_product_info(document)
            
            # Build medication record
            medication_data = {
                'spl_id': document.document_id,
                'set_id': document.set_id,
                'version_number': int(document.version_number) if document.version_number else 1,
                'effective_date': self._parse_date(document.effective_time),
                
                # Product information from manufactured product
                'brand_name': product_info.get('brand_name'),
                'generic_name': product_info.get('generic_name'),
                'manufacturer': product_info.get('manufacturer'),
                'labeler': product_info.get('labeler'),
                
                # Product identifiers
                'ndc_code': product_info.get('ndc_code'),
                'product_form': product_info.get('product_form'),
                'route_of_administration': product_info.get('route_of_administration'),
                
                # Regulatory information
                'approval_type': product_info.get('approval_type'),
                'approval_id': product_info.get('approval_id'),
                'marketing_status': product_info.get('marketing_status'),
                'marketing_date_start': product_info.get('marketing_date_start'),
                
                # Document metadata
                'document_code': document.document_code.code if document.document_code else None,
                'document_display_name': document.document_code.display_name if document.document_code else None,
                
                # Processing information
                'processed_at': datetime.now(),
                'processing_status': 'completed',
                'processing_errors': None,
                
                # Store additional data as JSON
                'additional_data': self._build_additional_data(document),
            }
            
            # Use upsert to handle version updates
            self.upsert_record(
                table='medications',
                data=medication_data,
                conflict_columns=['set_id', 'version_number'],
                update_columns=['effective_date', 'brand_name', 'generic_name', 'manufacturer', 
                              'ndc_code', 'product_form', 'processed_at', 'additional_data']
            )
            
            self.logger.info(f"Successfully inserted/updated medication: {document.document_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert medication {document.document_id}: {str(e)}")
            raise MappingError(f"Medication mapping failed: {str(e)}") from e
    
    def _extract_product_info(self, document: SPLDocument) -> Dict[str, Any]:
        """Extract product information from SPL document sections."""
        product_info = {}
        
        # Look for SPL Listing section (LOINC 48780-1) with manufactured product
        for section in document.sections:
            if (section.section_type == "48780-1" and 
                section.manufactured_product is not None):
                
                product = section.manufactured_product
                
                # Basic product information
                product_info['brand_name'] = product.product_name
                product_info['generic_name'] = product.generic_name
                
                # NDC code (primary product code)
                if product.product_code and product.product_code.code_system == "2.16.840.1.113883.6.69":
                    product_info['ndc_code'] = product.product_code.code
                
                # Product form
                if product.form_code:
                    product_info['product_form'] = product.form_code.display_name or product.form_code.code
                
                # Route of administration
                if product.routes_of_administration:
                    routes = [route.route_code.display_name or route.route_code.code 
                             for route in product.routes_of_administration]
                    product_info['route_of_administration'] = ', '.join(routes)
                
                # Marketing information
                if product.marketing_info:
                    product_info['marketing_status'] = product.marketing_info.status_code
                    product_info['marketing_date_start'] = self._parse_date(
                        product.marketing_info.effective_date_low
                    )
                
                # Approval information
                if product.approval_info:
                    product_info['approval_id'] = product.approval_info.approval_id
                    if product.approval_info.approval_type:
                        product_info['approval_type'] = (
                            product.approval_info.approval_type.display_name or 
                            product.approval_info.approval_type.code
                        )
                
                break
        
        # Extract manufacturer/labeler from document author
        if document.author and document.author.organizations:
            # Use first organization as primary manufacturer
            org = document.author.organizations[0]
            if org.name:
                product_info['manufacturer'] = org.name
                product_info['labeler'] = org.name
        
        return product_info
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse various date formats from SPL data."""
        if not date_str:
            return None
        
        # Remove whitespace
        date_str = date_str.strip()
        
        # Try different date formats commonly found in SPL
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
        
        self.logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def _build_additional_data(self, document: SPLDocument) -> Dict[str, Any]:
        """Build additional data JSON with extra document information."""
        additional_data = {}
        
        # Document processing information
        if document.processing_errors:
            additional_data['processing_errors'] = document.processing_errors
        
        if document.processed_at:
            additional_data['original_processed_at'] = document.processed_at.isoformat()
        
        # Document code information
        if document.document_code:
            additional_data['document_code_info'] = {
                'code': document.document_code.code,
                'code_system': document.document_code.code_system,
                'display_name': document.document_code.display_name
            }
        
        # Author information (full details)
        if document.author:
            additional_data['author_info'] = {
                'time': document.author.time,
                'organizations': []
            }
            
            for org in document.author.organizations:
                org_data = {
                    'name': org.name,
                    'id_root': org.id_root,
                    'id_extension': org.id_extension
                }
                additional_data['author_info']['organizations'].append(org_data)
        
        # Section summary
        additional_data['section_summary'] = {
            'total_sections': len(document.sections),
            'sections_with_text': len([s for s in document.sections if s.text_content]),
            'section_types': list(set(s.section_type for s in document.sections if s.section_type))
        }
        
        return additional_data
    
    def medication_exists(self, spl_id: str) -> bool:
        """Check if medication already exists."""
        return self.record_exists('medications', {'spl_id': spl_id})
    
    def get_medication(self, spl_id: str) -> Optional[Dict[str, Any]]:
        """Get existing medication record."""
        return self.get_record('medications', {'spl_id': spl_id})