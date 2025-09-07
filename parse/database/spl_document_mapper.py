"""
Main SPL Document Mapper orchestrator.
Coordinates insertion of complete SPL documents into the simplified database.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .db_connection import DatabaseConnection, get_database_connection
from .mappers.medication_mapper import MedicationMapper
from .mappers.ingredient_mapper import IngredientMapper
from .mappers.section_mapper import SectionMapper
from .mappers.indication_mapper import IndicationMapper
from ..models import SPLDocument

logger = logging.getLogger(__name__)


class ProcessingResult:
    """Result container for document processing operations."""
    
    def __init__(self, success: bool = True, spl_id: Optional[str] = None, 
                 error: Optional[str] = None, skipped: bool = False, 
                 reason: Optional[str] = None, processing_time: Optional[float] = None):
        self.success = success
        self.spl_id = spl_id
        self.error = error
        self.skipped = skipped
        self.reason = reason
        self.processing_time = processing_time
        self.timestamp = datetime.now()
    
    def __str__(self) -> str:
        if self.skipped:
            return f"ProcessingResult(SKIPPED, spl_id={self.spl_id}, reason={self.reason})"
        elif self.success:
            return f"ProcessingResult(SUCCESS, spl_id={self.spl_id}, time={self.processing_time:.3f}s)"
        else:
            return f"ProcessingResult(FAILED, spl_id={self.spl_id}, error={self.error})"


class SPLDocumentMapper:
    """
    Main orchestrator for inserting SPL documents into the simplified database.
    Coordinates all mapper components with transaction safety.
    """
    
    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        self.db = db_connection or get_database_connection()
        self.logger = logger.getChild(self.__class__.__name__)
        
        # Initialize mapper components
        self.medication_mapper = MedicationMapper(self.db)
        self.ingredient_mapper = IngredientMapper(self.db)
        self.section_mapper = SectionMapper(self.db)
        self.indication_mapper = IndicationMapper(self.db)
    
    def insert_document(self, document: SPLDocument) -> ProcessingResult:
        """
        Insert complete SPL document into simplified database.
        Uses transaction safety to ensure all-or-nothing insertion.
        """
        start_time = datetime.now()
        
        try:
            # Validate document
            if not self._validate_document(document):
                return ProcessingResult(
                    success=False, 
                    spl_id=document.document_id,
                    error="Document validation failed"
                )
            
            # Check for duplicates (optional - could allow updates)
            if self._should_skip_document(document):
                return ProcessingResult(
                    skipped=True,
                    spl_id=document.document_id,
                    reason="Document already exists with same version"
                )
            
            # Insert document with transaction safety
            with self.db.transaction():
                self.logger.info(f"Starting insertion of document: {document.document_id}")
                
                # Step 1: Insert main medication record (includes manufacturer, NDC, regulatory)
                self.medication_mapper.insert(document)
                self.logger.debug("✓ Medication metadata inserted")
                
                # Step 2: Process ingredients
                self.ingredient_mapper.insert_ingredients(document)
                self.logger.debug("✓ Ingredients inserted")
                
                # Step 3: Process all sections
                self.section_mapper.insert_sections(document)
                self.logger.debug("✓ Sections inserted")
                
                # Step 4: Extract and insert indications
                self.indication_mapper.insert_indications(document)
                self.logger.debug("✓ Indications inserted")
                
                # Transaction automatically commits here if no exceptions
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"Successfully inserted document {document.document_id} in {processing_time:.3f}s")
            
            return ProcessingResult(
                success=True,
                spl_id=document.document_id,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Failed to insert document {document.document_id}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            return ProcessingResult(
                success=False,
                spl_id=document.document_id,
                error=error_msg,
                processing_time=processing_time
            )
    
    def _validate_document(self, document: SPLDocument) -> bool:
        """Validate document has required fields for database insertion."""
        try:
            # Check required fields
            if not document.document_id:
                self.logger.error("Document missing document_id")
                return False
            
            if not document.set_id:
                self.logger.error("Document missing set_id")
                return False
            
            if not document.version_number:
                self.logger.warning("Document missing version_number, will use default")
            
            # Document should have at least some sections
            if not document.sections:
                self.logger.warning("Document has no sections")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Document validation error: {str(e)}")
            return False
    
    def _should_skip_document(self, document: SPLDocument) -> bool:
        """Check if document should be skipped (e.g., already exists)."""
        try:
            # Check if medication already exists
            existing = self.medication_mapper.get_medication(document.document_id)
            if existing:
                # For now, allow updates (don't skip)
                self.logger.info(f"Document {document.document_id} already exists, will update")
                return False
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Error checking document existence: {str(e)}")
            return False
    
    def get_document_summary(self, spl_id: str) -> Optional[Dict[str, Any]]:
        """Get complete document summary from database."""
        try:
            # Get medication info
            medication = self.medication_mapper.get_medication(spl_id)
            if not medication:
                return None
            
            # Get related data
            ingredients = self.ingredient_mapper.get_ingredients_for_medication(spl_id)
            sections = self.section_mapper.get_sections_with_text(spl_id)
            indications = self.indication_mapper.get_indications_for_medication(spl_id)
            
            return {
                'medication': dict(medication),
                'ingredients': ingredients,
                'sections': sections,
                'indications': indications,
                'summary': {
                    'total_ingredients': len(ingredients),
                    'active_ingredients': len([i for i in ingredients if i['ingredient_type'] == 'active']),
                    'total_sections': len(sections),
                    'total_indications': len(indications),
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get document summary for {spl_id}: {str(e)}")
            return None
    
    def document_exists(self, spl_id: str) -> bool:
        """Check if document exists in database."""
        return self.medication_mapper.medication_exists(spl_id)
    
    def delete_document(self, spl_id: str) -> bool:
        """Delete complete document and all related records."""
        try:
            with self.db.transaction():
                # Delete from all related tables (foreign keys will cascade)
                self.indication_mapper.delete_related_records('indications', 'spl_id', spl_id)
                self.section_mapper.delete_related_records('spl_sections', 'spl_id', spl_id)
                self.ingredient_mapper.delete_related_records('ingredients', 'spl_id', spl_id)
                
                # Delete main medication record
                query = "DELETE FROM medications WHERE spl_id = %(spl_id)s"
                with self.db.transaction() as cursor:
                    cursor.execute(query, {'spl_id': spl_id})
                    deleted_count = cursor.rowcount
                
                self.logger.info(f"Deleted document {spl_id} and all related records")
                return deleted_count > 0
                
        except Exception as e:
            self.logger.error(f"Failed to delete document {spl_id}: {str(e)}")
            return False


def process_spl_document(document: SPLDocument) -> ProcessingResult:
    """
    Convenience function to process a single SPL document.
    
    Args:
        document: SPLDocument instance to insert
        
    Returns:
        ProcessingResult: Success/failure result with details
    """
    mapper = SPLDocumentMapper()
    return mapper.insert_document(document)


def process_spl_documents(documents: list) -> Dict[str, Any]:
    """
    Convenience function to process multiple SPL documents.
    
    Args:
        documents: List of SPLDocument instances
        
    Returns:
        Dict with processing summary and results
    """
    mapper = SPLDocumentMapper()
    results = []
    
    start_time = datetime.now()
    
    for document in documents:
        result = mapper.insert_document(document)
        results.append(result)
    
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Calculate summary statistics
    successful = [r for r in results if r.success]
    skipped = [r for r in results if r.skipped]
    failed = [r for r in results if not r.success and not r.skipped]
    
    return {
        'total_documents': len(documents),
        'successful': len(successful),
        'skipped': len(skipped),
        'failed': len(failed),
        'total_time': total_time,
        'avg_time_per_doc': total_time / len(documents) if documents else 0,
        'results': results,
        'summary': {
            'success_rate': len(successful) / len(documents) if documents else 0,
            'processing_speed': len(documents) / total_time if total_time > 0 else 0,
        }
    }


if __name__ == "__main__":
    # Test the mapper with database connection
    logging.basicConfig(level=logging.INFO)
    
    print("Testing SPL Document Mapper...")
    
    try:
        mapper = SPLDocumentMapper()
        print("✅ SPL Document Mapper initialized successfully!")
        
        # Test database connection
        if mapper.db.test_connection():
            print("✅ Database connection test successful!")
        else:
            print("❌ Database connection test failed!")
            
    except Exception as e:
        print(f"❌ SPL Document Mapper initialization failed: {str(e)}")