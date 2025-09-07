"""
Core SPL Document Parser - Main orchestrator for SPL XML parsing.
Extracts document metadata and coordinates specialized parsers.
"""

import xml.etree.ElementTree as ET
from typing import Optional, List, Union
from datetime import datetime
import logging
import importlib
import json
from dataclasses import asdict

from .base_parser import BaseParser, XMLUtils, DocumentAuthorParser, ParseError
from .models import SPLDocument, CodedConcept, SPLSection, SectionType
from .validators import SPLDocumentValidator
from .section_parser import SectionParser


class SPLDocumentParser(BaseParser):
    """
    Main parser for complete SPL documents.
    Orchestrates extraction of document metadata and coordinates section parsing.
    """
    
    def __init__(self):
        super().__init__()
        self.author_parser = DocumentAuthorParser()
        self.validator = SPLDocumentValidator()
        self.section_parser = SectionParser()
    
    def parse(self, source: Union[str, ET.Element]) -> SPLDocument:
        """
        Parse an SPL document from XML string or Element.
        
        Args:
            source: XML string or ET.Element representing the SPL document
            
        Returns:
            SPLDocument: Parsed and validated SPL document
            
        Raises:
            ParseError: If parsing fails or validation errors are critical
        """
        self.clear_errors()
        
        try:
            # Parse XML if string provided
            if isinstance(source, str):
                root = ET.fromstring(source)
                raw_xml = source
            else:
                root = source
                raw_xml = ET.tostring(root, encoding='unicode')
            
            # Verify this is an SPL document
            if not self._is_spl_document(root):
                raise ParseError("Not a valid SPL document - missing HL7 v3 namespace or document structure")
            
            # Extract document metadata
            document = self._extract_document_metadata(root)
            #document.raw_xml = raw_xml
            document.processed_at = datetime.now()
            
            # Parse author information
            author_element = XMLUtils.find_element(root, "hl7:author")
            if author_element is not None:
                document.author = self.author_parser.parse(author_element)
            
            # Parse document sections
            document.sections = self._parse_document_sections(root)
            
            # Add any parsing errors to document
            document.processing_errors = [error for error in self.errors]
            
            # Validate the parsed document
            validation_result = self.validator.validate(document)
            if not validation_result.is_valid():
                self.logger.warning(f"Document validation failed with {len(validation_result.errors)} errors")
                document.processing_errors.extend([error.message for error in validation_result.errors])
            
            return document
            
        except ET.ParseError as e:
            error_msg = f"XML parsing error: {str(e)}"
            self.add_error(error_msg)
            raise ParseError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during SPL parsing: {str(e)}"
            self.add_error(error_msg)
            raise ParseError(error_msg) from e
    
    def _is_spl_document(self, root: ET.Element) -> bool:
        """Check if the XML root represents a valid SPL document."""
        # Check for HL7 v3 namespace
        if root.tag != "{urn:hl7-org:v3}document":
            return False
        
        # Check for required document elements
        required_elements = ["id", "code", "setId", "versionNumber"]
        for element_name in required_elements:
            if XMLUtils.find_element(root, f"hl7:{element_name}") is None:
                return False
        
        return True
    
    def _extract_document_metadata(self, root: ET.Element) -> SPLDocument:
        """Extract core document metadata from XML."""
        # Extract document ID
        id_element = XMLUtils.find_element(root, "hl7:id")
        document_id = XMLUtils.get_attribute(id_element, "root") if id_element is not None else ""
        
        if not document_id:
            self.add_error("Document ID is missing or empty")
            document_id = "unknown"
        
        # Extract set ID
        set_id_element = XMLUtils.find_element(root, "hl7:setId")
        set_id = XMLUtils.get_attribute(set_id_element, "root") if set_id_element is not None else ""
        
        if not set_id:
            self.add_error("Set ID is missing or empty")
            set_id = document_id  # Fallback to document ID
        
        # Extract version number
        version_element = XMLUtils.find_element(root, "hl7:versionNumber")
        version_number = XMLUtils.get_attribute(version_element, "value") if version_element is not None else ""
        
        if not version_number:
            self.add_error("Version number is missing or empty")
            version_number = "1"  # Default version
        
        # Extract effective time
        effective_time_element = XMLUtils.find_element(root, "hl7:effectiveTime")
        effective_time = XMLUtils.get_attribute(effective_time_element, "value") if effective_time_element is not None else None
        
        # Extract document code
        code_element = XMLUtils.find_element(root, "hl7:code")
        document_code = XMLUtils.parse_coded_concept(code_element) if code_element is not None else None
        
        # Create SPL document
        document = SPLDocument(
            document_id=document_id,
            set_id=set_id,
            version_number=version_number,
            effective_time=effective_time,
            document_code=document_code
        )
        
        return document
    
    def _parse_document_sections(self, root: ET.Element) -> List[SPLSection]:
        """Parse all sections from the document."""
        sections = []
        
        # Find the structured body
        component = XMLUtils.find_element(root, "hl7:component")
        if component is None:
            self.add_error("Document component is missing")
            return sections
        
        structured_body = XMLUtils.find_element(component, "hl7:structuredBody")
        if structured_body is None:
            self.add_error("Structured body is missing")
            return sections
        
        # Parse all section components
        section_components = XMLUtils.find_all_elements(structured_body, "hl7:component")
        
        for component in section_components:
            section_element = XMLUtils.find_element(component, "hl7:section")
            if section_element is not None:
                try:
                    section = self._parse_section(section_element)
                    if section:
                        sections.append(section)
                except Exception as e:
                    self.add_error(f"Failed to parse section: {str(e)}")
                    continue
        
        return sections
    
    def _parse_section(self, section_element: ET.Element) -> Optional[SPLSection]:
        """Parse a single section element using the enhanced section parser."""
        try:
            # Use the enhanced section parser for full parsing including products and ingredients
            return self.section_parser.parse(section_element)
        except Exception as e:
            self.add_error(f"Enhanced section parsing failed: {str(e)}")
            # Fallback to basic parsing if enhanced parsing fails
            return self._parse_basic_section(section_element)
    
    def _parse_basic_section(self, section_element: ET.Element) -> Optional[SPLSection]:
        """Parse a single section element with basic information only."""
        # Extract section ID
        id_element = XMLUtils.find_element(section_element, "hl7:id")
        section_id = XMLUtils.get_attribute(id_element, "root") if id_element is not None else ""
        
        if not section_id:
            self.add_error("Section missing required ID")
            return None
        
        # Extract section code
        code_element = XMLUtils.find_element(section_element, "hl7:code")
        section_code = XMLUtils.parse_coded_concept(code_element) if code_element is not None else None
        
        # Determine section type from LOINC code
        section_type = None
        if section_code and section_code.code:
            from base_parser import SectionTypeMapper
            section_type = SectionTypeMapper.get_section_type(section_code.code)
        
        # Extract title
        title_element = XMLUtils.find_element(section_element, "hl7:title")
        title = XMLUtils.get_text_content(title_element) if title_element is not None else None
        
        # Extract effective time
        effective_time_element = XMLUtils.find_element(section_element, "hl7:effectiveTime")
        effective_time = XMLUtils.get_attribute(effective_time_element, "value") if effective_time_element is not None else None
        
        # Extract text content
        text_element = XMLUtils.find_element(section_element, "hl7:text")
        text_content = None
        if text_element is not None:
            from base_parser import TextExtractor
            text_content = TextExtractor.extract_section_text(text_element)
        
        # Create section object
        section = SPLSection(
            section_id=section_id,
            section_code=section_code,
            section_type=section_type,
            title=title,
            text_content=text_content,
            effective_time=effective_time
        )
        
        # Parse subsections recursively
        subsection_components = XMLUtils.find_all_elements(section_element, "hl7:component")
        for sub_component in subsection_components:
            subsection_element = XMLUtils.find_element(sub_component, "hl7:section")
            if subsection_element is not None:
                subsection = self._parse_section(subsection_element)
                if subsection:
                    section.subsections.append(subsection)
        
        return section
    
    def parse_file(self, file_path: str) -> SPLDocument:
        """Parse an SPL document from a file path."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                xml_content = file.read()
            return self.parse(xml_content)
        except FileNotFoundError:
            raise ParseError(f"SPL file not found: {file_path}")
        except UnicodeDecodeError as e:
            raise ParseError(f"Failed to decode SPL file {file_path}: {str(e)}")
        except Exception as e:
            raise ParseError(f"Failed to parse SPL file {file_path}: {str(e)}")


class SPLParseResult:
    """Container for SPL parsing results with metadata."""
    
    def __init__(self, document: Optional[SPLDocument] = None, success: bool = True, 
                 errors: Optional[List[str]] = None, parse_time: Optional[float] = None):
        self.document = document
        self.success = success
        self.errors = errors or []
        self.parse_time = parse_time
        self.timestamp = datetime.now()
    
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        doc_id = self.document.document_id if self.document else "Unknown"
        return f"SPLParseResult({status}, doc_id={doc_id}, errors={len(self.errors)})"
    
    def to_json(self) -> str:
        """
        Convert the SPLParseResult to JSON string.
        
        Returns:
            str: JSON representation of the parse result
        """
        def datetime_serializer(obj):
            """Custom serializer for datetime and other objects."""
            if isinstance(obj, datetime):
                return obj.isoformat()
            # Handle dataclass instances
            if hasattr(obj, '__dataclass_fields__'):
                return asdict(obj)
            # Handle enum instances
            if hasattr(obj, 'value'):
                return obj.value
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        # Manually create dictionary since SPLParseResult is not a dataclass
        result_dict = {
            'document': self.document,
            'success': self.success,
            'errors': self.errors,
            'parse_time': self.parse_time,
            'timestamp': self.timestamp
        }
        
        # Convert to JSON string with custom serializer
        return json.dumps(result_dict, default=datetime_serializer, indent=2)


def parse_spl_document(source: Union[str, ET.Element]) -> SPLParseResult:
    """
    Convenience function to parse an SPL document and return results.
    
    Args:
        source: XML string or ET.Element
        
    Returns:
        SPLParseResult: Container with parsed document and metadata
    """
    parser = SPLDocumentParser()
    start_time = datetime.now()
    
    try:
        document = parser.parse(source)
        parse_time = (datetime.now() - start_time).total_seconds()
        
        return SPLParseResult(
            document=document,
            success=True,
            errors=parser.errors,
            parse_time=parse_time
        )
    except ParseError as e:
        parse_time = (datetime.now() - start_time).total_seconds()
        
        return SPLParseResult(
            document=None,
            success=False,
            errors=[str(e)] + parser.errors,
            parse_time=parse_time
        )


def parse_spl_file(file_path: str) -> SPLParseResult:
    """
    Convenience function to parse an SPL document from file.
    
    Args:
        file_path: Path to SPL XML file
        
    Returns:
        SPLParseResult: Container with parsed document and metadata
    """
    parser = SPLDocumentParser()
    start_time = datetime.now()
    
    try:
        document = parser.parse_file(file_path)
        parse_time = (datetime.now() - start_time).total_seconds()
        
        return SPLParseResult(
            document=document,
            success=True,
            errors=parser.errors,
            parse_time=parse_time
        )
    except ParseError as e:
        parse_time = (datetime.now() - start_time).total_seconds()
        
        return SPLParseResult(
            document=None,
            success=False,
            errors=[str(e)] + parser.errors,
            parse_time=parse_time
        )