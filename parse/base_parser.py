"""
Base parser classes and utilities for SPL document parsing.
Provides foundation classes for XML parsing and data extraction.
"""

import xml.etree.ElementTree as ET
from typing import Optional, Dict, List, Any, Union
from abc import ABC, abstractmethod
import logging
from datetime import datetime

from models import (
    SPLDocument, SPLSection, CodedConcept, Organization, 
    DocumentAuthor, SectionType, IngredientType
)


class ParseError(Exception):
    """Custom exception for parsing errors."""
    pass


class SPLNamespaces:
    """XML namespace constants for SPL documents."""
    HL7_V3 = "urn:hl7-org:v3"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"
    
    @classmethod
    def get_namespaces(cls) -> Dict[str, str]:
        """Return namespace dictionary for XML parsing."""
        return {
            "hl7": cls.HL7_V3,
            "xsi": cls.XSI
        }
    
    @classmethod
    def get_namespaces_with_default(cls) -> Dict[str, str]:
        """Return namespace dictionary including default namespace for XML parsing."""
        return {
            "hl7": cls.HL7_V3,
            "xsi": cls.XSI,
            "": cls.HL7_V3  # Default namespace
        }


class XMLUtils:
    """Utility functions for XML parsing."""
    
    @staticmethod
    def find_element(parent: ET.Element, tag: str, namespaces: Optional[Dict[str, str]] = None) -> Optional[ET.Element]:
        """Find first child element with given tag, handling both prefixed and default namespace elements."""
        if namespaces is None:
            namespaces = SPLNamespaces.get_namespaces_with_default()
        
        # If tag has hl7: prefix, try with explicit namespace
        if tag.startswith("hl7:"):
            unprefixed_tag = tag[4:]  # Remove "hl7:" prefix
            # Try with explicit namespace first
            result = parent.find(f"{{{SPLNamespaces.HL7_V3}}}{unprefixed_tag}")
            if result is not None:
                return result
            
            # Fallback: try with namespace prefix
            result = parent.find(tag, namespaces)
            if result is not None:
                return result
        else:
            # For non-prefixed tags, try as-is first
            result = parent.find(tag, namespaces)
            if result is not None:
                return result
        
        return None
    
    @staticmethod
    def find_all_elements(parent: ET.Element, tag: str, namespaces: Optional[Dict[str, str]] = None) -> List[ET.Element]:
        """Find all child elements with given tag, handling both prefixed and default namespace elements."""
        if namespaces is None:
            namespaces = SPLNamespaces.get_namespaces_with_default()
        
        # If tag has hl7: prefix, try with explicit namespace
        if tag.startswith("hl7:"):
            unprefixed_tag = tag[4:]  # Remove "hl7:" prefix
            # Try with explicit namespace first
            results = parent.findall(f"{{{SPLNamespaces.HL7_V3}}}{unprefixed_tag}")
            if results:
                return results
            
            # Fallback: try with namespace prefix
            results = parent.findall(tag, namespaces)
            return results
        else:
            # For non-prefixed tags, try as-is
            return parent.findall(tag, namespaces)
    
    @staticmethod
    def get_attribute(element: ET.Element, attr_name: str) -> Optional[str]:
        """Get attribute value from element."""
        if element is None:
            return None
        
        try:
            result = element.get(attr_name)
            # Only log if it's a section code attribute to avoid too much noise
            # if attr_name == "code" and result:
            #     print(f"[DEBUG] get_attribute('{attr_name}') = '{result}'")
            return result
        except Exception as e:
            print(f"[DEBUG] get_attribute exception: {e}")
            return None
    
    @staticmethod
    def get_text_content(element: ET.Element) -> Optional[str]:
        """Extract text content from element, handling nested elements."""
        if element.text:
            return element.text.strip()
        return None
    
    @staticmethod
    def parse_coded_concept(element: ET.Element) -> Optional[CodedConcept]:
        """Parse a coded concept from XML element."""
        if element is None:
            print(f"[DEBUG] parse_coded_concept: element is None")
            return None
        
        code = XMLUtils.get_attribute(element, "code")
        code_system = XMLUtils.get_attribute(element, "codeSystem") 
        display_name = XMLUtils.get_attribute(element, "displayName")
        
        # print(f"[DEBUG] parse_coded_concept: code='{code}', system='{code_system}', display='{display_name}'")
        
        if code and code_system:
            return CodedConcept(
                code=code,
                code_system=code_system,
                display_name=display_name
            )
        return None


class BaseParser(ABC):
    """Base class for SPL parsers."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.namespaces = SPLNamespaces.get_namespaces()
        self.errors: List[str] = []
    
    @abstractmethod
    def parse(self, source: Union[str, ET.Element]) -> Any:
        """Parse the source and return parsed data."""
        pass
    
    def add_error(self, error_msg: str) -> None:
        """Add an error message to the error list."""
        self.errors.append(error_msg)
        self.logger.warning(f"Parser error: {error_msg}")
    
    def clear_errors(self) -> None:
        """Clear the error list."""
        self.errors.clear()


class OrganizationParser(BaseParser):
    """Parser for organization elements."""
    
    def parse(self, org_element: ET.Element) -> Organization:
        """Parse organization from XML element."""
        id_element = XMLUtils.find_element(org_element, "hl7:id")
        name_element = XMLUtils.find_element(org_element, "hl7:name")
        
        organization = Organization()
        
        if id_element is not None:
            organization.id_extension = XMLUtils.get_attribute(id_element, "extension")
            organization.id_root = XMLUtils.get_attribute(id_element, "root")
        
        if name_element is not None:
            organization.name = XMLUtils.get_text_content(name_element)
        
        return organization


class DocumentAuthorParser(BaseParser):
    """Parser for document author information."""
    
    def parse(self, author_element: ET.Element) -> DocumentAuthor:
        """Parse document author from XML element."""
        author = DocumentAuthor()
        
        # Parse time
        time_element = XMLUtils.find_element(author_element, "hl7:time")
        if time_element is not None:
            author.time = XMLUtils.get_attribute(time_element, "value")
        
        # Parse organizations
        org_parser = OrganizationParser()
        assigned_entity = XMLUtils.find_element(author_element, "hl7:assignedEntity")
        
        if assigned_entity is not None:
            # Parse all organization levels (can be nested)
            author.organizations = self._parse_nested_organizations(assigned_entity, org_parser)
        
        return author
    
    def _parse_nested_organizations(self, element: ET.Element, org_parser: OrganizationParser) -> List[Organization]:
        """Parse nested organization structures."""
        organizations = []
        
        # Look for representedOrganization
        repr_org = XMLUtils.find_element(element, "hl7:representedOrganization")
        if repr_org is not None:
            organizations.append(org_parser.parse(repr_org))
            
            # Look for nested assignedEntity elements
            nested_entities = XMLUtils.find_all_elements(repr_org, "hl7:assignedEntity")
            for nested_entity in nested_entities:
                nested_orgs = XMLUtils.find_all_elements(nested_entity, "hl7:assignedOrganization")
                for nested_org in nested_orgs:
                    organizations.append(org_parser.parse(nested_org))
        
        return organizations


class SectionTypeMapper:
    """Maps LOINC codes to section types."""
    
    CODE_TO_TYPE = {
        "48780-1": SectionType.SPL_LISTING,
        "55106-9": SectionType.ACTIVE_INGREDIENT,
        "55105-1": SectionType.PURPOSE,
        "34071-1": SectionType.WARNINGS,
        "50570-1": SectionType.DO_NOT_USE,
        "50569-3": SectionType.ASK_DOCTOR,
        "50567-7": SectionType.WHEN_USING,
        "50566-9": SectionType.STOP_USE,
        "53414-9": SectionType.PREGNANCY_BREASTFEEDING,
        "50565-1": SectionType.KEEP_OUT_OF_REACH,
        "34067-9": SectionType.INDICATIONS_USAGE,
        "51727-6": SectionType.INACTIVE_INGREDIENT,
        "42229-5": SectionType.UNCLASSIFIED,
    }
    
    @classmethod
    def get_section_type(cls, loinc_code: str) -> Optional[SectionType]:
        """Get section type from LOINC code."""
        return cls.CODE_TO_TYPE.get(loinc_code)


class TextExtractor:
    """Utility for extracting and cleaning text from SPL sections."""
    
    @staticmethod
    def extract_section_text(text_element: ET.Element) -> str:
        """
        Extract and clean text content from SPL text elements.
        Handles nested content, lists, and formatting elements.
        """
        if text_element is None:
            return ""
        
        text_parts = []
        TextExtractor._extract_text_recursive(text_element, text_parts)
        
        # Join and clean up the text
        full_text = " ".join(text_parts)
        return TextExtractor._clean_text(full_text)
    
    @staticmethod
    def _extract_text_recursive(element: ET.Element, text_parts: List[str]) -> None:
        """Recursively extract text from nested elements."""
        # Add element's direct text
        if element.text and element.text.strip():
            text_parts.append(element.text.strip())
        
        # Process child elements
        for child in element:
            if child.tag.endswith("br"):
                text_parts.append("\n")
            elif child.tag.endswith("list"):
                TextExtractor._extract_list_text(child, text_parts)
            else:
                TextExtractor._extract_text_recursive(child, text_parts)
            
            # Add tail text
            if child.tail and child.tail.strip():
                text_parts.append(child.tail.strip())
    
    @staticmethod
    def _extract_list_text(list_element: ET.Element, text_parts: List[str]) -> None:
        """Extract text from list elements."""
        items = XMLUtils.find_all_elements(list_element, "hl7:item")
        for item in items:
            text_parts.append("• ")
            TextExtractor._extract_text_recursive(item, text_parts)
            text_parts.append("\n")
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize extracted text."""
        # Replace multiple whitespace with single space
        import re
        text = re.sub(r'\s+', ' ', text)
        # Clean up line breaks
        text = re.sub(r'\n\s*\n', '\n', text)
        return text.strip()