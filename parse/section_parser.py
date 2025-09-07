"""
Section Parser - Enhanced section parsing with routing to specialized parsers.
Coordinates section discovery, type identification, and routing.
"""

import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Type
from enum import Enum

from base_parser import BaseParser, XMLUtils, SectionTypeMapper
from models import SPLSection, SectionType, ManufacturedProduct
from clinical_section_parser import ClinicalSectionParser
from product_parser import ProductParser
from ingredient_parser import IngredientParser


class ParsingStrategy(Enum):
    """Different parsing strategies for different section types."""
    CLINICAL_TEXT = "clinical_text"
    PRODUCT_LISTING = "product_listing"
    INGREDIENT_FOCUS = "ingredient_focus"
    GENERIC = "generic"


class SectionParser(BaseParser):
    """
    Enhanced section parser that routes sections to appropriate specialized parsers.
    """
    
    def parse(self, source):
        """Implementation of abstract parse method - delegates to parse_section_enhanced."""
        return self.parse_section_enhanced(source)
    
    def __init__(self):
        super().__init__()
        self.clinical_parser = ClinicalSectionParser()
        self.product_parser = ProductParser()
        self.ingredient_parser = IngredientParser()
        self.strategy_mapping = self._initialize_strategy_mapping()
    
    def parse_section_enhanced(self, section_element: ET.Element) -> Optional[SPLSection]:
        """
        Parse a section with enhanced processing based on section type.
        
        Args:
            section_element: XML section element
            
        Returns:
            SPLSection: Fully parsed and enhanced section
        """
        # Start with basic section parsing
        section = self._parse_basic_section(section_element)
        if not section:
            return None
        
        # Determine parsing strategy
        strategy = self._get_parsing_strategy(section.section_type)
        # print(f"[DEBUG] Section {section.section_id} type: {section.section_type}, strategy: {strategy}")
        
        # Apply strategy-specific enhancements
        try:
            if strategy == ParsingStrategy.CLINICAL_TEXT:
                section = self._parse_clinical_section(section_element, section)
            elif strategy == ParsingStrategy.PRODUCT_LISTING:
                print(f"[DEBUG] Using PRODUCT_LISTING strategy for section {section.section_id}")
                section = self._parse_product_listing_section(section_element, section)
            elif strategy == ParsingStrategy.INGREDIENT_FOCUS:
                section = self._parse_ingredient_section(section_element, section)
            else:
                section = self._parse_generic_section(section_element, section)
            
            # Parse subsections recursively
            section.subsections = self._parse_subsections(section_element)
            
        except Exception as e:
            self.add_error(f"Failed to enhance section {section.section_id}: {str(e)}")
        
        return section
    
    def _parse_basic_section(self, section_element: ET.Element) -> Optional[SPLSection]:
        """Parse basic section information."""
        # Extract section ID
        id_element = XMLUtils.find_element(section_element, "hl7:id")
        section_id = XMLUtils.get_attribute(id_element, "root") if id_element is not None else ""
        
        if not section_id:
            self.add_error("Section missing required ID")
            return None
        
        # print(f"[DEBUG] Parsing basic section: {section_id}")
        
        # Extract section code and determine type
        code_element = XMLUtils.find_element(section_element, "hl7:code")
        # print(f"[DEBUG] Found code element: {code_element is not None}")
        
        section_code = None
        section_type = None
        
        if code_element is not None:
            try:
                # Use direct element access to bypass potential XMLUtils issues
                code_value = code_element.get("code")
                code_system = code_element.get("codeSystem")
                display_name = code_element.get("displayName")
                
                #print(f"[DEBUG] Direct element access - code: '{code_value}', system: '{code_system}'")
                
                if code_value and code_system:
                    from models import CodedConcept
                    section_code = CodedConcept(
                        code=code_value,
                        code_system=code_system,
                        display_name=display_name
                    )
                    
                    section_type = SectionTypeMapper.get_section_type(code_value)
                    # print(f"[DEBUG] Section code: {code_value}, mapped to type: {section_type}")
                
            except Exception as e:
                print(f"[DEBUG] Exception parsing section code: {e}")
        
        # print(f"[DEBUG] Final parsed section code: {section_code}")
        
        # Extract basic metadata
        title_element = XMLUtils.find_element(section_element, "hl7:title")
        title = XMLUtils.get_text_content(title_element) if title_element else None
        
        effective_time_element = XMLUtils.find_element(section_element, "hl7:effectiveTime")
        effective_time = XMLUtils.get_attribute(effective_time_element, "value") if effective_time_element is not None else None
        
        return SPLSection(
            section_id=section_id,
            section_code=section_code,
            section_type=section_type,
            title=title,
            effective_time=effective_time
        )
    
    def _get_parsing_strategy(self, section_type: Optional[SectionType]) -> ParsingStrategy:
        """Determine parsing strategy based on section type."""
        if section_type is None:
            return ParsingStrategy.GENERIC
        
        return self.strategy_mapping.get(section_type, ParsingStrategy.GENERIC)
    
    def _parse_clinical_section(self, section_element: ET.Element, section: SPLSection) -> SPLSection:
        """Parse clinical text sections with specialized processing."""
        return self.clinical_parser.parse_clinical_section(section_element, section)
    
    def _parse_product_listing_section(self, section_element: ET.Element, section: SPLSection) -> SPLSection:
        """Parse SPL listing sections containing product information."""
        # print(f"[DEBUG] Parsing SPL listing section: {section.section_id}")
        
        # Look for subject elements containing manufactured products
        subject_elements = XMLUtils.find_all_elements(section_element, "hl7:subject")
        # print(f"[DEBUG] Found {len(subject_elements)} subject elements in SPL listing section")
        
        for i, subject_element in enumerate(subject_elements):
            # print(f"[DEBUG] Processing subject {i+1}")
            try:
                manufactured_product = self.product_parser.parse(subject_element)
                if manufactured_product:
                    # print(f"[DEBUG] Successfully parsed manufactured product: {manufactured_product.product_name}")
                    
                    # Parse ingredients for this product
                    manufactured_product.ingredients = self._parse_product_ingredients(subject_element)
                    # print(f"[DEBUG] Found {len(manufactured_product.ingredients)} ingredients")
                    
                    section.manufactured_product = manufactured_product
                    # print(f"[DEBUG] Attached manufactured product to section")
                    break  # Usually only one product per section
                else:
                    print(f"[DEBUG] Product parser returned None")
                    if self.product_parser.errors:
                        print(f"[DEBUG] Product parser errors: {self.product_parser.errors}")
            except Exception as e:
                print(f"[DEBUG] Exception parsing manufactured product: {str(e)}")
                self.add_error(f"Failed to parse manufactured product: {str(e)}")
        
        # Also parse any clinical text content
        text_element = XMLUtils.find_element(section_element, "hl7:text")
        if text_element is not None:
            from base_parser import TextExtractor
            section.text_content = TextExtractor.extract_section_text(text_element)
        
        return section
    
    def _parse_ingredient_section(self, section_element: ET.Element, section: SPLSection) -> SPLSection:
        """Parse sections focused on ingredient information."""
        # Parse clinical text first
        section = self.clinical_parser.parse_clinical_section(section_element, section)
        
        # Look for any product information that might contain ingredients
        subject_elements = XMLUtils.find_all_elements(section_element, "hl7:subject")
        for subject_element in subject_elements:
            manufactured_product = self.product_parser.parse(subject_element)
            if manufactured_product:
                manufactured_product.ingredients = self._parse_product_ingredients(subject_element)
                section.manufactured_product = manufactured_product
                break
        
        return section
    
    def _parse_generic_section(self, section_element: ET.Element, section: SPLSection) -> SPLSection:
        """Parse generic sections with basic text extraction."""
        text_element = XMLUtils.find_element(section_element, "hl7:text")
        if text_element is not None:
            from base_parser import TextExtractor
            section.text_content = TextExtractor.extract_section_text(text_element)
        
        return section
    
    def _parse_product_ingredients(self, subject_element: ET.Element) -> List:
        """Parse ingredients from a subject element containing manufactured product."""
        manufactured_product_element = XMLUtils.find_element(subject_element, "hl7:manufacturedProduct")
        if manufactured_product_element is None:
            return []
        
        inner_product = XMLUtils.find_element(manufactured_product_element, "hl7:manufacturedProduct")
        if inner_product is None:
            return []
        
        return self.ingredient_parser.parse_ingredients(inner_product)
    
    def _parse_subsections(self, section_element: ET.Element) -> List[SPLSection]:
        """Parse subsections recursively."""
        subsections = []
        
        component_elements = XMLUtils.find_all_elements(section_element, "hl7:component")
        for component in component_elements:
            subsection_element = XMLUtils.find_element(component, "hl7:section")
            if subsection_element is not None:
                subsection = self.parse_section_enhanced(subsection_element)
                if subsection:
                    subsections.append(subsection)
        
        return subsections
    
    def _initialize_strategy_mapping(self) -> Dict[SectionType, ParsingStrategy]:
        """Initialize mapping of section types to parsing strategies."""
        return {
            # Clinical text sections
            SectionType.WARNINGS: ParsingStrategy.CLINICAL_TEXT,
            SectionType.INDICATIONS_USAGE: ParsingStrategy.CLINICAL_TEXT,
            SectionType.DO_NOT_USE: ParsingStrategy.CLINICAL_TEXT,
            SectionType.ASK_DOCTOR: ParsingStrategy.CLINICAL_TEXT,
            SectionType.WHEN_USING: ParsingStrategy.CLINICAL_TEXT,
            SectionType.STOP_USE: ParsingStrategy.CLINICAL_TEXT,
            SectionType.PREGNANCY_BREASTFEEDING: ParsingStrategy.CLINICAL_TEXT,
            SectionType.KEEP_OUT_OF_REACH: ParsingStrategy.CLINICAL_TEXT,
            SectionType.UNCLASSIFIED: ParsingStrategy.CLINICAL_TEXT,
            
            # Product listing sections
            SectionType.SPL_LISTING: ParsingStrategy.PRODUCT_LISTING,
            
            # Ingredient-focused sections
            SectionType.ACTIVE_INGREDIENT: ParsingStrategy.INGREDIENT_FOCUS,
            SectionType.INACTIVE_INGREDIENT: ParsingStrategy.INGREDIENT_FOCUS,
            SectionType.PURPOSE: ParsingStrategy.INGREDIENT_FOCUS,
        }


class SectionAnalyzer:
    """Utility for analyzing parsed sections and providing insights."""
    
    @staticmethod
    def analyze_section_distribution(sections: List[SPLSection]) -> Dict[str, any]:
        """Analyze the distribution of section types in a document."""
        type_counts = {}
        total_sections = len(sections)
        text_sections = 0
        product_sections = 0
        
        for section in sections:
            # Count by section type
            section_type_name = section.section_type.name if section.section_type else "UNKNOWN"
            type_counts[section_type_name] = type_counts.get(section_type_name, 0) + 1
            
            # Count sections with text vs product data
            if section.text_content:
                text_sections += 1
            if section.manufactured_product:
                product_sections += 1
        
        return {
            'total_sections': total_sections,
            'section_type_distribution': type_counts,
            'text_sections': text_sections,
            'product_sections': product_sections,
            'subsection_count': sum(len(section.subsections) for section in sections),
            'media_references': sum(len(section.media_references) for section in sections)
        }
    
    @staticmethod
    def get_section_completeness_score(section: SPLSection) -> float:
        """Calculate a completeness score for a section (0-1)."""
        score = 0.0
        total_criteria = 6
        
        # Basic metadata
        if section.section_id:
            score += 1
        if section.section_code:
            score += 1
        if section.section_type:
            score += 1
        
        # Content
        if section.text_content and len(section.text_content.strip()) > 10:
            score += 1
        if section.manufactured_product:
            score += 1
        if section.title:
            score += 1
        
        return score / total_criteria
    
    @staticmethod
    def identify_missing_sections(sections: List[SPLSection], document_type: str = "OTC") -> List[SectionType]:
        """Identify commonly expected sections that are missing."""
        existing_types = {section.section_type for section in sections if section.section_type}
        
        if document_type.upper() == "OTC":
            expected_sections = {
                SectionType.ACTIVE_INGREDIENT,
                SectionType.PURPOSE,
                SectionType.WARNINGS,
                SectionType.DO_NOT_USE,
                SectionType.INDICATIONS_USAGE,
                SectionType.INACTIVE_INGREDIENT,
            }
        else:
            expected_sections = {
                SectionType.INDICATIONS_USAGE,
                SectionType.WARNINGS,
                SectionType.SPL_LISTING,
            }
        
        return list(expected_sections - existing_types)
    
    @staticmethod
    def extract_section_keywords(section: SPLSection) -> List[str]:
        """Extract key medical/pharmaceutical terms from section content."""
        if not section.text_content:
            return []
        
        # Common pharmaceutical keywords to look for
        keyword_patterns = [
            r'\b(?:tablet|capsule|dose|dosage|mg|g|mcg|mL|%)\b',
            r'\b(?:daily|twice|once|morning|evening|bedtime)\b',
            r'\b(?:treat|treatment|prevent|relief|symptom)\b',
            r'\b(?:side effect|adverse|reaction|allergy)\b',
            r'\b(?:doctor|physician|pharmacist|healthcare)\b',
        ]
        
        import re
        keywords = []
        text = section.text_content.lower()
        
        for pattern in keyword_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            keywords.extend(matches)
        
        # Remove duplicates and return sorted
        return sorted(list(set(keywords)))
    
    @staticmethod
    def calculate_section_metrics(sections: List[SPLSection]) -> Dict[str, any]:
        """Calculate various metrics for a collection of sections."""
        if not sections:
            return {}
        
        text_lengths = []
        completeness_scores = []
        
        for section in sections:
            if section.text_content:
                text_lengths.append(len(section.text_content))
            completeness_scores.append(SectionAnalyzer.get_section_completeness_score(section))
        
        return {
            'avg_text_length': sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            'max_text_length': max(text_lengths) if text_lengths else 0,
            'min_text_length': min(text_lengths) if text_lengths else 0,
            'avg_completeness': sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0,
            'sections_with_products': sum(1 for s in sections if s.manufactured_product),
            'sections_with_media': sum(1 for s in sections if s.media_references),
            'total_subsections': sum(len(s.subsections) for s in sections)
        }