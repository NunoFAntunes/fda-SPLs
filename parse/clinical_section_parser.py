"""
Clinical Section Processing Parser.
Handles text-heavy clinical sections like warnings, indications, and dosage instructions.
"""

import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any
import re
from html import unescape

from base_parser import BaseParser, XMLUtils, TextExtractor
from models import SPLSection, MediaReference, SectionType


class ClinicalSectionParser(BaseParser):
    """Parser for clinical text sections in SPL documents."""
    
    def __init__(self):
        super().__init__()
        self.media_counter = 0
    
    def parse(self, source):
        """Implementation of abstract parse method - not used for clinical sections."""
        return None
    
    def parse_clinical_section(self, section_element: ET.Element, section: SPLSection) -> SPLSection:
        """
        Enhance an existing SPLSection with clinical-specific parsing.
        
        Args:
            section_element: XML section element
            section: Pre-parsed SPLSection object to enhance
            
        Returns:
            SPLSection: Enhanced section with clinical content
        """
        # Parse text content with clinical-specific processing
        text_element = XMLUtils.find_element(section_element, "hl7:text")
        if text_element is not None:
            section.text_content = self._extract_clinical_text(text_element, section.section_type)
            section.media_references = self._extract_media_references(text_element)
        
        # Parse observational media components
        media_components = self._extract_observation_media(section_element)
        section.media_references.extend(media_components)
        
        return section
    
    def _extract_clinical_text(self, text_element: ET.Element, section_type: Optional[SectionType]) -> str:
        """Extract and process clinical text with section-specific formatting."""
        # Use base text extractor
        raw_text = TextExtractor.extract_section_text(text_element)
        
        # Apply section-specific processing
        if section_type:
            processed_text = self._process_by_section_type(raw_text, section_type)
        else:
            processed_text = raw_text
        
        # Apply general clinical text cleaning
        return self._clean_clinical_text(processed_text)
    
    def _process_by_section_type(self, text: str, section_type: SectionType) -> str:
        """Apply section-type specific text processing."""
        processors = {
            SectionType.WARNINGS: self._process_warnings_text,
            SectionType.INDICATIONS_USAGE: self._process_indications_text,
            SectionType.ACTIVE_INGREDIENT: self._process_ingredients_text,
            SectionType.INACTIVE_INGREDIENT: self._process_ingredients_text,
            SectionType.DO_NOT_USE: self._process_contraindications_text,
            SectionType.ASK_DOCTOR: self._process_precautions_text,
            SectionType.WHEN_USING: self._process_usage_text,
            SectionType.STOP_USE: self._process_stop_use_text,
        }
        
        processor = processors.get(section_type, self._process_generic_text)
        return processor(text)
    
    def _process_warnings_text(self, text: str) -> str:
        """Process warnings section text."""
        # Ensure warning statements are clearly formatted
        text = self._emphasize_warning_keywords(text)
        text = self._format_warning_lists(text)
        return text
    
    def _process_indications_text(self, text: str) -> str:
        """Process indications and usage text."""
        # Standardize indication language
        text = self._normalize_indication_phrases(text)
        text = self._format_usage_instructions(text)
        return text
    
    def _process_ingredients_text(self, text: str) -> str:
        """Process active/inactive ingredients text."""
        # Clean up ingredient formatting
        text = self._format_ingredient_lists(text)
        text = self._standardize_ingredient_units(text)
        return text
    
    def _process_contraindications_text(self, text: str) -> str:
        """Process 'Do not use' contraindications text."""
        text = self._emphasize_contraindication_keywords(text)
        text = self._format_contraindication_lists(text)
        return text
    
    def _process_precautions_text(self, text: str) -> str:
        """Process 'Ask a doctor' precautions text."""
        text = self._format_precaution_conditions(text)
        return text
    
    def _process_usage_text(self, text: str) -> str:
        """Process 'When using this product' text."""
        text = self._format_usage_guidelines(text)
        return text
    
    def _process_stop_use_text(self, text: str) -> str:
        """Process 'Stop use and ask a doctor if' text."""
        text = self._emphasize_stop_conditions(text)
        return text
    
    def _process_generic_text(self, text: str) -> str:
        """Generic text processing for unspecified section types."""
        return text
    
    def _clean_clinical_text(self, text: str) -> str:
        """Apply general clinical text cleaning."""
        if not text:
            return ""
        
        # Unescape HTML entities
        text = unescape(text)
        
        # Normalize whitespace but preserve paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]*\n', '\n\n', text)
        
        # Clean up bullet point formatting
        text = re.sub(r'•\s*', '• ', text)
        text = re.sub(r'^\s*•\s*', '• ', text, flags=re.MULTILINE)
        
        # Standardize common abbreviations and terms
        text = self._standardize_medical_abbreviations(text)
        
        return text.strip()
    
    def _emphasize_warning_keywords(self, text: str) -> str:
        """Emphasize important warning keywords."""
        warning_keywords = [
            r'\bWARNING\b',
            r'\bDANGER\b', 
            r'\bCAUTION\b',
            r'\bBLACK BOX\b',
            r'\bCONTRAINDICATED\b',
            r'\bSERIOUS\b',
            r'\bSEVERE\b',
        ]
        
        for keyword in warning_keywords:
            text = re.sub(keyword, lambda m: m.group().upper(), text, flags=re.IGNORECASE)
        
        return text
    
    def _format_warning_lists(self, text: str) -> str:
        """Format warning lists for better readability."""
        # Convert numbered lists to bullet points for consistency
        text = re.sub(r'^\s*(\d+)\.', r'•', text, flags=re.MULTILINE)
        return text
    
    def _normalize_indication_phrases(self, text: str) -> str:
        """Normalize common indication phrases."""
        normalizations = {
            r'\bfor the treatment of\b': 'treats',
            r'\bis indicated for\b': 'is used to treat',
            r'\bis used in the treatment of\b': 'treats',
            r'\bmanagement of\b': 'treatment of',
        }
        
        for pattern, replacement in normalizations.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        return text
    
    def _format_usage_instructions(self, text: str) -> str:
        """Format usage instructions for clarity."""
        # Ensure dosage instructions are clearly separated
        text = re.sub(r'(\d+)\s*(tablet|capsule|dose)', r'\1 \2', text, flags=re.IGNORECASE)
        text = re.sub(r'(\d+)\s*(time|times)\s*(per|a)\s*(day|week)', r'\1 \2 per \4', text, flags=re.IGNORECASE)
        return text
    
    def _format_ingredient_lists(self, text: str) -> str:
        """Format ingredient lists for consistency."""
        # Standardize ingredient list formatting
        text = re.sub(r'([A-Z][A-Za-z\s]+)(\d+\.?\d*\s*(?:mg|g|mcg|%|units?))', r'\1 \2', text)
        return text
    
    def _standardize_ingredient_units(self, text: str) -> str:
        """Standardize ingredient units."""
        unit_standardizations = {
            r'\bmg\.\b': 'mg',
            r'\bg\.\b': 'g',
            r'\bmcg\.\b': 'mcg',
            r'\bIU\.\b': 'IU',
            r'\bunits?\.\b': 'units',
        }
        
        for pattern, replacement in unit_standardizations.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        return text
    
    def _emphasize_contraindication_keywords(self, text: str) -> str:
        """Emphasize contraindication keywords."""
        keywords = [
            r'\bDo not use\b',
            r'\bNever use\b',
            r'\bAvoid\b',
            r'\bContraindicated\b',
        ]
        
        for keyword in keywords:
            text = re.sub(keyword, lambda m: m.group().upper(), text, flags=re.IGNORECASE)
        
        return text
    
    def _format_contraindication_lists(self, text: str) -> str:
        """Format contraindication lists."""
        # Ensure 'if you' conditions are clearly formatted
        text = re.sub(r'\bif you\b', '\nif you', text, flags=re.IGNORECASE)
        return text
    
    def _format_precaution_conditions(self, text: str) -> str:
        """Format precautionary conditions."""
        # Format conditional statements
        text = re.sub(r'\bif you have\b', '\n• if you have', text, flags=re.IGNORECASE)
        return text
    
    def _format_usage_guidelines(self, text: str) -> str:
        """Format usage guidelines."""
        # Format do/don't statements
        text = re.sub(r'\bdo not\b', '\n• do not', text, flags=re.IGNORECASE)
        return text
    
    def _emphasize_stop_conditions(self, text: str) -> str:
        """Emphasize stop-use conditions."""
        text = re.sub(r'\bstop use\b', 'STOP USE', text, flags=re.IGNORECASE)
        return text
    
    def _standardize_medical_abbreviations(self, text: str) -> str:
        """Standardize common medical abbreviations."""
        abbreviations = {
            r'\btid\b': 'three times daily',
            r'\bbid\b': 'twice daily', 
            r'\bqd\b': 'once daily',
            r'\bprn\b': 'as needed',
            r'\bpo\b': 'by mouth',
            r'\bIV\b': 'intravenous',
            r'\bIM\b': 'intramuscular',
            r'\bSC\b': 'subcutaneous',
            r'\bSQ\b': 'subcutaneous',
        }
        
        for abbr, expansion in abbreviations.items():
            text = re.sub(abbr, expansion, text, flags=re.IGNORECASE)
        
        return text
    
    def _extract_media_references(self, text_element: ET.Element) -> List[MediaReference]:
        """Extract media references from text content."""
        media_refs = []
        
        # Find renderMultiMedia elements
        render_media_elements = text_element.findall(".//renderMultiMedia", self.namespaces)
        
        for render_element in render_media_elements:
            referenced_object = XMLUtils.get_attribute(render_element, "referencedObject")
            if referenced_object:
                media_ref = MediaReference(
                    media_id=referenced_object,
                    media_type="reference",  # Will be updated when actual media is found
                    reference_value=referenced_object
                )
                media_refs.append(media_ref)
        
        return media_refs
    
    def _extract_observation_media(self, section_element: ET.Element) -> List[MediaReference]:
        """Extract observationMedia components from section."""
        media_refs = []
        
        # Find component/observationMedia elements
        components = XMLUtils.find_all_elements(section_element, "hl7:component")
        
        for component in components:
            obs_media = XMLUtils.find_element(component, "hl7:observationMedia")
            if obs_media is not None:
                media_ref = self._parse_observation_media(obs_media)
                if media_ref:
                    media_refs.append(media_ref)
        
        return media_refs
    
    def _parse_observation_media(self, obs_media_element: ET.Element) -> Optional[MediaReference]:
        """Parse an observationMedia element."""
        # Extract media ID
        media_id = XMLUtils.get_attribute(obs_media_element, "ID")
        if not media_id:
            self.media_counter += 1
            media_id = f"media_{self.media_counter}"
        
        # Extract description
        text_element = XMLUtils.find_element(obs_media_element, "hl7:text")
        description = XMLUtils.get_text_content(text_element) if text_element else None
        
        # Extract value element with media reference
        value_element = XMLUtils.find_element(obs_media_element, "hl7:value")
        if value_element is None:
            return None
        
        media_type = XMLUtils.get_attribute(value_element, "mediaType")
        
        # Extract reference
        reference_element = XMLUtils.find_element(value_element, "hl7:reference")
        reference_value = XMLUtils.get_attribute(reference_element, "value") if reference_element is not None else None
        
        if not reference_value:
            return None
        
        return MediaReference(
            media_id=media_id,
            media_type=media_type or "unknown",
            reference_value=reference_value,
            description=description
        )


class ClinicalTextAnalyzer:
    """Utility for analyzing clinical text content."""
    
    @staticmethod
    def extract_dosage_information(text: str) -> List[Dict[str, str]]:
        """Extract structured dosage information from text."""
        dosage_patterns = [
            r'(\d+(?:\.\d+)?)\s*(tablet|capsule|dose)s?\s*(?:(\d+)\s*times?\s*(?:per|a)\s*(day|week|month))?',
            r'(\d+(?:\.\d+)?)\s*(mg|g|mcg|mL)\s*(?:(\d+)\s*times?\s*(?:per|a)\s*(day|week|month))?',
            r'(?:take|use)\s*(\d+(?:\.\d+)?)\s*(tablet|capsule|dose)s?\s*(?:(\d+)\s*times?\s*(?:per|a)\s*(day|week|month))?',
        ]
        
        dosages = []
        for pattern in dosage_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                dosage_info = {
                    'amount': match.group(1),
                    'unit': match.group(2),
                    'frequency': match.group(3) if len(match.groups()) > 2 else None,
                    'frequency_unit': match.group(4) if len(match.groups()) > 3 else None,
                    'full_match': match.group(0)
                }
                dosages.append(dosage_info)
        
        return dosages
    
    @staticmethod
    def extract_warnings_list(text: str) -> List[str]:
        """Extract individual warning items from text."""
        warnings = []
        
        # Split on bullet points or numbered lists
        items = re.split(r'(?:\n|^)\s*(?:•|\d+\.|\*)\s*', text)
        
        for item in items:
            item = item.strip()
            if item and len(item) > 10:  # Filter out very short items
                warnings.append(item)
        
        return warnings
    
    @staticmethod
    def identify_contraindications(text: str) -> List[str]:
        """Identify contraindication conditions from text."""
        contraindication_patterns = [
            r'do not use[^.]*if[^.]*',
            r'contraindicated[^.]*in[^.]*',
            r'should not be used[^.]*',
            r'avoid[^.]*if[^.]*',
        ]
        
        contraindications = []
        for pattern in contraindication_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                contraindications.append(match.group().strip())
        
        return contraindications
    
    @staticmethod
    def calculate_reading_level(text: str) -> Dict[str, float]:
        """Calculate basic reading level metrics for clinical text."""
        if not text or not text.strip():
            return {'flesch_kincaid': 0.0, 'avg_sentence_length': 0.0, 'avg_word_length': 0.0}
        
        # Basic text statistics
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        words = re.findall(r'\b\w+\b', text)
        syllables = sum(ClinicalTextAnalyzer._count_syllables(word) for word in words)
        
        if not sentences or not words:
            return {'flesch_kincaid': 0.0, 'avg_sentence_length': 0.0, 'avg_word_length': 0.0}
        
        avg_sentence_length = len(words) / len(sentences)
        avg_syllables_per_word = syllables / len(words)
        avg_word_length = sum(len(word) for word in words) / len(words)
        
        # Simplified Flesch-Kincaid Grade Level
        flesch_kincaid = (0.39 * avg_sentence_length) + (11.8 * avg_syllables_per_word) - 15.59
        
        return {
            'flesch_kincaid': max(0.0, flesch_kincaid),
            'avg_sentence_length': avg_sentence_length,
            'avg_word_length': avg_word_length
        }
    
    @staticmethod
    def _count_syllables(word: str) -> int:
        """Estimate syllable count for a word."""
        if not word:
            return 0
        
        word = word.lower()
        vowels = 'aeiouy'
        syllable_count = 0
        prev_was_vowel = False
        
        for char in word:
            if char in vowels:
                if not prev_was_vowel:
                    syllable_count += 1
                prev_was_vowel = True
            else:
                prev_was_vowel = False
        
        # Handle silent 'e' at the end
        if word.endswith('e') and syllable_count > 1:
            syllable_count -= 1
        
        return max(1, syllable_count)