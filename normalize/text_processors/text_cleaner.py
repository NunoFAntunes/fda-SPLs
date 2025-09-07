"""
Text cleaning utilities for SPL clinical text processing.
Removes XML markup and normalizes whitespace while preserving semantic structure.
"""

import re
from html import unescape
from typing import Optional


class TextCleaner:
    """Cleans and normalizes clinical text from SPL sections."""
    
    # XML/HTML tag removal patterns
    XML_TAG_PATTERN = re.compile(r'<[^>]+>')
    ENTITY_PATTERN = re.compile(r'&[a-zA-Z][a-zA-Z0-9]*;')
    
    # Whitespace normalization patterns
    MULTIPLE_SPACES = re.compile(r'\s+')
    MULTIPLE_NEWLINES = re.compile(r'\n\s*\n')
    LEADING_TRAILING_WS = re.compile(r'^\s+|\s+$')
    
    # Preserve semantic formatting patterns
    BULLET_PATTERNS = [
        re.compile(r'^\s*[•·∙▪▫◦‣⁃]\s*', re.MULTILINE),  # Unicode bullets
        re.compile(r'^\s*[-*+]\s*', re.MULTILINE),        # ASCII bullets
        re.compile(r'^\s*\d+\.\s*', re.MULTILINE),        # Numbered lists
        re.compile(r'^\s*[a-zA-Z]\.\s*', re.MULTILINE),   # Lettered lists
    ]
    
    @classmethod
    def clean_clinical_text(cls, text: str) -> str:
        """
        Clean clinical text while preserving semantic structure.
        
        Args:
            text: Raw clinical text potentially containing XML markup
            
        Returns:
            str: Cleaned and normalized text
        """
        if not text or not text.strip():
            return ""
        
        # Step 1: Remove XML/HTML tags but preserve content
        cleaned = cls._remove_xml_tags(text)
        
        # Step 2: Handle HTML entities
        cleaned = cls._decode_entities(cleaned)
        
        # Step 3: Normalize whitespace while preserving structure
        cleaned = cls._normalize_whitespace(cleaned)
        
        # Step 4: Clean up but preserve list formatting
        cleaned = cls._preserve_list_formatting(cleaned)
        
        return cleaned.strip()
    
    @classmethod
    def _remove_xml_tags(cls, text: str) -> str:
        """Remove XML/HTML tags while preserving text content."""
        # Handle common structural tags by adding spacing
        text = re.sub(r'</?(div|p|br|li|ul|ol)[^>]*>', ' ', text, flags=re.IGNORECASE)
        
        # Remove all other XML/HTML tags
        text = cls.XML_TAG_PATTERN.sub('', text)
        
        return text
    
    @classmethod
    def _decode_entities(cls, text: str) -> str:
        """Decode HTML entities to their Unicode equivalents."""
        # First handle common medical entities manually for better control
        common_entities = {
            '&nbsp;': ' ',
            '&lt;': '<',
            '&gt;': '>',
            '&amp;': '&',
            '&quot;': '"',
            '&apos;': "'",
            '&deg;': '°',
            '&micro;': 'μ',
            '&plusmn;': '±',
        }
        
        for entity, replacement in common_entities.items():
            text = text.replace(entity, replacement)
        
        # Handle remaining entities with html.unescape
        try:
            text = unescape(text)
        except Exception:
            # If unescape fails, just remove remaining entities
            text = cls.ENTITY_PATTERN.sub('', text)
        
        return text
    
    @classmethod
    def _normalize_whitespace(cls, text: str) -> str:
        """Normalize whitespace while preserving paragraph structure."""
        # Replace multiple spaces with single space
        text = cls.MULTIPLE_SPACES.sub(' ', text)
        
        # Preserve paragraph breaks (double newlines) but clean up extras
        text = cls.MULTIPLE_NEWLINES.sub('\n\n', text)
        
        return text
    
    @classmethod
    def _preserve_list_formatting(cls, text: str) -> str:
        """Preserve and clean up list formatting."""
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                cleaned_lines.append('')
                continue
            
            # Check if line starts with bullet or number
            is_list_item = any(pattern.match(line) for pattern in cls.BULLET_PATTERNS)
            
            if is_list_item:
                # Clean up list item spacing
                for pattern in cls.BULLET_PATTERNS:
                    if pattern.match(line):
                        # Normalize the bullet/number spacing
                        line = pattern.sub(lambda m: m.group().strip() + ' ', line)
                        break
            
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    @classmethod
    def extract_plain_text(cls, text: str) -> str:
        """
        Extract plain text for basic analysis, removing all formatting.
        
        Args:
            text: Text that may contain formatting
            
        Returns:
            str: Plain text suitable for basic NLP processing
        """
        if not text:
            return ""
        
        # Remove all XML/HTML
        plain = cls.XML_TAG_PATTERN.sub(' ', text)
        plain = cls._decode_entities(plain)
        
        # Remove all bullet points and list markers
        for pattern in cls.BULLET_PATTERNS:
            plain = pattern.sub('', plain)
        
        # Normalize all whitespace to single spaces
        plain = cls.MULTIPLE_SPACES.sub(' ', plain)
        plain = plain.replace('\n', ' ')
        
        return plain.strip()
    
    @classmethod
    def has_meaningful_content(cls, text: str) -> bool:
        """
        Check if text contains meaningful clinical content.
        
        Args:
            text: Text to evaluate
            
        Returns:
            bool: True if text appears to contain meaningful content
        """
        if not text:
            return False
        
        plain_text = cls.extract_plain_text(text)
        
        # Check minimum length
        if len(plain_text) < 10:
            return False
        
        # Check for meaningful words (not just numbers/symbols)
        word_count = len([word for word in plain_text.split() if word.isalpha() and len(word) > 2])
        
        return word_count >= 3