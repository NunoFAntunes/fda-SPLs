"""
NDC (National Drug Code) validation and normalization utilities.
Handles validation and standardization of NDC formats according to FDA standards.
"""

import re
from typing import List, Optional, Tuple, Set


class NDCValidator:
    """Validates and normalizes NDC (National Drug Code) identifiers."""
    
    # NDC format patterns (without hyphens)
    NDC_PATTERNS = [
        re.compile(r'^\d{10}$'),    # 10-digit format (most common)
        re.compile(r'^\d{11}$'),    # 11-digit format
    ]
    
    # NDC format patterns (with hyphens) - various configurations
    NDC_HYPHEN_PATTERNS = [
        re.compile(r'^\d{4}-\d{4}-\d{2}$'),  # 4-4-2 format
        re.compile(r'^\d{5}-\d{3}-\d{2}$'),  # 5-3-2 format  
        re.compile(r'^\d{5}-\d{4}-\d{1}$'),  # 5-4-1 format
        re.compile(r'^\d{6}-\d{3}-\d{2}$'),  # 6-3-2 format (less common)
    ]
    
    # Pattern for extracting NDC from text
    NDC_EXTRACTION_PATTERN = re.compile(r'\b(?:NDC[:\s]*)?(\d{4,6}[-\s]?\d{3,4}[-\s]?\d{1,2})\b', re.IGNORECASE)
    
    @classmethod
    def validate_ndc(cls, ndc: str) -> bool:
        """
        Validate if string is a properly formatted NDC.
        
        Args:
            ndc: NDC string to validate
            
        Returns:
            bool: True if valid NDC format
        """
        if not ndc:
            return False
        
        ndc_clean = cls._clean_ndc(ndc)
        
        # Check basic patterns first
        for pattern in cls.NDC_PATTERNS:
            if pattern.match(ndc_clean):
                return cls._validate_ndc_structure(ndc_clean)
        
        # Check hyphenated patterns
        for pattern in cls.NDC_HYPHEN_PATTERNS:
            if pattern.match(ndc):
                # Convert to clean format and validate
                ndc_clean = ndc.replace('-', '')
                return cls._validate_ndc_structure(ndc_clean)
        
        return False
    
    @classmethod
    def normalize_ndc(cls, ndc: str) -> Optional[str]:
        """
        Normalize NDC to standard 10-digit format with hyphens.
        
        Args:
            ndc: NDC string in various formats
            
        Returns:
            str: Normalized NDC in 5-4-1 format or None if invalid
        """
        if not cls.validate_ndc(ndc):
            return None
        
        ndc_clean = cls._clean_ndc(ndc)
        
        # Pad to 10 digits if needed (some NDCs are stored with leading zeros removed)
        if len(ndc_clean) == 9:
            ndc_clean = '0' + ndc_clean
        elif len(ndc_clean) == 11:
            # For 11-digit NDCs, typically remove the first digit if it's 0
            if ndc_clean.startswith('0'):
                ndc_clean = ndc_clean[1:]
        
        if len(ndc_clean) != 10:
            return None
        
        # Format as 5-4-1 (FDA standard)
        return f"{ndc_clean[:5]}-{ndc_clean[5:9]}-{ndc_clean[9:]}"
    
    @classmethod
    def extract_ndcs_from_text(cls, text: str) -> List[str]:
        """
        Extract all valid NDCs from text.
        
        Args:
            text: Text that may contain NDC codes
            
        Returns:
            list: List of normalized NDC codes found in text
        """
        if not text:
            return []
        
        ndcs = []
        matches = cls.NDC_EXTRACTION_PATTERN.findall(text)
        
        for match in matches:
            normalized = cls.normalize_ndc(match)
            if normalized and normalized not in ndcs:
                ndcs.append(normalized)
        
        return ndcs
    
    @classmethod
    def _clean_ndc(cls, ndc: str) -> str:
        """Remove hyphens, spaces, and other formatting from NDC."""
        if not ndc:
            return ""
        
        # Remove common prefixes
        clean = re.sub(r'^NDC[:\s]*', '', ndc, flags=re.IGNORECASE)
        
        # Remove hyphens and spaces
        clean = re.sub(r'[-\s]', '', clean)
        
        return clean.strip()
    
    @classmethod
    def _validate_ndc_structure(cls, ndc_clean: str) -> bool:
        """
        Validate the internal structure of a clean NDC.
        Performs basic sanity checks beyond format validation.
        """
        if not ndc_clean or not ndc_clean.isdigit():
            return False
        
        # NDCs should be 10 or 11 digits
        if len(ndc_clean) not in [10, 11]:
            return False
        
        # Basic sanity checks - NDC shouldn't be all zeros or all the same digit
        if len(set(ndc_clean)) == 1:
            return False
        
        # For 10-digit NDCs, validate basic structure
        if len(ndc_clean) == 10:
            # First part (labeler code) shouldn't be all zeros
            if ndc_clean[:5] == '00000':
                return False
            
            # Product code shouldn't be all zeros
            if ndc_clean[5:9] == '0000':
                return False
        
        return True
    
    @classmethod
    def get_ndc_components(cls, ndc: str) -> Optional[Tuple[str, str, str]]:
        """
        Extract components from normalized NDC.
        
        Args:
            ndc: Normalized NDC string
            
        Returns:
            tuple: (labeler_code, product_code, package_code) or None if invalid
        """
        normalized = cls.normalize_ndc(ndc)
        if not normalized:
            return None
        
        parts = normalized.split('-')
        if len(parts) != 3:
            return None
        
        return (parts[0], parts[1], parts[2])
    
    @classmethod
    def is_valid_labeler_code(cls, labeler_code: str) -> bool:
        """
        Check if labeler code appears valid.
        
        Args:
            labeler_code: 5-digit labeler code
            
        Returns:
            bool: True if appears to be valid labeler code
        """
        if not labeler_code or len(labeler_code) != 5:
            return False
        
        if not labeler_code.isdigit():
            return False
        
        # Shouldn't be all zeros
        if labeler_code == '00000':
            return False
        
        return True
    
    @classmethod
    def deduplicate_ndcs(cls, ndcs: List[str]) -> List[str]:
        """
        Remove duplicate NDCs from list, keeping normalized versions.
        
        Args:
            ndcs: List of NDC strings (may be in different formats)
            
        Returns:
            list: Deduplicated list of normalized NDCs
        """
        if not ndcs:
            return []
        
        normalized_set = set()
        result = []
        
        for ndc in ndcs:
            normalized = cls.normalize_ndc(ndc)
            if normalized and normalized not in normalized_set:
                normalized_set.add(normalized)
                result.append(normalized)
        
        return result
    
    @classmethod
    def validate_ndc_list(cls, ndcs: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate a list of NDCs and separate valid from invalid.
        
        Args:
            ndcs: List of NDC strings to validate
            
        Returns:
            tuple: (valid_ndcs, invalid_ndcs) - both normalized/cleaned
        """
        valid = []
        invalid = []
        
        for ndc in ndcs:
            if cls.validate_ndc(ndc):
                normalized = cls.normalize_ndc(ndc)
                if normalized:
                    valid.append(normalized)
                else:
                    invalid.append(ndc)
            else:
                invalid.append(ndc)
        
        return (cls.deduplicate_ndcs(valid), list(set(invalid)))