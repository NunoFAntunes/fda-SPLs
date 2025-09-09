"""
Route of administration normalization utilities for SPL documents.
Standardizes pharmaceutical routes to consistent terminology.
"""

from typing import Dict, Optional, Set
import re


class RouteNormalizer:
    """Normalizes routes of administration to standard terminology."""
    
    # Standard route mappings (FDA preferred terms)
    ROUTE_MAPPINGS = {
        # Oral routes
        'po': 'oral',
        'oral': 'oral',
        'by mouth': 'oral',
        'mouth': 'oral',
        'orally': 'oral',
        'per os': 'oral',
        'buccal': 'buccal',
        'sublingual': 'sublingual',
        'sl': 'sublingual',
        
        # Injection routes
        'iv': 'intravenous',
        'intravenous': 'intravenous',
        'intravenously': 'intravenous',
        'im': 'intramuscular',
        'intramuscular': 'intramuscular',
        'intramuscularly': 'intramuscular',
        'sc': 'subcutaneous',
        'subcut': 'subcutaneous',
        'subcutaneous': 'subcutaneous',
        'subcutaneously': 'subcutaneous',
        'sq': 'subcutaneous',
        'intradermal': 'intradermal',
        'id': 'intradermal',
        'intraperitoneal': 'intraperitoneal',
        'ip': 'intraperitoneal',
        'intrathecal': 'intrathecal',
        'it': 'intrathecal',
        'epidural': 'epidural',
        'intraarticular': 'intraarticular',
        'ia': 'intraarticular',
        'intravitreal': 'intravitreal',
        'intracardiac': 'intracardiac',
        'intraosseous': 'intraosseous',
        'io': 'intraosseous',
        
        # Topical routes
        'topical': 'topical',
        'topically': 'topical',
        'external': 'topical',
        'cutaneous': 'topical',
        'dermal': 'topical',
        'skin': 'topical',
        'transdermal': 'transdermal',
        
        # Inhalation routes
        'inhalation': 'inhalation',
        'inhaled': 'inhalation',
        'respiratory': 'inhalation',
        'pulmonary': 'inhalation',
        'nasal': 'nasal',
        'intranasal': 'nasal',
        'nasally': 'nasal',
        
        # Ophthalmic routes
        'ophthalmic': 'ophthalmic',
        'ocular': 'ophthalmic',
        'eye': 'ophthalmic',
        'conjunctival': 'ophthalmic',
        'intraocular': 'intraocular',
        
        # Otic routes
        'otic': 'otic',
        'aural': 'otic',
        'ear': 'otic',
        'auricular': 'otic',
        
        # Rectal/vaginal routes
        'rectal': 'rectal',
        'rectally': 'rectal',
        'pr': 'rectal',
        'per rectum': 'rectal',
        'vaginal': 'vaginal',
        'vaginally': 'vaginal',
        'intravaginal': 'vaginal',
        'pv': 'vaginal',
        
        # Urogenital routes
        'urethral': 'urethral',
        'intraurethral': 'urethral',
        'bladder': 'intravesical',
        'intravesical': 'intravesical',
        
        # Dental/gingival
        'dental': 'dental',
        'gingival': 'gingival',
        'periodontal': 'periodontal',
        
        # Other routes
        'irrigation': 'irrigation',
        'implantation': 'implantation',
        'implant': 'implantation',
        'not applicable': 'not_applicable',
        'na': 'not_applicable',
        'unknown': 'unknown',
    }
    
    # Route categories
    ROUTE_CATEGORIES = {
        'enteral': {'oral', 'buccal', 'sublingual'},
        'parenteral': {
            'intravenous', 'intramuscular', 'subcutaneous', 'intradermal',
            'intraperitoneal', 'intrathecal', 'epidural', 'intraarticular',
            'intravitreal', 'intracardiac', 'intraosseous', 'intraocular'
        },
        'topical': {'topical', 'transdermal'},
        'inhalation': {'inhalation', 'nasal'},
        'mucosal': {'ophthalmic', 'otic', 'rectal', 'vaginal', 'urethral', 'intravesical'},
        'dental': {'dental', 'gingival', 'periodontal'},
        'other': {'irrigation', 'implantation', 'not_applicable', 'unknown'},
    }
    
    # Systemic vs local action
    SYSTEMIC_ROUTES = {
        'oral', 'intravenous', 'intramuscular', 'subcutaneous', 'intradermal',
        'intraperitoneal', 'buccal', 'sublingual', 'transdermal', 'inhalation'
    }
    
    LOCAL_ROUTES = {
        'topical', 'ophthalmic', 'otic', 'nasal', 'rectal', 'vaginal',
        'urethral', 'intravesical', 'dental', 'gingival', 'periodontal',
        'irrigation', 'intraarticular', 'intravitreal'
    }
    
    @classmethod
    def normalize_route(cls, route_code: str) -> Optional[str]:
        """
        Normalize route of administration to standard terminology.
        
        Args:
            route_code: Raw route code or description
            
        Returns:
            str: Standardized route or None if not recognized
        """
        if not route_code:
            return None
        
        # Clean and normalize input
        route_clean = route_code.strip().lower()
        route_clean = re.sub(r'[^\w\s]', ' ', route_clean)  # Replace punctuation with space
        route_clean = re.sub(r'\s+', ' ', route_clean).strip()  # Normalize whitespace
        
        # Direct lookup
        if route_clean in cls.ROUTE_MAPPINGS:
            return cls.ROUTE_MAPPINGS[route_clean]
        
        # Try partial matching
        for route_key, standard_route in cls.ROUTE_MAPPINGS.items():
            if route_key in route_clean or route_clean in route_key:
                return standard_route
        
        return None
    
    @classmethod
    def get_route_category(cls, route: str) -> Optional[str]:
        """
        Get the category for a normalized route.
        
        Args:
            route: Normalized route of administration
            
        Returns:
            str: Category name or None if not found
        """
        if not route:
            return None
        
        for category, routes in cls.ROUTE_CATEGORIES.items():
            if route in routes:
                return category
        
        return 'other'
    
    @classmethod
    def is_systemic_route(cls, route: str) -> Optional[bool]:
        """
        Determine if route provides systemic drug exposure.
        
        Args:
            route: Normalized route of administration
            
        Returns:
            bool: True if systemic, False if local, None if unknown
        """
        if not route:
            return None
        
        if route in cls.SYSTEMIC_ROUTES:
            return True
        elif route in cls.LOCAL_ROUTES:
            return False
        else:
            return None
    
    @classmethod
    def is_valid_route(cls, route: str) -> bool:
        """
        Check if route is recognized.
        
        Args:
            route: Route to validate
            
        Returns:
            bool: True if recognized route
        """
        return cls.normalize_route(route) is not None
    
    @classmethod
    def get_common_abbreviations(cls, route: str) -> Set[str]:
        """
        Get common abbreviations for a normalized route.
        
        Args:
            route: Normalized route of administration
            
        Returns:
            set: Set of common abbreviations
        """
        abbreviations = set()
        
        for abbrev, normalized in cls.ROUTE_MAPPINGS.items():
            if normalized == route and len(abbrev) <= 4:
                abbreviations.add(abbrev.upper())
        
        return abbreviations