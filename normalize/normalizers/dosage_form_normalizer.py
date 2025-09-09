"""
Dosage form normalization utilities for SPL documents.
Standardizes pharmaceutical dosage forms to consistent terminology.
"""

from typing import Dict, Optional, Set
import re


class DosageFormNormalizer:
    """Normalizes pharmaceutical dosage forms to standard terminology."""
    
    # Standard dosage form mappings (FDA preferred terms)
    DOSAGE_FORM_MAPPINGS = {
        # Tablets
        'tab': 'tablet',
        'tabs': 'tablet',
        'tablet': 'tablet',
        'tablets': 'tablet',
        'tbl': 'tablet',
        'pill': 'tablet',
        'pills': 'tablet',
        
        # Capsules
        'cap': 'capsule',
        'caps': 'capsule',
        'capsule': 'capsule',
        'capsules': 'capsule',
        
        # Liquids
        'sol': 'solution',
        'soln': 'solution',
        'solution': 'solution',
        'solutions': 'solution',
        'liq': 'liquid',
        'liquid': 'liquid',
        'susp': 'suspension',
        'suspension': 'suspension',
        'syr': 'syrup',
        'syrup': 'syrup',
        'elixir': 'elixir',
        'tincture': 'tincture',
        
        # Powders and granules
        'powder': 'powder',
        'powders': 'powder',
        'granule': 'granules',
        'granules': 'granules',
        
        # Injections
        'inj': 'injection',
        'injection': 'injection',
        'injections': 'injection',
        'vial': 'injection',
        'vials': 'injection',
        'ampule': 'injection',
        'ampules': 'injection',
        'ampoule': 'injection',
        'ampoules': 'injection',
        
        # Topical forms
        'cream': 'cream',
        'creams': 'cream',
        'oint': 'ointment',
        'ointment': 'ointment',
        'ointments': 'ointment',
        'gel': 'gel',
        'gels': 'gel',
        'lotion': 'lotion',
        'lotions': 'lotion',
        'foam': 'foam',
        'foams': 'foam',
        'patch': 'patch',
        'patches': 'patch',
        
        # Inhalation forms
        'inhaler': 'inhaler',
        'inhalers': 'inhaler',
        'aerosol': 'aerosol',
        'aerosols': 'aerosol',
        'spray': 'spray',
        'sprays': 'spray',
        'powder': 'powder',
        'powders': 'powder',
        
        # Suppositories
        'supp': 'suppository',
        'supps': 'suppository',
        'suppository': 'suppository',
        'suppositories': 'suppository',
        
        # Extended release forms
        'er': 'extended_release',
        'xr': 'extended_release',
        'sr': 'sustained_release',
        'cr': 'controlled_release',
        'la': 'long_acting',
        'xl': 'extended_release',
        'cd': 'controlled_delivery',
        
        # Other forms
        'drop': 'drops',
        'drops': 'drops',
        'gtt': 'drops',
        'pellet': 'pellet',
        'pellets': 'pellet',
        'film': 'film',
        'films': 'film',
        'strip': 'strip',
        'strips': 'strip',
        'device': 'device',
        'kit': 'kit',
        'kits': 'kit',
        
        # Specialized topical forms
        'shampoo': 'shampoo',
        'paste': 'paste',
        'bar': 'bar',
        'soap': 'soap',
        'cleanser': 'cleanser',
        'rinse': 'rinse',
        'mouthwash': 'mouthwash',
        'wash': 'wash',
    }
    
    # Categories for dosage forms
    DOSAGE_FORM_CATEGORIES = {
        'solid_oral': {'tablet', 'capsule', 'pellet'},
        'powder_oral': {'granules', 'powder'},  # Powders can be oral
        'liquid_oral': {'solution', 'suspension', 'syrup', 'elixir', 'liquid'},
        'paste_oral': {'paste'},  # Pastes can be oral (like toothpaste with fluoride)
        'injection': {'injection'},
        'topical': {'cream', 'ointment', 'gel', 'lotion', 'foam', 'patch', 'shampoo', 'bar', 'soap', 'cleanser', 'wash'},
        'topical_liquid': {'liquid'},  # Liquids can be topical
        'inhalation': {'inhaler', 'aerosol', 'spray'},
        'inhalation_powder': {'powder'},  # Powders can be inhaled
        'suppository': {'suppository'},
        'ophthalmic': {'drops'},
        'oral_rinse': {'rinse', 'mouthwash'},  # Oral cavity applications
        'other': {'film', 'strip', 'device', 'kit', 'tincture'},
    }
    
    @classmethod
    def normalize_dosage_form(cls, form_code: str) -> Optional[str]:
        """
        Normalize dosage form to standard terminology.
        
        Args:
            form_code: Raw dosage form code or description
            
        Returns:
            str: Standardized dosage form or None if not recognized
        """
        if not form_code:
            return None
        
        # Clean and normalize input
        form_clean = form_code.strip().lower()
        form_clean = re.sub(r'[^\w\s]', '', form_clean)  # Remove punctuation
        
        # Direct lookup
        if form_clean in cls.DOSAGE_FORM_MAPPINGS:
            return cls.DOSAGE_FORM_MAPPINGS[form_clean]
        
        # Try partial matching for compound forms
        normalized = cls._handle_compound_forms(form_clean)
        if normalized:
            return normalized
        
        return None
    
    @classmethod
    def _handle_compound_forms(cls, form_str: str) -> Optional[str]:
        """Handle compound dosage forms like 'tablet, extended release'."""
        # Split on common separators
        parts = re.split(r'[,\s]+', form_str)
        
        base_form = None
        modifiers = []
        
        for part in parts:
            if part in cls.DOSAGE_FORM_MAPPINGS:
                mapped = cls.DOSAGE_FORM_MAPPINGS[part]
                
                # Identify base form vs modifier
                if mapped in {'extended_release', 'sustained_release', 'controlled_release', 
                            'long_acting', 'controlled_delivery'}:
                    modifiers.append(mapped)
                elif not base_form:
                    base_form = mapped
        
        if base_form:
            if modifiers:
                return f"{base_form}_{modifiers[0]}"  # Take first modifier
            return base_form
        
        return None
    
    @classmethod
    def get_dosage_form_category(cls, dosage_form: str) -> Optional[str]:
        """
        Get the category for a normalized dosage form.
        
        Args:
            dosage_form: Normalized dosage form
            
        Returns:
            str: Category name or None if not found
        """
        if not dosage_form:
            return None
        
        # Remove modifiers for categorization
        base_form = dosage_form.split('_')[0]
        
        for category, forms in cls.DOSAGE_FORM_CATEGORIES.items():
            if base_form in forms:
                return category
        
        return 'other'
    
    @classmethod
    def is_valid_dosage_form(cls, dosage_form: str) -> bool:
        """
        Check if dosage form is recognized.
        
        Args:
            dosage_form: Dosage form to validate
            
        Returns:
            bool: True if recognized dosage form
        """
        return cls.normalize_dosage_form(dosage_form) is not None
    
    @classmethod
    def get_route_dosage_compatibility(cls, dosage_form: str, route: str) -> bool:
        """
        Check if dosage form is compatible with route of administration.
        
        Args:
            dosage_form: Normalized dosage form
            route: Route of administration (normalized)
            
        Returns:
            bool: True if compatible combination
        """
        if not dosage_form or not route:
            return True  # Can't validate, assume compatible
        
        route_lower = route.lower()
        form_lower = dosage_form.lower()
        
        # Define compatible combinations (more comprehensive)
        compatibility_rules = {
            'oral': {
                'compatible_forms': {'tablet', 'capsule', 'pellet', 'granules', 'powder', 'solution', 
                                   'suspension', 'syrup', 'elixir', 'liquid', 'paste', 'film', 'strip'},
                'incompatible_forms': {'injection', 'cream', 'ointment', 'gel', 'lotion', 'foam', 
                                     'patch', 'inhaler', 'aerosol', 'suppository'}
            },
            'topical': {
                'compatible_forms': {'cream', 'ointment', 'gel', 'lotion', 'foam', 'patch', 'shampoo', 
                                   'paste', 'bar', 'soap', 'cleanser', 'wash', 'liquid', 'solution', 
                                   'suspension', 'spray', 'powder'},
                'incompatible_forms': {'tablet', 'capsule', 'injection', 'suppository', 'inhaler'}
            },
            'intravenous': {
                'compatible_forms': {'injection', 'solution'},
                'incompatible_forms': {'tablet', 'capsule', 'cream', 'ointment', 'gel', 'suppository'}
            },
            'intramuscular': {
                'compatible_forms': {'injection', 'solution'},
                'incompatible_forms': {'tablet', 'capsule', 'cream', 'ointment', 'gel', 'suppository'}
            },
            'subcutaneous': {
                'compatible_forms': {'injection', 'solution'},
                'incompatible_forms': {'tablet', 'capsule', 'cream', 'ointment', 'gel', 'suppository'}
            },
            'inhalation': {
                'compatible_forms': {'inhaler', 'aerosol', 'spray', 'powder', 'solution'},
                'incompatible_forms': {'tablet', 'capsule', 'cream', 'ointment', 'gel', 'suppository'}
            },
            'nasal': {
                'compatible_forms': {'spray', 'drops', 'gel', 'solution', 'suspension'},
                'incompatible_forms': {'tablet', 'capsule', 'injection', 'suppository'}
            },
            'rectal': {
                'compatible_forms': {'suppository', 'solution', 'suspension', 'gel'},
                'incompatible_forms': {'tablet', 'capsule', 'injection', 'inhaler'}
            },
            'ophthalmic': {
                'compatible_forms': {'drops', 'solution', 'suspension', 'gel', 'ointment'},
                'incompatible_forms': {'tablet', 'capsule', 'injection', 'suppository'}
            },
            'otic': {
                'compatible_forms': {'drops', 'solution', 'suspension', 'gel'},
                'incompatible_forms': {'tablet', 'capsule', 'injection', 'suppository'}
            }
        }
        
        # Check each route pattern
        for route_pattern, rules in compatibility_rules.items():
            if route_pattern in route_lower:
                if form_lower in rules['compatible_forms']:
                    return True
                elif form_lower in rules['incompatible_forms']:
                    return False
        
        # If no specific rule found, assume compatible
        return True