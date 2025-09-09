#!/usr/bin/env python3
"""
Test script to verify the cleaning pipeline fixes.
"""

import sys
from pathlib import Path

# Add normalize package to path
sys.path.append(str(Path(__file__).parent.parent / 'normalize'))

from normalizers.dosage_form_normalizer import DosageFormNormalizer
from normalizers.route_normalizer import RouteNormalizer
from normalizers.unii_validator import UNIIValidator


def test_dosage_form_route_compatibility():
    """Test dosage form and route compatibility fixes."""
    print("Testing dosage form and route compatibility...")
    
    test_cases = [
        # Previously failing cases
        ('powder', 'oral', True),     # Powders can be oral
        ('liquid', 'topical', True),  # Liquids can be topical
        ('shampoo', 'topical', True), # Shampoos are topical
        ('paste', 'oral', True),      # Pastes can be oral
        
        # Should still fail
        ('tablet', 'topical', False), # Tablets shouldn't be topical
        ('injection', 'oral', False), # Injections shouldn't be oral
    ]
    
    for dosage_form, route, expected in test_cases:
        result = DosageFormNormalizer.get_route_dosage_compatibility(dosage_form, route)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {dosage_form} + {route}: {result} (expected {expected})")


def test_unii_validation():
    """Test UNII validation fixes."""
    print("\nTesting UNII validation...")
    
    test_codes = [
        ('9J765S329G', True),   # The code from your error
        ('7YNJ3PO35Z', True),   # Another valid format
        ('ABC1234567', True),   # Valid format
        ('123456789A', True),   # Valid format
        ('9J765S329', False),   # Too short
        ('9J765S329GH', False), # Too long
        ('9J765S329!', False),  # Invalid character
        ('', False),            # Empty
    ]
    
    for code, expected in test_codes:
        result = UNIIValidator.is_valid_unii_format(code)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{code}': {result} (expected {expected})")


def test_normalizations():
    """Test various normalizations."""
    print("\nTesting normalizations...")
    
    # Dosage form normalization
    form_tests = [
        ('SHAMPOO', 'shampoo'),
        ('TAB', 'tablet'),
        ('PASTE', 'paste'),
        ('powder', 'powder'),
    ]
    
    for input_form, expected in form_tests:
        result = DosageFormNormalizer.normalize_dosage_form(input_form)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Form '{input_form}' → '{result}' (expected '{expected}')")
    
    # Route normalization
    route_tests = [
        ('PO', 'oral'),
        ('TOPICAL', 'topical'),
        ('IV', 'intravenous'),
    ]
    
    for input_route, expected in route_tests:
        result = RouteNormalizer.normalize_route(input_route)
        status = "✓" if result == expected else "✗"
        print(f"  {status} Route '{input_route}' → '{result}' (expected '{expected}')")


if __name__ == '__main__':
    print("Testing cleaning pipeline fixes...\n")
    
    test_dosage_form_route_compatibility()
    test_unii_validation()
    test_normalizations()
    
    print("\nTest completed!")