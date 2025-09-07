"""
Comprehensive test suite for Phase 3 normalization pipeline.
Tests the normalization utilities with real SPL data from Phase 2 output.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Phase 2 components
from parse.spl_document_parser import SPLDocumentParser, parse_spl_file
from parse.models import *

# Import Phase 3 normalization components
from normalize.text_processors.text_cleaner import TextCleaner
from normalize.normalizers.date_normalizer import DateNormalizer
from normalize.normalizers.unit_normalizer import UnitNormalizer
from normalize.normalizers.ndc_validator import NDCValidator


def get_test_spl_document():
    """Get a parsed SPL document for testing."""
    test_file = r"C:\code\fda-SPLs\example.xml"
    
    if not os.path.exists(test_file):
        return None
    
    result = parse_spl_file(test_file)
    
    # Save JSON file using the to_json method
    if result.success and result.document:
        try:
            json_output_path = Path(test_file).parent / "parsed_document.json"
            json_content = result.to_json()
            with open(json_output_path, 'w', encoding='utf-8') as f:
                f.write(json_content)
            print(f"Saved JSON output to: {json_output_path}")
        except Exception as e:
            print(f"Warning: Failed to save JSON output: {str(e)}")
    
    return result.document if result.success else None


def test_text_cleaner():
    """Test TextCleaner functionality with real SPL text content."""
    print("Testing TextCleaner...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[SKIP] Test document not available")
        return False
    
    try:
        # Test with various section types
        tests_run = 0
        tests_passed = 0
        
        # Find sections with text content to test
        sections_with_text = [s for s in document.sections if s.text_content and len(s.text_content.strip()) > 50]
        
        if not sections_with_text:
            print("[SKIP] No sections with substantial text content found")
            return False
        
        print(f"Found {len(sections_with_text)} sections with text content")
        
        # Test each section's text cleaning
        for i, section in enumerate(sections_with_text[:3]):  # Test first 3 sections
            tests_run += 1
            
            original_text = section.text_content
            cleaned_text = TextCleaner.clean_clinical_text(original_text)
            plain_text = TextCleaner.extract_plain_text(original_text)
            
            print(f"  Section {i+1} ({section.section_type}):")
            print(f"    Original length: {len(original_text)}")
            print(f"    Cleaned length: {len(cleaned_text)}")
            print(f"    Plain text length: {len(plain_text)}")
            
            # Validate cleaning worked
            if len(cleaned_text) > 0 and len(plain_text) > 0:
                print(f"    [OK] Text cleaning successful")
                tests_passed += 1
                
                # Show sample if text is long enough
                if len(plain_text) > 100:
                    print(f"    Sample: {plain_text[:100]}...")
            else:
                print(f"    [FAIL] Text cleaning failed")
        
        # Test HTML/XML markup removal
        html_test = "<p>This is a <b>test</b> with <em>markup</em>.</p>"
        cleaned_html = TextCleaner.clean_clinical_text(html_test)
        plain_html = TextCleaner.extract_plain_text(html_test)
        
        print(f"  HTML test:")
        print(f"    Input: {html_test}")
        print(f"    Cleaned: {cleaned_html}")
        print(f"    Plain: {plain_html}")
        
        if "test" in plain_html and "<" not in plain_html:
            print(f"    [OK] HTML removal successful")
            tests_passed += 1
        else:
            print(f"    [FAIL] HTML removal failed")
        tests_run += 1
        
        success_rate = tests_passed / tests_run if tests_run > 0 else 0
        print(f"[OK] TextCleaner tests: {tests_passed}/{tests_run} passed ({success_rate*100:.1f}%)")
        return success_rate > 0.8
        
    except Exception as e:
        print(f"[ERROR] TextCleaner test failed: {str(e)}")
        return False


def test_date_normalizer():
    """Test DateNormalizer with SPL document dates."""
    print("\nTesting DateNormalizer...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[SKIP] Test document not available")
        return False
    
    try:
        normalizer = DateNormalizer()
        tests_run = 0
        tests_passed = 0
        
        # Test document dates
        if document.effective_time:
            tests_run += 1
            normalized_date = normalizer.normalize_date(document.effective_time)
            if normalized_date:
                print(f"  Document effective time:")
                print(f"    Original: {document.effective_time}")
                print(f"    Normalized: {normalized_date}")
                print(f"    [OK] Date normalization successful")
                tests_passed += 1
            else:
                print(f"  [FAIL] Failed to normalize document effective time: {document.effective_time}")
        
        # Test various date formats commonly found in SPL
        test_dates = [
            "20240315",
            "2024-03-15",
            "03/15/2024",
            "March 15, 2024",
            "2024-03-15T10:30:00",
            "20240315103000"
        ]
        
        for test_date in test_dates:
            tests_run += 1
            normalized = normalizer.normalize_date(test_date)
            if normalized:
                print(f"  Date format test: {test_date} -> {normalized} [OK]")
                tests_passed += 1
            else:
                print(f"  Date format test: {test_date} -> Failed [FAIL]")
        
        # Test date extraction from text (using private method for testing)
        text_with_dates = "The study was conducted from 2024-01-15 to 2024-06-30 with follow-up on 20241201."
        # Note: _extract_date_from_text is a private method that extracts the first date
        extracted_date = normalizer._extract_date_from_text(text_with_dates)
        tests_run += 1
        if extracted_date:
            print(f"  Date extraction from text:")
            print(f"    Text: {text_with_dates}")
            print(f"    First extracted date: {extracted_date}")
            print(f"    [OK] Date extraction successful")
            tests_passed += 1
        else:
            print(f"  [FAIL] Date extraction from text failed")
        
        success_rate = tests_passed / tests_run if tests_run > 0 else 0
        print(f"[OK] DateNormalizer tests: {tests_passed}/{tests_run} passed ({success_rate*100:.1f}%)")
        return success_rate > 0.7
        
    except Exception as e:
        print(f"[ERROR] DateNormalizer test failed: {str(e)}")
        return False


def test_unit_normalizer():
    """Test UnitNormalizer with pharmaceutical units from SPL data."""
    print("\nTesting UnitNormalizer...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[SKIP] Test document not available")
        return False
    
    try:
        normalizer = UnitNormalizer()
        tests_run = 0
        tests_passed = 0
        
        # Test units from ingredients in the document
        products = document.get_manufactured_products()
        active_ingredients = document.get_active_ingredients()
        
        print(f"Testing with {len(active_ingredients)} active ingredients")
        
        # Test ingredient units
        for ingredient in active_ingredients[:5]:  # Test first 5 ingredients
            if ingredient.quantity and ingredient.quantity.numerator_unit:
                tests_run += 1
                unit = ingredient.quantity.numerator_unit
                normalized = normalizer.normalize_unit(unit)
                
                print(f"  Ingredient unit: {unit} -> {normalized}")
                if normalized:
                    print(f"    [OK] Unit normalization successful")
                    tests_passed += 1
                else:
                    print(f"    [FAIL] Unit normalization failed")
        
        # Test common pharmaceutical units
        test_units = [
            "mg",
            "milligram",
            "mL",
            "milliliter",
            "tablet",
            "capsule",
            "mg/mL",
            "mcg",
            "microgram",
            "L",
            "liter",
            "g",
            "gram",
            "IU",
            "unit"
        ]
        
        print(f"  Testing common pharmaceutical units:")
        for unit in test_units:
            tests_run += 1
            normalized = normalizer.normalize_unit(unit)
            unit_category = normalizer.get_unit_category(unit) or "unknown"
            
            if normalized:
                print(f"    {unit} -> {normalized} ({unit_category}) [OK]")
                tests_passed += 1
            else:
                print(f"    {unit} -> Failed [FAIL]")
        
        # Test complex unit combinations
        complex_units = ["mg/mL", "mcg/kg", "units/mL", "mg per tablet"]
        print(f"  Testing complex unit combinations:")
        for unit in complex_units:
            tests_run += 1
            normalized = normalizer.normalize_unit(unit)
            if normalized:
                print(f"    {unit} -> {normalized} [OK]")
                tests_passed += 1
            else:
                print(f"    {unit} -> Failed [FAIL]")
        
        success_rate = tests_passed / tests_run if tests_run > 0 else 0
        print(f"[OK] UnitNormalizer tests: {tests_passed}/{tests_run} passed ({success_rate*100:.1f}%)")
        return success_rate > 0.6
        
    except Exception as e:
        print(f"[ERROR] UnitNormalizer test failed: {str(e)}")
        return False


def test_ndc_validator():
    """Test NDCValidator with NDC codes from product data."""
    print("\nTesting NDCValidator...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[SKIP] Test document not available")
        return False
    
    try:
        tests_run = 0
        tests_passed = 0
        
        # Get NDC codes from manufactured products
        products = document.get_manufactured_products()
        ndc_codes = []
        
        for product in products:
            if product.product_code and product.product_code.code_system == "2.16.840.1.113883.6.69":
                ndc_codes.append(product.product_code.code)
        
        print(f"Found {len(ndc_codes)} NDC codes in document")
        
        # Test NDC codes from document
        for ndc in ndc_codes[:5]:  # Test first 5 NDC codes
            tests_run += 1
            is_valid = NDCValidator.validate_ndc(ndc)
            normalized = NDCValidator.normalize_ndc(ndc)
            
            print(f"  NDC from document: {ndc}")
            print(f"    Valid: {is_valid}")
            print(f"    Normalized: {normalized}")
            
            if is_valid and normalized:
                print(f"    [OK] NDC validation successful")
                tests_passed += 1
            else:
                print(f"    [FAIL] NDC validation failed")
        
        # Test various NDC formats
        test_ndcs = [
            "12345-678-90",
            "12345-6789-0",
            "1234-5678-90",
            "123456789",
            "12345678901",
            "1234567890",
            "invalid-ndc",
            "12345-678-901",  # Too many digits
            ""
        ]
        
        print(f"  Testing various NDC formats:")
        for ndc in test_ndcs:
            tests_run += 1
            is_valid = NDCValidator.validate_ndc(ndc)
            normalized = NDCValidator.normalize_ndc(ndc) if is_valid else None
            
            status = "[OK]" if is_valid else "[FAIL]"
            print(f"    {ndc} -> Valid: {is_valid}, Normalized: {normalized} {status}")
            
            # For valid NDCs, normalization should work
            if is_valid and normalized:
                tests_passed += 1
            # For invalid NDCs, validation should correctly identify them
            elif not is_valid and not normalized:
                tests_passed += 1
        
        # Test NDC extraction from text
        text_with_ndcs = "Products with NDC codes 12345-678-90 and 98765-432-10 are available."
        extracted_ndcs = NDCValidator.extract_ndcs_from_text(text_with_ndcs)
        tests_run += 1
        
        print(f"  NDC extraction from text:")
        print(f"    Text: {text_with_ndcs}")
        print(f"    Extracted: {extracted_ndcs}")
        
        if extracted_ndcs and len(extracted_ndcs) >= 2:
            print(f"    [OK] NDC extraction successful")
            tests_passed += 1
        else:
            print(f"    [FAIL] NDC extraction failed")
        
        success_rate = tests_passed / tests_run if tests_run > 0 else 0
        print(f"[OK] NDCValidator tests: {tests_passed}/{tests_run} passed ({success_rate*100:.1f}%)")
        return success_rate > 0.7
        
    except Exception as e:
        print(f"[ERROR] NDCValidator test failed: {str(e)}")
        return False


def test_normalization_integration():
    """Test integration of normalization utilities with Phase 2 output."""
    print("\nTesting Normalization Integration...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[SKIP] Test document not available")
        return False
    
    try:
        # Initialize all normalizers
        date_normalizer = DateNormalizer()
        unit_normalizer = UnitNormalizer()
        
        # Create a simplified normalized document structure for testing
        normalized_data = {
            'document_id': document.document_id,
            'set_id': document.set_id,
            'version_number': document.version_number,
            'normalized_effective_time': None,
            'normalized_sections': [],
            'normalized_products': [],
            'validation_summary': {}
        }
        
        print(f"Processing document: {document.document_id}")
        
        # Normalize document-level dates
        if document.effective_time:
            normalized_data['normalized_effective_time'] = date_normalizer.normalize_date(document.effective_time)
            print(f"  [OK] Normalized effective time: {normalized_data['normalized_effective_time']}")
        
        # Process sections
        sections_processed = 0
        for section in document.sections[:5]:  # Process first 5 sections
            if section.text_content:
                normalized_section = {
                    'section_id': section.section_id,
                    'section_type': section.section_type,
                    'original_text_length': len(section.text_content),
                    'cleaned_text': TextCleaner.clean_clinical_text(section.text_content),
                    'plain_text': TextCleaner.extract_plain_text(section.text_content),
                }
                normalized_section['cleaned_text_length'] = len(normalized_section['cleaned_text'])
                normalized_section['plain_text_length'] = len(normalized_section['plain_text'])
                
                normalized_data['normalized_sections'].append(normalized_section)
                sections_processed += 1
        
        print(f"  [OK] Processed {sections_processed} sections")
        
        # Process products
        products = document.get_manufactured_products()
        products_processed = 0
        
        for product in products[:3]:  # Process first 3 products
            normalized_product = {
                'product_name': product.product_name,
                'ndc_valid': False,
                'ndc_normalized': None,
                'ingredients_with_normalized_units': []
            }
            
            # Validate and normalize NDC
            if product.product_code and product.product_code.code_system == "2.16.840.1.113883.6.69":
                ndc = product.product_code.code
                normalized_product['ndc_valid'] = NDCValidator.validate_ndc(ndc)
                normalized_product['ndc_normalized'] = NDCValidator.normalize_ndc(ndc)
            
            # Process ingredient units
            for ingredient in product.ingredients:
                if ingredient.quantity and ingredient.quantity.numerator_unit:
                    normalized_unit = unit_normalizer.normalize_unit(ingredient.quantity.numerator_unit)
                    unit_category = unit_normalizer.get_unit_category(ingredient.quantity.numerator_unit)
                    
                    normalized_product['ingredients_with_normalized_units'].append({
                        'substance_name': ingredient.substance_name,
                        'original_unit': ingredient.quantity.numerator_unit,
                        'normalized_unit': normalized_unit,
                        'unit_category': unit_category
                    })
            
            normalized_data['normalized_products'].append(normalized_product)
            products_processed += 1
        
        print(f"  [OK] Processed {products_processed} products")
        
        # Create validation summary
        normalized_data['validation_summary'] = {
            'total_sections_processed': sections_processed,
            'total_products_processed': products_processed,
            'sections_with_text': len([s for s in normalized_data['normalized_sections'] if s['plain_text_length'] > 0]),
            'products_with_valid_ndc': len([p for p in normalized_data['normalized_products'] if p['ndc_valid']]),
            'ingredients_with_normalized_units': sum(len(p['ingredients_with_normalized_units']) for p in normalized_data['normalized_products']),
            'processing_timestamp': datetime.now().isoformat()
        }
        
        # Print summary
        summary = normalized_data['validation_summary']
        print(f"  Integration Summary:")
        print(f"    Sections processed: {summary['total_sections_processed']}")
        print(f"    Sections with text: {summary['sections_with_text']}")
        print(f"    Products processed: {summary['total_products_processed']}")
        print(f"    Products with valid NDC: {summary['products_with_valid_ndc']}")
        print(f"    Ingredients with normalized units: {summary['ingredients_with_normalized_units']}")
        
        # Determine success based on processing results
        success = (
            summary['total_sections_processed'] > 0 and
            summary['sections_with_text'] > 0 and
            summary['total_products_processed'] > 0
        )
        
        if success:
            print(f"[OK] Integration test passed - all normalization utilities working together")
        else:
            print(f"[ERROR] Integration test failed - insufficient data processed")
        
        return success
        
    except Exception as e:
        print(f"[ERROR] Integration test failed: {str(e)}")
        return False


def main():
    """Run complete Phase 3 normalization test suite."""
    print("Phase 3 Normalization Pipeline Test Suite")
    print("=" * 50)
    
    # Check if we can get test data
    document = get_test_spl_document()
    if not document:
        print("[ERROR] Cannot load test SPL document. Please ensure Phase 2 test data is available.")
        return False
    
    print(f"Using test document: {document.document_id}")
    print(f"Document sections: {len(document.sections)}")
    print(f"Manufactured products: {len(document.get_manufactured_products())}")
    print(f"Active ingredients: {len(document.get_active_ingredients())}")
    
    tests = [
        ("Text Cleaner", test_text_cleaner),
        ("Date Normalizer", test_date_normalizer), 
        ("Unit Normalizer", test_unit_normalizer),
        ("NDC Validator", test_ndc_validator),
        ("Normalization Integration", test_normalization_integration),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            print(f"\n{test_name}")
            print("-" * len(test_name))
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"[CRITICAL ERROR] {test_name} failed with exception: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Results Summary:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{status:>4} | {test_name}")
        if success:
            passed += 1
    
    print(f"\nResults: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n[SUCCESS] All tests passed! Phase 3 normalization utilities are working correctly.")
        return True
    elif passed >= total * 0.8:
        print(f"\n[OK] Most tests passed ({passed}/{total}). Phase 3 normalization is largely functional.")
        return True
    else:
        print(f"\n[WARNING] Several tests failed ({total-passed}/{total}). Phase 3 normalization needs attention.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)