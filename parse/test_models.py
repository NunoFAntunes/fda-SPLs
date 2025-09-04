"""
Test script to verify the SPL data models and parsing infrastructure.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parse.models import *
from parse.validators import *
from parse.base_parser import *
import xml.etree.ElementTree as ET
from datetime import datetime


def test_basic_models():
    """Test basic model creation and validation."""
    print("Testing basic model creation...")
    
    # Test CodedConcept
    concept = CodedConcept(
        code="34390-5",
        code_system="2.16.840.1.113883.6.1",
        display_name="HUMAN OTC DRUG LABEL"
    )
    print(f"[OK] Created CodedConcept: {concept.display_name}")
    
    # Test Ingredient
    ingredient = Ingredient(
        type=IngredientType.ACTIVE,
        substance_name="TIOCONAZOLE",
        substance_code=CodedConcept(
            code="S57Y5X1117",
            code_system="2.16.840.1.113883.4.9"
        ),
        quantity=Quantity(
            numerator_value=65.0,
            numerator_unit="mg",
            denominator_value=1.0,
            denominator_unit="g"
        )
    )
    print(f"[OK] Created Ingredient: {ingredient.substance_name}")
    
    # Test ManufacturedProduct
    product = ManufacturedProduct(
        product_name="TIOCONAZOLE",
        product_code=CodedConcept(
            code="63094-0426",
            code_system="2.16.840.1.113883.6.69"
        ),
        ingredients=[ingredient]
    )
    print(f"[OK] Created ManufacturedProduct: {product.product_name}")
    
    # Test SPLDocument
    document = SPLDocument(
        document_id="11ddf219-8537-4a8f-b267-ef965159885e",
        set_id="11ddf219-8537-4a8f-b267-ef965159885e",
        version_number="1",
        processed_at=datetime.now()
    )
    print(f"[OK] Created SPLDocument: {document.document_id}")
    
    return document, product, ingredient


def test_validation():
    """Test validation functionality."""
    print("\nTesting validation...")
    
    # Create a document with some issues for testing
    document = SPLDocument(
        document_id="invalid-uuid",  # Invalid UUID
        set_id="11ddf219-8537-4a8f-b267-ef965159885e",
        version_number="1"
        # Missing effective_time
    )
    
    # Add a section with issues
    section = SPLSection(
        section_id="566ff39e-b0f9-4060-9860-45fa170ed498",
        section_code=CodedConcept(
            code="55106-9",
            code_system="2.16.840.1.113883.6.1",
            display_name="OTC - ACTIVE INGREDIENT SECTION"
        )
    )
    document.sections.append(section)
    
    # Validate the document
    validator = SPLDocumentValidator()
    result = validator.validate(document)
    
    print(f"[OK] Validation completed: {result}")
    print(f"  - Errors: {len(result.errors)}")
    print(f"  - Warnings: {len(result.warnings)}")
    
    # Print first few issues
    for error in result.errors[:3]:
        print(f"  Error: {error.message}")
    
    return result


def test_xml_utils():
    """Test XML utility functions."""
    print("\nTesting XML utilities...")
    
    # Create a simple XML element for testing
    xml_string = '''
    <document xmlns="urn:hl7-org:v3">
        <id root="test-id"/>
        <code code="34390-5" codeSystem="2.16.840.1.113883.6.1" displayName="HUMAN OTC DRUG LABEL"/>
        <effectiveTime value="20090831"/>
    </document>
    '''
    
    root = ET.fromstring(xml_string)
    
    # Test finding elements
    id_element = XMLUtils.find_element(root, "id")
    if id_element is not None:
        root_attr = XMLUtils.get_attribute(id_element, "root")
        print(f"[OK] Found ID element with root: {root_attr}")
    
    # Test parsing coded concept
    code_element = XMLUtils.find_element(root, "code")
    if code_element is not None:
        concept = XMLUtils.parse_coded_concept(code_element)
        print(f"[OK] Parsed CodedConcept: {concept.display_name}")
    
    return True


def test_section_type_mapping():
    """Test section type mapping."""
    print("\nTesting section type mapping...")
    
    # Test known section types
    test_codes = [
        ("55106-9", "ACTIVE_INGREDIENT"),
        ("34071-1", "WARNINGS"),
        ("50570-1", "DO_NOT_USE"),
        ("unknown", None)
    ]
    
    for code, expected_name in test_codes:
        section_type = SectionTypeMapper.get_section_type(code)
        if expected_name is None:
            if section_type is None:
                print(f"[OK] Unknown code '{code}' correctly returned None")
            else:
                print(f"[ERROR] Expected None for unknown code '{code}', got {section_type}")
        else:
            if section_type and section_type.name == expected_name:
                print(f"[OK] Code '{code}' mapped to {section_type.name}")
            else:
                print(f"[ERROR] Code '{code}' mapping failed. Expected {expected_name}, got {section_type}")
    
    return True


def test_document_methods():
    """Test SPLDocument utility methods."""
    print("\nTesting SPLDocument methods...")
    
    document = SPLDocument(
        document_id="11ddf219-8537-4a8f-b267-ef965159885e",
        set_id="11ddf219-8537-4a8f-b267-ef965159885e",
        version_number="1"
    )
    
    # Add sections
    active_ingredient_section = SPLSection(
        section_id="section1",
        section_type=SectionType.ACTIVE_INGREDIENT,
        text_content="Active ingredient: Tioconazole 300 mg"
    )
    
    warnings_section = SPLSection(
        section_id="section2",
        section_type=SectionType.WARNINGS,
        text_content="For vaginal use only"
    )
    
    document.sections.extend([active_ingredient_section, warnings_section])
    
    # Test getting sections by type
    active_sections = document.get_sections_by_type(SectionType.ACTIVE_INGREDIENT)
    print(f"[OK] Found {len(active_sections)} active ingredient sections")
    
    warnings_sections = document.get_sections_by_type(SectionType.WARNINGS)
    print(f"[OK] Found {len(warnings_sections)} warnings sections")
    
    # Test getting section text
    active_text = document.get_section_text_by_type(SectionType.ACTIVE_INGREDIENT)
    print(f"[OK] Active ingredient text: {active_text}")
    
    return True


def main():
    """Run all tests."""
    print("SPL Data Models Test Suite")
    print("=" * 40)
    
    try:
        # Run all tests
        test_basic_models()
        test_validation()
        test_xml_utils()
        test_section_type_mapping()
        test_document_methods()
        
        print("\n" + "=" * 40)
        print("[OK] All tests completed successfully!")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)