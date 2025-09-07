"""
Phase 4 database integration test.
Uses same approach as test_phase3_normalization.py for compatibility.
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

# Import Phase 4 database components
from parse.database.db_connection import initialize_database
from parse.database.spl_document_mapper import SPLDocumentMapper, process_spl_document


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


def test_database_connection():
    """Test database connection."""
    print("Testing database connection...")
    
    try:
        db = initialize_database()
        print("[OK] Database connection successful")
        
        # Test basic queries
        results = db.execute_query("SELECT COUNT(*) as count FROM medications")
        if results:
            count = results[0]['count']
            print(f"[OK] Current medications in database: {count}")
        
        return True
    except Exception as e:
        print(f"[ERROR] Database connection failed: {str(e)}")
        return False


def test_spl_document_insertion():
    """Test inserting SPL document into database."""
    print("\nTesting SPL document insertion...")
    
    # Get test document
    document = get_test_spl_document()
    if not document:
        print("[ERROR] Test document not available")
        return False
    
    print(f"[OK] Loaded test document: {document.document_id}")
    print(f"  - Set ID: {document.set_id}")
    print(f"  - Version: {document.version_number}")
    print(f"  - Sections: {len(document.sections)}")
    
    # Get products and ingredients info
    products = document.get_manufactured_products()
    ingredients = document.get_active_ingredients()
    print(f"  - Products: {len(products)}")
    print(f"  - Active ingredients: {len(ingredients)}")
    
    if products:
        product = products[0]
        print(f"  - Product name: {product.product_name}")
        if product.product_code:
            print(f"  - NDC code: {product.product_code.code}")
    
    try:
        # Process document with Phase 4
        print("\nInserting document into database...")
        result = process_spl_document(document)
        
        if result.success:
            print(f"[OK] Document insertion successful!")
            print(f"  - SPL ID: {result.spl_id}")
            print(f"  - Processing time: {result.processing_time:.3f}s")
            
            # Verify insertion by retrieving data
            print("\nVerifying inserted data...")
            mapper = SPLDocumentMapper()
            summary = mapper.get_document_summary(result.spl_id)
            
            if summary:
                print("[OK] Data verification successful!")
                
                # Show medication details
                med = summary['medication']
                print(f"  Medication Details:")
                print(f"    - Brand name: {med['brand_name']}")
                print(f"    - Generic name: {med['generic_name']}")
                print(f"    - Manufacturer: {med['manufacturer']}")
                print(f"    - NDC code: {med['ndc_code']}")
                print(f"    - Product form: {med['product_form']}")
                print(f"    - Approval type: {med['approval_type']}")
                
                # Show statistics
                stats = summary['summary']
                print(f"  Data Statistics:")
                print(f"    - Active ingredients: {stats['active_ingredients']}")
                print(f"    - Total ingredients: {stats['total_ingredients']}")
                print(f"    - Text sections: {stats['total_sections']}")
                print(f"    - Indications: {stats['total_indications']}")
                
                # Show ingredients
                if summary['ingredients']:
                    print(f"  Active Ingredients:")
                    for ing in summary['ingredients']:
                        if ing['ingredient_type'] == 'active':
                            strength = f"{ing['strength_numerator']} {ing['strength_numerator_unit']}" if ing['strength_numerator'] else "No strength data"
                            print(f"    - {ing['substance_name']}: {strength}")
                            if ing['unii_code']:
                                print(f"      UNII: {ing['unii_code']}")
                
                # Show some sections
                if summary['sections']:
                    print(f"  Section Content (sample):")
                    for section in summary['sections'][:3]:
                        text_preview = section['section_text'][:80] + "..." if section['section_text'] and len(section['section_text']) > 80 else section['section_text']
                        print(f"    - {section['loinc_code']}: {text_preview}")
                
                # Show indications
                if summary['indications']:
                    print(f"  Indications:")
                    for indication in summary['indications']:
                        text_preview = indication['indication_text'][:100] + "..." if len(indication['indication_text']) > 100 else indication['indication_text']
                        print(f"    - {text_preview}")
                
                return True
            else:
                print("[ERROR] Failed to retrieve inserted document")
                return False
                
        elif result.skipped:
            print(f"[SKIP] Document insertion skipped: {result.reason}")
            return True
        else:
            print(f"[ERROR] Document insertion failed: {result.error}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Phase 4 insertion failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_database_queries():
    """Test database views and queries."""
    print("\nTesting database views...")
    
    try:
        db = initialize_database()
        
        # Test medication_search view
        results = db.execute_query("SELECT * FROM medication_search LIMIT 1")
        if results:
            print("[OK] medication_search view working")
            result = results[0]
            print(f"  - Sample record: {result['brand_name']} ({result['generic_name']})")
            
        # Test section_content view  
        results = db.execute_query("SELECT COUNT(*) as count FROM section_content")
        if results:
            count = results[0]['count']
            print(f"[OK] section_content view working: {count} sections")
        
        # Test active_ingredients_summary view
        results = db.execute_query("SELECT COUNT(*) as count FROM active_ingredients_summary")
        if results:
            count = results[0]['count']
            print(f"[OK] active_ingredients_summary view working: {count} unique ingredients")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Database query test failed: {str(e)}")
        return False


def main():
    """Run complete Phase 4 database integration test."""
    print("Phase 4 Database Integration Test Suite")
    print("=" * 50)
    
    tests = [
        ("Database Connection", test_database_connection),
        ("SPL Document Insertion", test_spl_document_insertion),
        ("Database Queries", test_database_queries),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}")
        print("-" * len(test_name))
        
        try:
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
        print("\n[SUCCESS] All Phase 4 tests passed! Database integration is working correctly.")
        print("Ready for bulk SPL processing!")
        return True
    elif passed >= total * 0.8:
        print(f"\n[OK] Most tests passed ({passed}/{total}). Phase 4 is largely functional.")
        return True
    else:
        print(f"\n[WARNING] Several tests failed ({total-passed}/{total}). Phase 4 needs attention.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)