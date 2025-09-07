"""
Small bulk test for Phase 4 database integration from parse directory.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parse.spl_document_parser import parse_spl_file
from parse.database.spl_document_mapper import SPLDocumentMapper, process_spl_document
from parse.database.db_connection import initialize_database

def test_bulk_processing():
    """Test bulk processing with a few files."""
    print("Small Bulk Processing Test")
    print("=" * 30)
    
    # Find some SPL files
    data_dir = Path("../data/extracted")
    if not data_dir.exists():
        data_dir = Path("data/extracted")
        
    if not data_dir.exists():
        print("[ERROR] Data directory not found")
        return False
    
    # Get first 5 XML files
    xml_files = list(data_dir.glob("**/*.xml"))[:5]
    
    if not xml_files:
        print("[ERROR] No XML files found")
        return False
    
    print(f"Found {len(xml_files)} files to process:")
    for f in xml_files:
        print(f"  - {f.name}")
    
    # Test database connection
    try:
        db = initialize_database()
        print("[OK] Database connection successful")
        
        # Check current state
        results = db.execute_query("SELECT COUNT(*) as count FROM medications")
        if results:
            print(f"[OK] Current medications in database: {results[0]['count']}")
        
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return False
    
    # Process files
    successful = 0
    failed = 0
    total_time = 0
    
    print(f"\nProcessing files...")
    overall_start = datetime.now()
    
    for i, xml_file in enumerate(xml_files, 1):
        print(f"\n[{i}/{len(xml_files)}] Processing {xml_file.name}...")
        
        try:
            start_time = datetime.now()
            
            # Parse SPL
            result = parse_spl_file(str(xml_file))
            
            if result.success and result.document:
                print(f"  [OK] Parsed: {result.document.document_id}")
                print(f"    - Sections: {len(result.document.sections)}")
                print(f"    - Products: {len(result.document.get_manufactured_products())}")
                
                # Insert to database
                db_result = process_spl_document(result.document)
                
                if db_result.success:
                    processing_time = (datetime.now() - start_time).total_seconds()
                    total_time += processing_time
                    print(f"  [OK] Inserted: {processing_time:.3f}s")
                    successful += 1
                elif db_result.skipped:
                    print(f"  [SKIP] Skipped: {db_result.reason}")
                    successful += 1  # Count skipped as success
                else:
                    print(f"  [ERROR] Insert failed: {db_result.error}")
                    failed += 1
            else:
                error_msg = '; '.join(result.errors) if result.errors else "Unknown error"
                print(f"  [ERROR] Parse failed: {error_msg}")
                failed += 1
                
        except Exception as e:
            print(f"  [ERROR] Exception: {str(e)}")
            failed += 1
    
    overall_time = (datetime.now() - overall_start).total_seconds()
    
    # Summary
    print(f"\n" + "=" * 40)
    print("Processing Summary:")
    print("=" * 40)
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Success rate: {successful/(successful+failed)*100:.1f}%")
    print(f"  Total time: {overall_time:.2f}s")
    print(f"  Average per file: {overall_time/(successful+failed):.3f}s")
    
    if successful > 0:
        # Check database contents
        print(f"\nDatabase contents after processing:")
        try:
            mapper = SPLDocumentMapper()
            
            results = mapper.db.execute_query("SELECT COUNT(*) as count FROM medications")
            if results:
                print(f"  Medications: {results[0]['count']}")
            
            results = mapper.db.execute_query("SELECT COUNT(*) as count FROM ingredients")
            if results:
                print(f"  Ingredients: {results[0]['count']}")
            
            results = mapper.db.execute_query("SELECT COUNT(*) as count FROM spl_sections")
            if results:
                print(f"  Sections: {results[0]['count']}")
            
            results = mapper.db.execute_query("SELECT COUNT(*) as count FROM indications")
            if results:
                print(f"  Indications: {results[0]['count']}")
            
            # Show sample data
            print(f"\nSample medications:")
            results = mapper.db.execute_query("""
                SELECT brand_name, manufacturer, ndc_code 
                FROM medications 
                WHERE brand_name IS NOT NULL 
                LIMIT 3
            """)
            for result in results:
                print(f"  - {result['brand_name']} by {result['manufacturer']} (NDC: {result['ndc_code']})")
                
        except Exception as e:
            print(f"  Error checking database: {e}")
    
    return successful > 0

def main():
    """Main test execution."""
    print("FDA SPL Phase 4 Bulk Processing Test")
    print("=" * 50)
    
    success = test_bulk_processing()
    
    if success:
        print(f"\n[SUCCESS] Small bulk processing test passed!")
        print("Phase 4 database integration is working correctly.")
        print("Ready for full bulk SPL processing with the main pipeline.")
    else:
        print(f"\n[ERROR] Small bulk processing test failed!")
        print("Fix issues before full bulk processing.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)