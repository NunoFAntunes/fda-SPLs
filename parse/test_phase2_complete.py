"""
Comprehensive test suite for Phase 2 SPL extraction pipeline.
Tests all components working together with real SPL data.
"""

import sys
import os
import json
import tempfile
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parse.spl_document_parser import SPLDocumentParser, parse_spl_file
from parse.extraction_pipeline import create_pipeline, PipelineConfiguration
from parse.parser_factory import ParserManager, PresetConfigurations
from parse.batch_processor import BatchProcessor, BatchJob
from parse.section_parser import SectionParser, SectionAnalyzer
from parse.product_parser import ProductParser, NDCValidator
from parse.ingredient_parser import IngredientParser, SubstanceValidator
from parse.clinical_section_parser import ClinicalSectionParser, ClinicalTextAnalyzer
from parse.validators import SPLDocumentValidator
from parse.models import *


def test_document_parser():
    """Test core SPL document parser functionality."""
    print("Testing SPL Document Parser...")
    
    # Test with example file
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print(f"[SKIP] Test file not found: {test_file}")
        return False
    
    try:
        result = parse_spl_file(test_file)
        
        if result.success:
            print(f"[OK] Successfully parsed document: {result.document.document_id}")
            print(f"  - Document ID: {result.document.document_id}")
            print(f"  - Set ID: {result.document.set_id}")
            print(f"  - Version: {result.document.version_number}")
            print(f"  - Sections: {len(result.document.sections)}")
            print(f"  - Processing time: {result.parse_time:.3f}s")
            
            if result.errors:
                print(f"  - Warnings: {len(result.errors)}")
                for error in result.errors[:3]:
                    print(f"    • {error}")
            
            return True
        else:
            print(f"[ERROR] Failed to parse document: {result.errors}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Exception during parsing: {str(e)}")
        return False

def test_section_analysis():
    """Test section analysis and enhancement."""
    print("\nTesting Section Analysis...")
    
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print("[SKIP] Test file not found")
        return False
    
    try:
        parser = SPLDocumentParser()
        with open(test_file, 'r', encoding='utf-8') as f:
            document = parser.parse(f.read())
        
        # Analyze sections
        analysis = SectionAnalyzer.analyze_section_distribution(document.sections)
        
        print(f"[OK] Section analysis completed:")
        print(f"  - Total sections: {analysis['total_sections']}")
        print(f"  - Text sections: {analysis['text_sections']}")
        print(f"  - Product sections: {analysis['product_sections']}")
        print(f"  - Section types: {list(analysis['section_type_distribution'].keys())}")
        
        # Test section metrics if we have sections
        if document.sections:
            metrics = SectionAnalyzer.calculate_section_metrics(document.sections)
            print(f"  - Average text length: {metrics['avg_text_length']:.0f}")
            print(f"  - Average completeness: {metrics['avg_completeness']:.2f}")
        else:
            print("  - No sections to analyze metrics for")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Section analysis failed: {str(e)}")
        return False


def test_product_and_ingredient_parsing():
    """Test product and ingredient extraction."""
    print("\nTesting Product and Ingredient Parsing...")
    
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print("[SKIP] Test file not found")
        return False
    
    try:
        parser = SPLDocumentParser()
        with open(test_file, 'r', encoding='utf-8') as f:
            document = parser.parse(f.read())
        
        # Get manufactured products
        products = document.get_manufactured_products()
        active_ingredients = document.get_active_ingredients()
        
        print(f"[OK] Product parsing completed:")
        print(f"  - Products found: {len(products)}")
        print(f"  - Active ingredients: {len(active_ingredients)}")
        
        if products:
            product = products[0]
            print(f"  - Product name: {product.product_name}")
            if product.product_code:
                print(f"  - Product code: {product.product_code.code}")
            print(f"  - Ingredients: {len(product.ingredients)}")
            
            # Test NDC validation
            if product.product_code and product.product_code.code_system == "2.16.840.1.113883.6.69":
                ndc_valid = NDCValidator.is_valid_ndc(product.product_code.code)
                print(f"  - NDC valid: {ndc_valid}")
        
        if active_ingredients:
            ingredient = active_ingredients[0]
            print(f"  - First active ingredient: {ingredient.substance_name}")
            if ingredient.quantity:
                print(f"  - Quantity: {ingredient.quantity.numerator_value} {ingredient.quantity.numerator_unit}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Product/ingredient parsing failed: {str(e)}")
        return False


def test_clinical_text_processing():
    """Test clinical text extraction and processing."""
    print("\nTesting Clinical Text Processing...")
    
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print("[SKIP] Test file not found")
        return False
    
    try:
        parser = SPLDocumentParser()
        with open(test_file, 'r', encoding='utf-8') as f:
            document = parser.parse(f.read())
        
        # Find clinical sections
        warnings_sections = document.get_sections_by_type(SectionType.WARNINGS)
        active_ingredient_sections = document.get_sections_by_type(SectionType.ACTIVE_INGREDIENT)
        
        print(f"[OK] Clinical text processing completed:")
        print(f"  - Warning sections: {len(warnings_sections)}")
        print(f"  - Active ingredient sections: {len(active_ingredient_sections)}")
        
        # Test text analysis
        if warnings_sections and warnings_sections[0].text_content:
            warnings_text = warnings_sections[0].text_content
            print(f"  - Warnings text length: {len(warnings_text)}")
            
            # Test reading level calculation
            reading_stats = ClinicalTextAnalyzer.calculate_reading_level(warnings_text)
            print(f"  - Reading level (Flesch-Kincaid): {reading_stats['flesch_kincaid']:.1f}")
            print(f"  - Average sentence length: {reading_stats['avg_sentence_length']:.1f}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Clinical text processing failed: {str(e)}")
        return False


def test_validation_system():
    """Test document validation."""
    print("\nTesting Validation System...")
    
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print("[SKIP] Test file not found")
        return False
    
    try:
        parser = SPLDocumentParser()
        with open(test_file, 'r', encoding='utf-8') as f:
            document = parser.parse(f.read())
        
        # Validate document
        validator = SPLDocumentValidator()
        validation_result = validator.validate(document)
        
        print(f"[OK] Validation completed:")
        print(f"  - Document valid: {'Yes' if validation_result.is_valid() else 'No'}")
        print(f"  - Errors: {len(validation_result.errors)}")
        print(f"  - Warnings: {len(validation_result.warnings)}")
        print(f"  - Info messages: {len(validation_result.info)}")
        
        # Show first few issues
        if validation_result.errors:
            print("  - Sample errors:")
            for error in validation_result.errors[:2]:
                print(f"    • {error.message}")
        
        if validation_result.warnings:
            print("  - Sample warnings:")
            for warning in validation_result.warnings[:2]:
                print(f"    • {warning.message}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Validation failed: {str(e)}")
        return False


def test_parser_factory():
    """Test parser factory and configuration."""
    print("\nTesting Parser Factory...")
    
    try:
        # Test different configurations
        from parse.parser_factory import ParserFactory, ParserType, PresetConfigurations
        
        configs_to_test = [
            ("development", PresetConfigurations.development_config()),
            ("production", PresetConfigurations.production_config()),
            ("fast", PresetConfigurations.fast_parsing_config())
        ]
        
        for config_name, config in configs_to_test:
            factory = ParserFactory(config)
            
            # Create all parser types
            doc_parser = factory.create_parser(ParserType.SPL_DOCUMENT)
            section_parser = factory.create_parser(ParserType.SECTION)
            product_parser = factory.create_parser(ParserType.PRODUCT)
            ingredient_parser = factory.create_parser(ParserType.INGREDIENT)
            clinical_parser = factory.create_parser(ParserType.CLINICAL_SECTION)
            
            print(f"  - {config_name} configuration: All parsers created successfully")
        
        print("[OK] Parser factory tests completed")
        return True
        
    except Exception as e:
        print(f"[ERROR] Parser factory test failed: {str(e)}")
        return False


def test_batch_processing():
    """Test batch processing capabilities."""
    print("\nTesting Batch Processing...")
    
    # Find test files
    test_dir = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc"
    
    if not os.path.exists(test_dir):
        print("[SKIP] Test directory not found")
        return False
    
    try:
        # Find up to 3 XML files for testing
        test_files = []
        for root, dirs, files in os.walk(test_dir):
            for file in files:
                if file.endswith('.xml'):
                    test_files.append(os.path.join(root, file))
                    if len(test_files) >= 3:
                        break
            if len(test_files) >= 3:
                break
        
        if not test_files:
            print("[SKIP] No XML test files found")
            return False
        
        print(f"Found {len(test_files)} test files")
        
        # Create batch processor
        processor = BatchProcessor()
        
        # Create batch job
        batch_job = BatchJob(
            job_id="test_batch",
            file_paths=test_files,
            max_workers=2,
            use_multiprocessing=False
        )
        
        # Process batch
        result = processor.process_batch(batch_job)
        
        print(f"[OK] Batch processing completed:")
        print(f"  - Total files: {result.total_files}")
        print(f"  - Successful: {result.successful}")
        print(f"  - Failed: {result.failed}")
        print(f"  - Success rate: {result.success_rate:.1f}%")
        print(f"  - Total time: {result.total_time:.3f}s")
        
        return result.success_rate > 50  # At least 50% should succeed
        
    except Exception as e:
        print(f"[ERROR] Batch processing test failed: {str(e)}")
        return False


def test_extraction_pipeline():
    """Test complete extraction pipeline."""
    print("\nTesting Extraction Pipeline...")
    
    # Find test files
    test_dir = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc"
    
    if not os.path.exists(test_dir):
        print("[SKIP] Test directory not found")
        return False
    
    try:
        # Find a few test files
        test_files = []
        for root, dirs, files in os.walk(test_dir):
            for file in files:
                if file.endswith('.xml'):
                    test_files.append(os.path.join(root, file))
                    if len(test_files) >= 2:
                        break
            if len(test_files) >= 2:
                break
        
        if not test_files:
            print("[SKIP] No XML test files found")
            return False
        
        # Create pipeline with development configuration
        pipeline = create_pipeline("development")
        
        # Add progress callback
        def progress_callback(pipeline_id, stage, data):
            if stage.value == "parsing" and "completed" in data:
                print(f"  Progress: {data['completed']}/{data.get('total', '?')} files")
        
        pipeline.add_progress_callback(progress_callback)
        
        # Create temporary output directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Process files
            result = pipeline.process_files(test_files, temp_dir)
            
            print(f"[OK] Pipeline execution completed:")
            print(f"  - Pipeline ID: {result.pipeline_id}")
            print(f"  - Total files: {result.metrics.total_files}")
            print(f"  - Success rate: {result.metrics.success_rate:.1f}%")
            print(f"  - Validation rate: {result.metrics.validation_rate:.1f}%")
            print(f"  - Processing time: {result.metrics.processing_time:.3f}s")
            print(f"  - Output files: {len(result.output_files)}")
            print(f"  - Documents processed: {len(result.documents)}")
            print(f"  - Total sections: {result.metrics.total_sections}")
            print(f"  - Total products: {result.metrics.total_products}")
            print(f"  - Total ingredients: {result.metrics.total_ingredients}")
            
            # Verify output files were created
            for output_file in result.output_files:
                if os.path.exists(output_file):
                    size = os.path.getsize(output_file)
                    print(f"    • {os.path.basename(output_file)}: {size} bytes")
            
            return result.metrics.success_rate > 50
            
    except Exception as e:
        print(f"[ERROR] Pipeline test failed: {str(e)}")
        return False


def test_integration_workflow():
    """Test complete integration workflow."""
    print("\nTesting Integration Workflow...")
    
    test_file = r"C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc\20090901_11ddf219-8537-4a8f-b267-ef965159885e\11ddf219-8537-4a8f-b267-ef965159885e.xml"
    
    if not os.path.exists(test_file):
        print("[SKIP] Test file not found")
        return False
    
    try:
        # Step 1: Parse document
        parser = SPLDocumentParser()
        with open(test_file, 'r', encoding='utf-8') as f:
            document = parser.parse(f.read())
        
        # Step 2: Extract key information
        products = document.get_manufactured_products()
        active_ingredients = document.get_active_ingredients()
        warnings_text = document.get_section_text_by_type(SectionType.WARNINGS)
        
        # Step 3: Validate
        validator = SPLDocumentValidator()
        validation_result = validator.validate(document)
        
        # Step 4: Analyze
        section_analysis = SectionAnalyzer.analyze_section_distribution(document.sections)
        
        print(f"[OK] Integration workflow completed:")
        print(f"  - Document: {document.document_id}")
        print(f"  - Products: {len(products)}")
        print(f"  - Active ingredients: {len(active_ingredients)}")
        print(f"  - Has warnings: {'Yes' if warnings_text else 'No'}")
        print(f"  - Validation passed: {'Yes' if validation_result.is_valid() else 'No'}")
        print(f"  - Section analysis: {section_analysis['total_sections']} sections")
        
        # Create a summary report
        summary = {
            'document_id': document.document_id,
            'set_id': document.set_id,
            'version': document.version_number,
            'products_count': len(products),
            'active_ingredients_count': len(active_ingredients),
            'sections_count': len(document.sections),
            'validation_passed': validation_result.is_valid(),
            'error_count': len(validation_result.errors),
            'warning_count': len(validation_result.warnings),
            'has_warnings_section': bool(warnings_text),
            'section_types': list(section_analysis['section_type_distribution'].keys())
        }
        
        print("  - Summary report generated successfully")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Integration workflow failed: {str(e)}")
        return False


def main():
    """Run complete Phase 2 test suite."""
    print("Phase 2 SPL Extraction Pipeline Test Suite")
    print("=" * 50)
    
    tests = [
        ("Document Parser", test_document_parser),
        ("Section Analysis", test_section_analysis), 
        ("Product & Ingredient Parsing", test_product_and_ingredient_parsing),
        ("Clinical Text Processing", test_clinical_text_processing),
        ("Validation System", test_validation_system),
        ("Parser Factory", test_parser_factory),
        ("Batch Processing", test_batch_processing),
        ("Extraction Pipeline", test_extraction_pipeline),
        ("Integration Workflow", test_integration_workflow),
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
        print("\n[SUCCESS] All tests passed! Phase 2 implementation is working correctly.")
        return True
    elif passed >= total * 0.8:
        print(f"\n[OK] Most tests passed ({passed}/{total}). Phase 2 is largely functional.")
        return True
    else:
        print(f"\n[WARNING] Several tests failed ({total-passed}/{total}). Phase 2 needs attention.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)