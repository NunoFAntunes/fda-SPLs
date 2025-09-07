#!/usr/bin/env python3
"""
Verbose Single SPL Document Test Script

This script processes a single SPL XML file and provides extremely detailed
output showing every field mapping and extraction result. Perfect for
understanding how the parsing pipeline works and validating extraction quality.

Usage:
    python test_single_verbose.py <path_to_spl_xml_file>
    
Example:
    python test_single_verbose.py ../test_data/extracted/dm_spl_release_human_rx_part5/prescription/20250424_c0dc0656-3c46-43f9-adea-5285c522cef8/c0dc0656-3c46-43f9-adea-5285c522cef8.xml
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import json

# Add parse directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models import *
from extraction_pipeline import ExtractionPipeline, PipelineConfiguration
from spl_document_parser import parse_spl_file


def print_separator(title: str, level: int = 1):
    """Print a formatted separator with title."""
    if level == 1:
        print(f"\n{'='*80}")
        print(f"  {title.upper()}")
        print(f"{'='*80}")
    elif level == 2:
        print(f"\n{'-'*60}")
        print(f"  {title}")
        print(f"{'-'*60}")
    else:
        print(f"\n>>> {title}")


def print_field(field_name: str, value: Any, indent: int = 0):
    """Print a field with proper formatting and type information."""
    prefix = "  " * indent
    if value is None:
        print(f"{prefix}{field_name}: NULL")
    elif isinstance(value, str):
        if len(value) > 100:
            print(f"{prefix}{field_name}: (string, {len(value)} chars) '{value[:100]}...'")
        else:
            print(f"{prefix}{field_name}: (string) '{value}'")
    elif isinstance(value, (int, float)):
        print(f"{prefix}{field_name}: ({type(value).__name__}) {value}")
    elif isinstance(value, bool):
        print(f"{prefix}{field_name}: (boolean) {value}")
    elif isinstance(value, list):
        print(f"{prefix}{field_name}: (list, {len(value)} items)")
        if value:
            print(f"{prefix}  └─ First item type: {type(value[0]).__name__}")
    elif isinstance(value, Enum):
        print(f"{prefix}{field_name}: (enum) {value.value}")
    else:
        print(f"{prefix}{field_name}: ({type(value).__name__}) {str(value)[:100]}")


def print_coded_concept(concept: CodedConcept, name: str = "Coded Concept", indent: int = 1):
    """Print coded concept details."""
    print_field(f"{name} Code", concept.code, indent)
    print_field(f"{name} System", concept.code_system, indent)
    print_field(f"{name} Display", concept.display_name, indent)


def print_quantity(quantity: Quantity, name: str = "Quantity", indent: int = 1):
    """Print quantity details."""
    print_field(f"{name} Numerator Value", quantity.numerator_value, indent)
    print_field(f"{name} Numerator Unit", quantity.numerator_unit, indent)
    print_field(f"{name} Denominator Value", quantity.denominator_value, indent)
    print_field(f"{name} Denominator Unit", quantity.denominator_unit, indent)


def print_organization(org: Organization, name: str = "Organization", indent: int = 1):
    """Print organization details."""
    print_field(f"{name} ID Extension", org.id_extension, indent)
    print_field(f"{name} ID Root", org.id_root, indent)
    print_field(f"{name} Name", org.name, indent)


def print_ingredient(ingredient: Ingredient, name: str = "Ingredient", indent: int = 1):
    """Print ingredient details."""
    print_field(f"{name} Type", ingredient.type.value if ingredient.type else None, indent)
    print_field(f"{name} Substance Name", ingredient.substance_name, indent)
    
    if ingredient.substance_code:
        print(f"{'  ' * indent}{name} Substance Code:")
        print_coded_concept(ingredient.substance_code, "Substance", indent + 1)
    
    if ingredient.quantity:
        print(f"{'  ' * indent}{name} Quantity:")
        print_quantity(ingredient.quantity, "Amount", indent + 1)
    
    if ingredient.active_moiety:
        print(f"{'  ' * indent}{name} Active Moiety:")
        print_ingredient(ingredient.active_moiety, "Moiety", indent + 1)


def print_package_info(package: PackageInfo, indent: int = 1):
    """Print package information."""
    if package.code:
        print(f"{'  ' * indent}Package Code:")
        print_coded_concept(package.code, "Code", indent + 1)
    
    if package.form_code:
        print(f"{'  ' * indent}Package Form Code:")
        print_coded_concept(package.form_code, "Form", indent + 1)
    
    if package.quantity:
        print(f"{'  ' * indent}Package Quantity:")
        print_quantity(package.quantity, "Quantity", indent + 1)


def print_marketing_info(marketing: MarketingInfo, indent: int = 1):
    """Print marketing information."""
    if marketing.marketing_code:
        print(f"{'  ' * indent}Marketing Code:")
        print_coded_concept(marketing.marketing_code, "Marketing", indent + 1)
    
    print_field("Status Code", marketing.status_code, indent)
    print_field("Effective Date Low", marketing.effective_date_low, indent)
    print_field("Effective Date High", marketing.effective_date_high, indent)


def print_approval_info(approval: ApprovalInfo, indent: int = 1):
    """Print approval information."""
    print_field("Approval ID", approval.approval_id, indent)
    
    if approval.approval_type:
        print(f"{'  ' * indent}Approval Type:")
        print_coded_concept(approval.approval_type, "Type", indent + 1)
    
    print_field("Territory Code", approval.territory_code, indent)


def print_manufactured_product(product: ManufacturedProduct, indent: int = 0):
    """Print manufactured product details."""
    print_separator("MANUFACTURED PRODUCT DETAILS", 3)
    
    if product.product_code:
        print("Product Code:")
        print_coded_concept(product.product_code, "Product", 1)
    
    print_field("Product Name", product.product_name, indent)
    print_field("Product Name Suffix", product.product_name_suffix, indent)
    print_field("Generic Name", product.generic_name, indent)
    
    if product.form_code:
        print("Form Code:")
        print_coded_concept(product.form_code, "Form", 1)
    
    print(f"\nIngredients ({len(product.ingredients)} total):")
    for i, ingredient in enumerate(product.ingredients):
        print(f"  Ingredient #{i+1}:")
        print_ingredient(ingredient, f"Ingredient_{i+1}", 2)
    
    if product.package_info:
        print("\nPackage Information:")
        print_package_info(product.package_info, 1)
    
    if product.marketing_info:
        print("\nMarketing Information:")
        print_marketing_info(product.marketing_info, 1)
    
    if product.approval_info:
        print("\nApproval Information:")
        print_approval_info(product.approval_info, 1)
    
    if product.routes_of_administration:
        print(f"\nRoutes of Administration ({len(product.routes_of_administration)} total):")
        for i, route in enumerate(product.routes_of_administration):
            if route.route_code:
                print(f"  Route #{i+1}:")
                print_coded_concept(route.route_code, "Route", 2)


def print_spl_section(section: SPLSection, depth: int = 0):
    """Print SPL section details recursively."""
    indent_str = "  " * depth
    print(f"\n{indent_str}SECTION: {section.section_id}")
    
    if section.section_code:
        print(f"{indent_str}Section Code:")
        print_coded_concept(section.section_code, "Code", depth + 1)
    
    print_field("Section Type", section.section_type.value if section.section_type else None, depth)
    print_field("Title", section.title, depth)
    print_field("Effective Time", section.effective_time, depth)
    
    if section.text_content:
        text_preview = section.text_content[:200] + "..." if len(section.text_content) > 200 else section.text_content
        print_field("Text Content", f"({len(section.text_content)} chars) {text_preview}", depth)
    
    if section.manufactured_product:
        print(f"{indent_str}Contains Manufactured Product:")
        print_manufactured_product(section.manufactured_product, depth + 1)
    
    if section.media_references:
        print(f"{indent_str}Media References ({len(section.media_references)} total):")
        for i, media in enumerate(section.media_references):
            print(f"{indent_str}  Media #{i+1}:")
            print_field("Media ID", media.media_id, depth + 2)
            print_field("Media Type", media.media_type, depth + 2)
            print_field("Reference Value", media.reference_value, depth + 2)
            print_field("Description", media.description, depth + 2)
    
    if section.subsections:
        print(f"{indent_str}Subsections ({len(section.subsections)} total):")
        for i, subsection in enumerate(section.subsections):
            print(f"{indent_str}  Subsection #{i+1}:")
            print_spl_section(subsection, depth + 2)


def print_document_analysis(doc: SPLDocument):
    """Print detailed document analysis."""
    print_separator("DOCUMENT ANALYSIS", 2)
    
    # Basic counts
    total_sections = len(doc.sections)
    manufactured_products = doc.get_manufactured_products()
    active_ingredients = doc.get_active_ingredients()
    
    print(f"Total Sections: {total_sections}")
    print(f"Manufactured Products: {len(manufactured_products)}")
    print(f"Active Ingredients: {len(active_ingredients)}")
    
    # Section type distribution
    section_types = {}
    for section in doc.sections:
        if section.section_type:
            section_types[section.section_type.value] = section_types.get(section.section_type.value, 0) + 1
    
    if section_types:
        print("\nSection Type Distribution:")
        for section_type, count in sorted(section_types.items()):
            print(f"  {section_type}: {count}")
    
    # Ingredient analysis
    if active_ingredients:
        print("\nActive Ingredients Summary:")
        for i, ingredient in enumerate(active_ingredients):
            print(f"  {i+1}. {ingredient.substance_name or 'Unknown'}")
            if ingredient.substance_code:
                print(f"      Code: {ingredient.substance_code.code} ({ingredient.substance_code.code_system})")
    
    # Processing errors
    if doc.processing_errors:
        print(f"\nProcessing Errors ({len(doc.processing_errors)}):")
        for i, error in enumerate(doc.processing_errors):
            print(f"  {i+1}. {error}")


def analyze_single_spl_file(file_path: str):
    """Analyze a single SPL file with verbose output."""
    
    print_separator("VERBOSE SPL DOCUMENT ANALYSIS")
    print(f"File: {file_path}")
    print(f"Analysis Time: {datetime.now().isoformat()}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return
    
    # Get file info
    file_stat = os.stat(file_path)
    print(f"File Size: {file_stat.st_size:,} bytes")
    print(f"Modified: {datetime.fromtimestamp(file_stat.st_mtime).isoformat()}")
    
    try:
        print_separator("PHASE 1: XML PARSING", 2)
        print("Attempting to parse SPL XML file...")
        
        # Parse the document
        result = parse_spl_file(file_path)
        
        if not result.success:
            print(f"PARSING FAILED: {result.error_message}")
            if result.validation_errors:
                print("\nValidation Errors:")
                for error in result.validation_errors:
                    print(f"  - {error}")
            return
        
        doc = result.document
        print("XML parsing successful!")
        print(f"Document parsed with {len(doc.processing_errors)} processing errors")
        
        print_separator("DOCUMENT METADATA", 2)
        print_field("Document ID", doc.document_id)
        print_field("Set ID", doc.set_id)
        print_field("Version Number", doc.version_number)
        print_field("Effective Time", doc.effective_time)
        print_field("Processed At", doc.processed_at)
        
        if doc.document_code:
            print("Document Code:")
            print_coded_concept(doc.document_code, "Document", 1)
        
        if doc.author:
            print_separator("DOCUMENT AUTHOR", 3)
            print_field("Author Time", doc.author.time)
            print(f"Organizations ({len(doc.author.organizations)} total):")
            for i, org in enumerate(doc.author.organizations):
                print(f"  Organization #{i+1}:")
                print_organization(org, f"Org_{i+1}", 2)
        
        print_separator("DOCUMENT SECTIONS", 2)
        print(f"Total Sections: {len(doc.sections)}")
        
        for i, section in enumerate(doc.sections):
            print(f"\n--- SECTION #{i+1} ---")
            print_spl_section(section)
        
        # Document analysis
        print_document_analysis(doc)
        
        print_separator("PHASE 2: PIPELINE PROCESSING", 2)
        print("Running through extraction pipeline...")
        
        # Run through extraction pipeline for additional processing
        config = PipelineConfiguration(
            enable_validation=True,
            max_workers=1,
            use_multiprocessing=False
        )
        
        pipeline = ExtractionPipeline(config)
        
        # Process single document
        pipeline_result = pipeline.process_files([file_path])
        
        print(f"Pipeline processing complete!")
        print(f"Success Rate: {pipeline_result.metrics.success_rate:.1f}%")
        print(f"Documents Processed: {pipeline_result.metrics.total_files}")
        print(f"Processing Time: {pipeline_result.metrics.processing_time:.2f}s")
        
        if pipeline_result.processing_errors:
            print(f"\nPipeline Errors ({len(pipeline_result.processing_errors)}):")
            for error in pipeline_result.processing_errors:
                print(f"  - {error}")
        
        print_separator("EXTRACTION VALIDATION", 2)
        
        # Validate key extractions
        products = doc.get_manufactured_products()
        ingredients = doc.get_active_ingredients()
        
        print(f"Extracted {len(products)} manufactured product(s)")
        print(f"Extracted {len(ingredients)} active ingredient(s)")
        
        # Check for common section types
        common_sections = [
            SectionType.SPL_LISTING,
            SectionType.ACTIVE_INGREDIENT,
            SectionType.WARNINGS,
            SectionType.INDICATIONS_USAGE
        ]
        
        for section_type in common_sections:
            sections = doc.get_sections_by_type(section_type)
            if sections:
                print(f"Found {len(sections)} section(s) of type {section_type.value}")
                text = doc.get_section_text_by_type(section_type)
                if text:
                    preview = text[:100] + "..." if len(text) > 100 else text
                    print(f"  Text preview: {preview}")
        
        print_separator("SUMMARY & RECOMMENDATIONS", 2)
        
        print("Data Quality Assessment:")
        quality_score = 0
        total_checks = 0
        
        # Check document completeness
        total_checks += 1
        if doc.document_id and doc.set_id:
            print("[PASS] Document identifiers present")
            quality_score += 1
        else:
            print("[FAIL] Missing document identifiers")
        
        total_checks += 1
        if doc.author and doc.author.organizations:
            print("[PASS] Author information present")
            quality_score += 1
        else:
            print("[FAIL] Missing author information")
        
        total_checks += 1
        if products:
            print("[PASS] Manufactured products found")
            quality_score += 1
        else:
            print("[FAIL] No manufactured products found")
        
        total_checks += 1
        if ingredients:
            print("[PASS] Active ingredients found")
            quality_score += 1
        else:
            print("[FAIL] No active ingredients found")
        
        total_checks += 1
        if len(doc.sections) >= 3:
            print("[PASS] Multiple sections present")
            quality_score += 1
        else:
            print("[FAIL] Very few sections found")
        
        quality_percentage = (quality_score / total_checks) * 100
        print(f"\nOverall Quality Score: {quality_score}/{total_checks} ({quality_percentage:.1f}%)")
        
        if quality_percentage >= 80:
            print("[EXCELLENT] Document is well-structured and complete")
        elif quality_percentage >= 60:
            print("[GOOD] Document has most required elements")
        elif quality_percentage >= 40:
            print("[FAIR] Document has some missing elements")
        else:
            print("[POOR] Document has significant structural issues")
        
        print(f"\nNext Steps:")
        print("1. This document can proceed to Phase 3 (Normalization)")
        print("2. Consider implementing text extraction for clinical sections")
        print("3. Add ICD-10 mapping for adverse effects and conditions")
        print("4. Implement dosing information extraction")
        
    except Exception as e:
        print(f"ERROR: Unexpected error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) != 2:
        print("Usage: python test_single_verbose.py <path_to_spl_xml_file>")
        print("\nExample:")
        print("python test_single_verbose.py ../test_data/extracted/dm_spl_release_human_rx_part5/prescription/20250424_c0dc0656-3c46-43f9-adea-5285c522cef8/c0dc0656-3c46-43f9-adea-5285c522cef8.xml")
        sys.exit(1)
    
    file_path = sys.argv[1]
    analyze_single_spl_file(file_path)


if __name__ == "__main__":
    main()