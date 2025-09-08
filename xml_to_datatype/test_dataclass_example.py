#!/usr/bin/env python3
"""
Example usage of the SPL dataclass converter.
Demonstrates how to work with the typed dataclass objects.
"""

from spl_xml_to_dataclass import SPLXMLToDataclassConverter
from spl_data_types import SPLDocument, ManufacturedProduct, Ingredient
import json
from pathlib import Path


def main():
    """Demonstrate usage of the SPL dataclass converter."""
    
    # Convert XML to dataclass
    converter = SPLXMLToDataclassConverter("example.xml")
    spl_doc = converter.convert()
    
    print("=== SPL Document Information ===")
    print(f"Document ID: {spl_doc.document.id.root}")
    print(f"Document Title: {spl_doc.document.title.text}")
    print(f"Document Code: {spl_doc.document.code.displayName}")
    print(f"Effective Time: {spl_doc.document.effectiveTime.value}")
    
    if spl_doc.author and spl_doc.author.assignedEntity:
        org = spl_doc.author.assignedEntity.representedOrganization
        print(f"Author Organization: {org.name}")
    
    print(f"\n=== Products ({len(spl_doc.manufactured_products)}) ===")
    for i, product in enumerate(spl_doc.manufactured_products):
        print(f"\nProduct {i+1}:")
        if product.name:
            print(f"  Name: {product.name.text}")
        if product.code:
            print(f"  Code: {product.code.code}")
        if product.formCode:
            print(f"  Form: {product.formCode.displayName}")
        
        print(f"  Generic Medicine: {', '.join(product.genericMedicine)}")
        print(f"  Ingredients: {len(product.ingredients)}")
        print(f"  Packaging Options: {len(product.packaging)}")
        
        # Show active ingredients
        active_ingredients = [ing for ing in product.ingredients if ing.classCode == "ACTIM"]
        if active_ingredients:
            print("  Active Ingredients:")
            for ing in active_ingredients:
                if ing.ingredientSubstance and ing.ingredientSubstance.name:
                    name = ing.ingredientSubstance.name
                    if ing.quantity and ing.quantity.numerator:
                        amount = f"{ing.quantity.numerator.value} {ing.quantity.numerator.unit}"
                        print(f"    - {name}: {amount}")
                    else:
                        print(f"    - {name}")
        
        # Show characteristics
        if product.subjectOf and product.subjectOf.characteristics:
            print("  Characteristics:")
            for char in product.subjectOf.characteristics:
                char_name = char.code.code.replace('SPL', '').lower()
                if char.value:
                    if char.value.displayName:
                        print(f"    - {char_name}: {char.value.displayName}")
                    elif char.value.value:
                        unit = f" {char.value.unit}" if char.value.unit else ""
                        print(f"    - {char_name}: {char.value.value}{unit}")
                    elif char.value.text:
                        print(f"    - {char_name}: {char.value.text}")
    
    print(f"\n=== Text Sections ({len(spl_doc.free_text_sections)}) ===")
    for section in spl_doc.free_text_sections:
        if section.title:
            print(f"- {section.title}")
        elif section.code and section.code.displayName:
            print(f"- {section.code.displayName}")
        
        # Show list items if present
        if section.list_items:
            for item in section.list_items[:2]:  # Show first 2 items
                print(f"  • {item}")
            if len(section.list_items) > 2:
                print(f"  ... and {len(section.list_items) - 2} more items")
    
    print(f"\n=== Media ({len(spl_doc.observation_media)}) ===")
    for media in spl_doc.observation_media:
        print(f"- {media.ID}: {media.text}")
        if media.value:
            print(f"  Type: {media.value.mediaType}, Reference: {media.value.reference}")
    
    # Demonstrate JSON serialization
    print(f"\n=== JSON Serialization ===")
    json_str = spl_doc.to_json(indent=2)
    print(f"JSON Length: {len(json_str):,} characters")
    
    # Demonstrate JSON deserialization
    spl_doc_from_json = SPLDocument.from_json(json_str)
    print(f"Deserialized document ID: {spl_doc_from_json.document.id.root}")
    
    # Save to file
    output_file = Path("example_typed.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json_str)
    print(f"Saved to: {output_file}")
    
    # Demonstrate type safety
    print(f"\n=== Type Safety Example ===")
    # This demonstrates that the objects are properly typed
    first_product = spl_doc.manufactured_products[0] if spl_doc.manufactured_products else None
    if first_product:
        print(f"Type of first product: {type(first_product).__name__}")
        print(f"Has ingredients attribute: {hasattr(first_product, 'ingredients')}")
        print(f"Ingredients is a list: {isinstance(first_product.ingredients, list)}")
        
        if first_product.ingredients:
            first_ingredient = first_product.ingredients[0]
            print(f"First ingredient type: {type(first_ingredient).__name__}")
            print(f"First ingredient class code: {first_ingredient.classCode}")


if __name__ == "__main__":
    main()