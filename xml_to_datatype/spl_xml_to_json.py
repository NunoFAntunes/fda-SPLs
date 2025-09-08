#!/usr/bin/env python3
"""
FDA SPL XML to JSON Converter

This script converts FDA Structured Product Labeling (SPL) XML documents
to a structured JSON format based on the provided schema.
"""

import xml.etree.ElementTree as ET
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import argparse


class SPLXMLToJSONConverter:
    """Converts FDA SPL XML documents to structured JSON format."""
    
    # HL7 v3 namespace
    NS = {'hl7': 'urn:hl7-org:v3'}
    
    def __init__(self, xml_file_path: str):
        self.xml_file_path = Path(xml_file_path)
        self.tree = ET.parse(xml_file_path)
        self.root = self.tree.getroot()
        
    def convert(self) -> Dict[str, Any]:
        """Main conversion method that returns the complete JSON structure."""
        return {
            "source_file": {
                "path": str(self.xml_file_path.absolute()),
                "filecite": f"file://{self.xml_file_path.absolute()}"
            },
            "document": self._extract_document_info(),
            "author": self._extract_author_info(),
            "manufactured_products": self._extract_manufactured_products(),
            "observation_media": self._extract_observation_media(),
            "free_text_sections": self._extract_free_text_sections(),
            "notes": {
                "mapping_rules": "XML to JSON conversion preserving all structural information and content",
                "sources_consulted": [
                    "FDA SPL Implementation Guide",
                    "HL7 CDA R2 Specification",
                    "SPL Schema Definition (spl.xsd)"
                ]
            },
            "original_xml_file": {
                "path": str(self.xml_file_path.absolute()),
                "filecite": f"file://{self.xml_file_path.absolute()}"
            }
        }
    
    def _extract_document_info(self) -> Dict[str, Any]:
        """Extract main document information."""
        doc_info = {}
        
        # Document ID
        id_elem = self.root.find('hl7:id', self.NS)
        if id_elem is not None:
            doc_info['id'] = {'root': id_elem.get('root')}
        
        # Document code
        code_elem = self.root.find('hl7:code', self.NS)
        if code_elem is not None:
            doc_info['code'] = self._extract_coded_element(code_elem)
        
        # Title
        title_elem = self.root.find('hl7:title', self.NS)
        if title_elem is not None:
            doc_info['title'] = self._extract_text_content(title_elem)
        
        # Effective time
        effective_time = self.root.find('hl7:effectiveTime', self.NS)
        if effective_time is not None:
            doc_info['effectiveTime'] = self._extract_time_value(effective_time)
        
        # Set ID
        set_id = self.root.find('hl7:setId', self.NS)
        if set_id is not None:
            doc_info['setId'] = {'root': set_id.get('root')}
        
        # Version number
        version_num = self.root.find('hl7:versionNumber', self.NS)
        if version_num is not None:
            doc_info['versionNumber'] = {'value': version_num.get('value')}
        
        return doc_info
    
    def _extract_author_info(self) -> Dict[str, Any]:
        """Extract author information."""
        author_elem = self.root.find('hl7:author', self.NS)
        if author_elem is None:
            return {}
        
        author_info = {'time': None}
        
        assigned_entity = author_elem.find('hl7:assignedEntity', self.NS)
        if assigned_entity is not None:
            rep_org = assigned_entity.find('hl7:representedOrganization', self.NS)
            if rep_org is not None:
                org_info = {}
                
                # Organization ID
                org_id = rep_org.find('hl7:id', self.NS)
                if org_id is not None:
                    org_info['id'] = {
                        'root': org_id.get('root'),
                        'extension': org_id.get('extension')
                    }
                
                # Organization name
                org_name = rep_org.find('hl7:name', self.NS)
                if org_name is not None:
                    org_info['name'] = org_name.text
                
                org_info['assignedEntity'] = {'assignedOrganization': {}}
                
                author_info['assignedEntity'] = {'representedOrganization': org_info}
        
        return author_info
    
    def _extract_manufactured_products(self) -> List[Dict[str, Any]]:
        """Extract manufactured product information."""
        products = []
        
        # Find manufactured products in the SPL listing data elements section
        dlde_section = self.root.find('.//hl7:section[@ID="DLDE"]', self.NS)
        if dlde_section is None:
            # Fallback: search for any subject with manufactured product
            subject_elems = self.root.findall('.//hl7:subject', self.NS)
        else:
            subject_elems = dlde_section.findall('.//hl7:subject', self.NS)
        
        for subject_elem in subject_elems:
            # Find the outer manufacturedProduct (direct child of subject)
            outer_product_elem = subject_elem.find('hl7:manufacturedProduct', self.NS)
            if outer_product_elem is not None:
                product_info = self._extract_complete_manufactured_product(outer_product_elem)
                if product_info:
                    products.append(product_info)
        
        return products
    
    def _extract_complete_manufactured_product(self, outer_product_elem: ET.Element) -> Dict[str, Any]:
        """Extract complete manufactured product from nested XML structure."""
        # Look for inner manufacturedProduct element that contains the core product details
        inner_product_elem = outer_product_elem.find('hl7:manufacturedProduct', self.NS)
        if inner_product_elem is None:
            # If no nested element, treat the outer element as the product
            inner_product_elem = outer_product_elem
        
        # Extract core product information from inner element
        product_info = self._extract_single_manufactured_product(inner_product_elem)
        if not product_info:
            return {}
        
        # Extract subjectOf information from outer element (characteristics, approvals, etc.)
        outer_subject_of = self._extract_subject_of(outer_product_elem)
        if outer_subject_of:
            # Merge with any existing subjectOf from inner element
            if 'subjectOf' in product_info and product_info['subjectOf']:
                # Merge the lists
                if 'approvals' in outer_subject_of:
                    product_info['subjectOf'].setdefault('approvals', []).extend(outer_subject_of['approvals'])
                if 'marketingActs' in outer_subject_of:
                    product_info['subjectOf'].setdefault('marketingActs', []).extend(outer_subject_of['marketingActs'])
                if 'characteristics' in outer_subject_of:
                    product_info['subjectOf'].setdefault('characteristics', []).extend(outer_subject_of['characteristics'])
            else:
                product_info['subjectOf'] = outer_subject_of
        
        # Extract consumedIn information from outer element
        consumed_in_elem = outer_product_elem.find('hl7:consumedIn', self.NS)
        if consumed_in_elem is not None:
            substance_admin = consumed_in_elem.find('hl7:substanceAdministration', self.NS)
            if substance_admin is not None:
                route_code = substance_admin.find('hl7:routeCode', self.NS)
                if route_code is not None:
                    product_info['consumedIn'] = {
                        'substanceAdministration': {
                            'routeCode': self._extract_coded_element(route_code)
                        }
                    }
        
        return product_info
    
    def _extract_single_manufactured_product(self, product_elem: ET.Element) -> Dict[str, Any]:
        """Extract information for a single manufactured product."""
        product_info = {}
        
        # Product code
        code_elem = product_elem.find('hl7:code', self.NS)
        if code_elem is not None:
            product_info['code'] = self._extract_coded_element(code_elem)
        
        # Product name
        name_elem = product_elem.find('hl7:name', self.NS)
        if name_elem is not None:
            product_info['name'] = self._extract_text_content(name_elem)
        
        # Form code
        form_code = product_elem.find('hl7:formCode', self.NS)
        if form_code is not None:
            product_info['formCode'] = self._extract_coded_element(form_code)
        
        # Generic medicine
        generic_medicines = []
        generic_elems = product_elem.findall('.//hl7:genericMedicine/hl7:name', self.NS)
        for generic_elem in generic_elems:
            if generic_elem.text:
                generic_medicines.append(generic_elem.text)
        product_info['genericMedicine'] = generic_medicines
        
        # Ingredients
        product_info['ingredients'] = self._extract_ingredients(product_elem)
        
        # Packaging
        product_info['packaging'] = self._extract_packaging(product_elem)
        
        # Subject of (approvals, marketing acts, characteristics)
        # Note: This will be merged with outer subjectOf in _extract_complete_manufactured_product
        product_info['subjectOf'] = self._extract_subject_of(product_elem)
        
        # Note: consumedIn is handled at the outer level in _extract_complete_manufactured_product
        
        return product_info
    
    def _extract_ingredients(self, product_elem: ET.Element) -> List[Dict[str, Any]]:
        """Extract ingredient information."""
        ingredients = []
        
        ingredient_elems = product_elem.findall('hl7:ingredient', self.NS)
        for ingredient_elem in ingredient_elems:
            ingredient_info = {
                'classCode': ingredient_elem.get('classCode', '')
            }
            
            # Quantity (for active ingredients)
            quantity_elem = ingredient_elem.find('hl7:quantity', self.NS)
            if quantity_elem is not None:
                ingredient_info['quantity'] = self._extract_quantity(quantity_elem)
            
            # Ingredient substance
            substance_elem = ingredient_elem.find('hl7:ingredientSubstance', self.NS)
            if substance_elem is not None:
                substance_info = {}
                
                # Code
                code_elem = substance_elem.find('hl7:code', self.NS)
                if code_elem is not None:
                    substance_info['code'] = self._extract_coded_element(code_elem)
                
                # Name
                name_elem = substance_elem.find('hl7:name', self.NS)
                if name_elem is not None and name_elem.text:
                    substance_info['name'] = name_elem.text
                
                # Active moiety
                active_moieties = []
                moiety_elems = substance_elem.findall('.//hl7:activeMoiety', self.NS)
                for moiety_elem in moiety_elems:
                    moiety_code = moiety_elem.find('hl7:code', self.NS)
                    moiety_name = moiety_elem.find('hl7:name', self.NS)
                    if moiety_code is not None and moiety_name is not None:
                        active_moieties.append({
                            'code': self._extract_coded_element(moiety_code),
                            'name': moiety_name.text
                        })
                
                if active_moieties:
                    substance_info['activeMoiety'] = active_moieties
                
                ingredient_info['ingredientSubstance'] = substance_info
            
            ingredients.append(ingredient_info)
        
        return ingredients
    
    def _extract_packaging(self, product_elem: ET.Element) -> List[Dict[str, Any]]:
        """Extract packaging information."""
        packaging = []
        
        content_elems = product_elem.findall('hl7:asContent', self.NS)
        for content_elem in content_elems:
            package_info = {}
            
            # Quantity
            quantity_elem = content_elem.find('hl7:quantity', self.NS)
            if quantity_elem is not None:
                package_info['quantity'] = self._extract_quantity(quantity_elem)
            
            # Container packaged product
            container_elem = content_elem.find('hl7:containerPackagedProduct', self.NS)
            if container_elem is not None:
                container_info = {}
                
                code_elem = container_elem.find('hl7:code', self.NS)
                if code_elem is not None:
                    container_info['code'] = code_elem.get('code')
                    container_info['codeSystem'] = code_elem.get('codeSystem')
                
                form_code = container_elem.find('hl7:formCode', self.NS)
                if form_code is not None:
                    container_info['formCode'] = self._extract_coded_element(form_code)
                
                package_info['containerPackagedProduct'] = container_info
            
            packaging.append(package_info)
        
        return packaging
    
    def _extract_subject_of(self, product_elem: ET.Element) -> Dict[str, Any]:
        """Extract subjectOf information (approvals, marketing acts, characteristics)."""
        subject_of = {}
        
        # Approvals
        approvals = []
        approval_elems = product_elem.findall('hl7:subjectOf/hl7:approval', self.NS)
        for approval_elem in approval_elems:
            approval_info = {}
            
            id_elem = approval_elem.find('hl7:id', self.NS)
            if id_elem is not None:
                approval_info['id'] = {
                    'root': id_elem.get('root'),
                    'extension': id_elem.get('extension')
                }
            
            code_elem = approval_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                approval_info['code'] = self._extract_coded_element(code_elem)
            
            author_elem = approval_elem.find('hl7:author', self.NS)
            if author_elem is not None:
                territory_elem = author_elem.find('.//hl7:territory/hl7:code', self.NS)
                if territory_elem is not None:
                    approval_info['author'] = {
                        'territorialAuthority': {
                            'territory': self._extract_coded_element(territory_elem)
                        }
                    }
            
            approvals.append(approval_info)
        
        if approvals:
            subject_of['approvals'] = approvals
        
        # Marketing acts
        marketing_acts = []
        marketing_elems = product_elem.findall('hl7:subjectOf/hl7:marketingAct', self.NS)
        for marketing_elem in marketing_elems:
            marketing_info = {}
            
            code_elem = marketing_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                marketing_info['code'] = self._extract_coded_element(code_elem)
            
            status_elem = marketing_elem.find('hl7:statusCode', self.NS)
            if status_elem is not None:
                marketing_info['statusCode'] = {'code': status_elem.get('code')}
            
            effective_time = marketing_elem.find('hl7:effectiveTime', self.NS)
            if effective_time is not None:
                marketing_info['effectiveTime'] = self._extract_time_value(effective_time)
            
            marketing_acts.append(marketing_info)
        
        if marketing_acts:
            subject_of['marketingActs'] = marketing_acts
        
        # Characteristics
        characteristics = []
        char_elems = product_elem.findall('hl7:subjectOf/hl7:characteristic', self.NS)
        for char_elem in char_elems:
            char_info = {}
            
            code_elem = char_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                char_info['code'] = self._extract_coded_element(code_elem)
            
            value_elem = char_elem.find('hl7:value', self.NS)
            if value_elem is not None:
                value_info = {'xsi_type': value_elem.get('{http://www.w3.org/2001/XMLSchema-instance}type', '')}
                
                if value_info['xsi_type'] == 'CE':
                    value_info.update(self._extract_coded_element(value_elem))
                elif value_info['xsi_type'] == 'PQ':
                    value_info['value'] = value_elem.get('value')
                    value_info['unit'] = value_elem.get('unit')
                elif value_info['xsi_type'] == 'ST':
                    value_info['text'] = value_elem.text
                elif value_info['xsi_type'] == 'INT':
                    value_info['value'] = value_elem.get('value')
                
                char_info['value'] = value_info
            
            characteristics.append(char_info)
        
        if characteristics:
            subject_of['characteristics'] = characteristics
        
        return subject_of
    
    def _extract_observation_media(self) -> List[Dict[str, Any]]:
        """Extract observation media (images, etc.)."""
        media_list = []
        
        media_elems = self.root.findall('.//hl7:observationMedia', self.NS)
        for media_elem in media_elems:
            media_info = {}
            
            media_id = media_elem.get('ID')
            if media_id:
                media_info['ID'] = media_id
            
            text_elem = media_elem.find('hl7:text', self.NS)
            if text_elem is not None and text_elem.text:
                media_info['text'] = text_elem.text
            
            value_elem = media_elem.find('hl7:value', self.NS)
            if value_elem is not None:
                value_info = {
                    'xsi_type': value_elem.get('{http://www.w3.org/2001/XMLSchema-instance}type', ''),
                    'mediaType': value_elem.get('mediaType', '')
                }
                
                ref_elem = value_elem.find('hl7:reference', self.NS)
                if ref_elem is not None:
                    value_info['reference'] = ref_elem.get('value', '')
                
                media_info['value'] = value_info
            
            media_list.append(media_info)
        
        return media_list
    
    def _extract_free_text_sections(self) -> List[Dict[str, Any]]:
        """Extract free text sections from the document."""
        sections = []
        
        # Find all sections in the structured body
        section_elems = self.root.findall('.//hl7:section', self.NS)
        
        for section_elem in section_elems:
            # Skip the DLDE section as it's handled separately
            if section_elem.get('ID') == 'DLDE':
                continue
            
            section_info = self._extract_single_section(section_elem)
            if section_info:
                sections.append(section_info)
        
        return sections
    
    def _extract_single_section(self, section_elem: ET.Element) -> Dict[str, Any]:
        """Extract information from a single section."""
        section_info = {}
        
        # Section ID
        id_elem = section_elem.find('hl7:id', self.NS)
        if id_elem is not None:
            section_info['id_root'] = id_elem.get('root')
        
        # Section code
        code_elem = section_elem.find('hl7:code', self.NS)
        if code_elem is not None:
            section_info['code'] = self._extract_coded_element(code_elem)
        
        # Title
        title_elem = section_elem.find('hl7:title', self.NS)
        if title_elem is not None and title_elem.text:
            section_info['title'] = title_elem.text
        
        # Text content
        text_elem = section_elem.find('hl7:text', self.NS)
        if text_elem is not None:
            # Get raw HTML
            raw_html = ET.tostring(text_elem, encoding='unicode', method='html')
            section_info['text_html'] = raw_html
            
            # Extract plain text
            plain_text = self._extract_plain_text(text_elem)
            section_info['text_plain'] = plain_text
            
            # Extract list items if present
            list_items = self._extract_list_items(text_elem)
            if list_items:
                section_info['list_items'] = list_items
            
            # Check for media references
            media_refs = text_elem.findall('.//hl7:renderMultiMedia', self.NS)
            for media_ref in media_refs:
                ref_obj = media_ref.get('referencedObject')
                if ref_obj:
                    section_info['media_reference'] = ref_obj
        
        # Effective time
        effective_time = section_elem.find('hl7:effectiveTime', self.NS)
        if effective_time is not None:
            section_info['effectiveTime'] = self._extract_time_value(effective_time)
        
        # Handle nested components (like warnings subsections)
        components = []
        component_elems = section_elem.findall('hl7:component/hl7:section', self.NS)
        for comp_elem in component_elems:
            comp_info = {}
            
            comp_code = comp_elem.find('hl7:code', self.NS)
            if comp_code is not None:
                comp_info['code'] = self._extract_coded_element(comp_code)
            
            comp_text = comp_elem.find('hl7:text', self.NS)
            if comp_text is not None:
                comp_info['text_plain'] = self._extract_plain_text(comp_text)
                comp_info['text_html'] = ET.tostring(comp_text, encoding='unicode', method='html')
                
                comp_list_items = self._extract_list_items(comp_text)
                if comp_list_items:
                    comp_info['list_items'] = comp_list_items
            
            components.append(comp_info)
        
        if components:
            section_info['components'] = components
        
        return section_info
    
    def _extract_coded_element(self, elem: ET.Element) -> Dict[str, str]:
        """Extract code, codeSystem, and displayName from a coded element."""
        coded = {}
        
        if elem.get('code'):
            coded['code'] = elem.get('code')
        if elem.get('codeSystem'):
            coded['codeSystem'] = elem.get('codeSystem')
        if elem.get('displayName'):
            coded['displayName'] = elem.get('displayName')
        
        return coded
    
    def _extract_text_content(self, elem: ET.Element) -> Dict[str, str]:
        """Extract text content with HTML and plain text versions."""
        content = {}
        
        # Raw HTML content
        raw_html = ET.tostring(elem, encoding='unicode', method='html')
        # Remove the outer tag
        inner_html = re.sub(r'^<[^>]+>|</[^>]+>$', '', raw_html)
        content['html'] = inner_html
        
        # Plain text content
        content['text'] = self._extract_plain_text(elem)
        
        # Check for suffix elements
        suffix_elem = elem.find('hl7:suffix', self.NS)
        if suffix_elem is not None and suffix_elem.text:
            content['suffix'] = suffix_elem.text
        
        return content
    
    def _extract_time_value(self, elem: ET.Element) -> Dict[str, Any]:
        """Extract time value information."""
        time_info = {}
        
        if elem.get('value'):
            time_info['value'] = elem.get('value')
        
        low_elem = elem.find('hl7:low', self.NS)
        if low_elem is not None:
            time_info['low'] = {'value': low_elem.get('value')}
        
        return time_info
    
    def _extract_quantity(self, elem: ET.Element) -> Dict[str, Any]:
        """Extract quantity information."""
        quantity = {}
        
        numerator = elem.find('hl7:numerator', self.NS)
        if numerator is not None:
            num_info = {
                'value': numerator.get('value'),
                'unit': numerator.get('unit')
            }
            
            # Check for translation
            translation = numerator.find('hl7:translation', self.NS)
            if translation is not None:
                num_info['translation'] = self._extract_coded_element(translation)
            
            quantity['numerator'] = num_info
        
        denominator = elem.find('hl7:denominator', self.NS)
        if denominator is not None:
            quantity['denominator'] = {
                'value': denominator.get('value'),
                'unit': denominator.get('unit')
            }
        
        return quantity
    
    def _extract_plain_text(self, elem: ET.Element) -> str:
        """Extract plain text from an element, removing HTML tags."""
        text_parts = []
        
        if elem.text:
            text_parts.append(elem.text.strip())
        
        for child in elem:
            child_text = self._extract_plain_text(child)
            if child_text:
                text_parts.append(child_text)
            
            if child.tail:
                text_parts.append(child.tail.strip())
        
        return ' '.join(text_parts).strip()
    
    def _extract_list_items(self, elem: ET.Element) -> List[str]:
        """Extract list items from text elements."""
        items = []
        
        list_elems = elem.findall('.//hl7:list', self.NS)
        for list_elem in list_elems:
            item_elems = list_elem.findall('hl7:item', self.NS)
            for item_elem in item_elems:
                item_text = self._extract_plain_text(item_elem)
                if item_text:
                    items.append(item_text)
        
        return items


def main():
    """Main function to handle command line arguments and run conversion."""
    parser = argparse.ArgumentParser(description='Convert FDA SPL XML to JSON')
    parser.add_argument('xml_file', help='Input XML file path')
    parser.add_argument('json_file', nargs='?', help='Output JSON file path (optional)')
    parser.add_argument('--pretty', action='store_true', help='Pretty print JSON output')
    
    args = parser.parse_args()
    
    # Validate input file
    if not Path(args.xml_file).exists():
        print(f"Error: Input file '{args.xml_file}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # Determine output file
    if args.json_file:
        output_file = Path(args.json_file)
    else:
        output_file = Path(args.xml_file).with_suffix('.json')
    
    try:
        # Convert XML to JSON
        converter = SPLXMLToJSONConverter(args.xml_file)
        json_data = converter.convert()
        
        # Write output
        with open(output_file, 'w', encoding='utf-8') as f:
            if args.pretty:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            else:
                json.dump(json_data, f, ensure_ascii=False)
        
        print(f"Successfully converted '{args.xml_file}' to '{output_file}'")
        
    except Exception as e:
        print(f"Error converting file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()