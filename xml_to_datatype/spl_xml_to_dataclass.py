#!/usr/bin/env python3
"""
FDA SPL XML to Dataclass Converter

This script converts FDA Structured Product Labeling (SPL) XML documents
to typed Python dataclass objects and then to JSON format.
"""

import xml.etree.ElementTree as ET
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse

from spl_data_types import (
    SPLDocument, Document, Author, ManufacturedProduct, ObservationMedia,
    FreeTextSection, Notes, SourceFile, Identifier, CodedElement, TextContent,
    TimeValue, Quantity, QuantityPart, Ingredient, IngredientSubstance,
    ActiveMoiety, Packaging, ContainerPackagedProduct, SubjectOf, Approval,
    MarketingAct, Characteristic, CharacteristicValue, ConsumedIn,
    SubstanceAdministration, AuthorAssignedEntity, RepresentedOrganization,
    AssignedEntity, StatusCode, ApprovalAuthor, TerritorialAuthority,
    ObservationMediaValue, SectionComponent
)


class SPLXMLToDataclassConverter:
    """Converts FDA SPL XML documents to typed dataclass objects."""
    
    # HL7 v3 namespace
    NS = {'hl7': 'urn:hl7-org:v3'}
    
    def __init__(self, xml_file_path: str):
        self.xml_file_path = Path(xml_file_path)
        self.tree = ET.parse(xml_file_path)
        self.root = self.tree.getroot()
        
    def convert(self) -> SPLDocument:
        """Main conversion method that returns the complete SPL document."""
        source_file = SourceFile(
            path=str(self.xml_file_path.absolute()),
            filecite=f"file://{self.xml_file_path.absolute()}"
        )
        
        return SPLDocument(
            source_file=source_file,
            document=self._extract_document_info(),
            author=self._extract_author_info(),
            manufactured_products=self._extract_manufactured_products(),
            observation_media=self._extract_observation_media(),
            free_text_sections=self._extract_free_text_sections(),
            notes=Notes(
                mapping_rules="XML to dataclass conversion preserving all structural information and content",
                sources_consulted=[
                    "FDA SPL Implementation Guide",
                    "HL7 CDA R2 Specification",
                    "SPL Schema Definition (spl.xsd)"
                ]
            ),
            original_xml_file=source_file
        )
    
    def _extract_document_info(self) -> Document:
        """Extract main document information."""
        # Document ID (required)
        id_elem = self.root.find('hl7:id', self.NS)
        doc_id = Identifier(root=id_elem.get('root')) if id_elem is not None else Identifier(root="")
        
        # Document code (required)
        code_elem = self.root.find('hl7:code', self.NS)
        doc_code = self._extract_coded_element(code_elem) if code_elem is not None else CodedElement(code="")
        
        # Title (required)
        title_elem = self.root.find('hl7:title', self.NS)
        doc_title = self._extract_text_content(title_elem) if title_elem is not None else TextContent()
        
        # Optional fields
        effective_time = None
        effective_time_elem = self.root.find('hl7:effectiveTime', self.NS)
        if effective_time_elem is not None:
            effective_time = self._extract_time_value(effective_time_elem)
        
        set_id = None
        set_id_elem = self.root.find('hl7:setId', self.NS)
        if set_id_elem is not None:
            set_id = Identifier(root=set_id_elem.get('root'))
        
        version_number = None
        version_elem = self.root.find('hl7:versionNumber', self.NS)
        if version_elem is not None:
            version_number = TimeValue(value=version_elem.get('value'))
        
        return Document(
            id=doc_id,
            code=doc_code,
            title=doc_title,
            effectiveTime=effective_time,
            setId=set_id,
            versionNumber=version_number
        )
    
    def _extract_author_info(self) -> Optional[Author]:
        """Extract author information."""
        author_elem = self.root.find('hl7:author', self.NS)
        if author_elem is None:
            return None
        
        assigned_entity = None
        assigned_entity_elem = author_elem.find('hl7:assignedEntity', self.NS)
        if assigned_entity_elem is not None:
            rep_org_elem = assigned_entity_elem.find('hl7:representedOrganization', self.NS)
            if rep_org_elem is not None:
                # Organization ID
                org_id = None
                org_id_elem = rep_org_elem.find('hl7:id', self.NS)
                if org_id_elem is not None:
                    org_id = Identifier(
                        root=org_id_elem.get('root'),
                        extension=org_id_elem.get('extension')
                    )
                
                # Organization name
                org_name = None
                org_name_elem = rep_org_elem.find('hl7:name', self.NS)
                if org_name_elem is not None:
                    org_name = org_name_elem.text
                
                rep_org = RepresentedOrganization(
                    id=org_id,
                    name=org_name,
                    assignedEntity=AssignedEntity()
                )
                
                assigned_entity = AuthorAssignedEntity(representedOrganization=rep_org)
        
        return Author(time=None, assignedEntity=assigned_entity)
    
    def _extract_manufactured_products(self) -> List[ManufacturedProduct]:
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
                product = self._extract_complete_manufactured_product(outer_product_elem)
                if product:
                    products.append(product)
        
        return products
    
    def _extract_complete_manufactured_product(self, outer_product_elem: ET.Element) -> Optional[ManufacturedProduct]:
        """Extract complete manufactured product from nested XML structure."""
        # Look for inner manufacturedProduct element that contains the core product details
        inner_product_elem = outer_product_elem.find('hl7:manufacturedProduct', self.NS)
        if inner_product_elem is None:
            # If no nested element, treat the outer element as the product
            inner_product_elem = outer_product_elem
        
        # Extract core product information from inner element
        product = self._extract_single_manufactured_product(inner_product_elem)
        if product is None:
            return None
        
        # Extract subjectOf information from outer element (characteristics, approvals, etc.)
        outer_subject_of = self._extract_subject_of(outer_product_elem)
        if outer_subject_of:
            # Merge with any existing subjectOf from inner element
            if product.subjectOf:
                # Merge the lists
                product.subjectOf.approvals.extend(outer_subject_of.approvals)
                product.subjectOf.marketingActs.extend(outer_subject_of.marketingActs)
                product.subjectOf.characteristics.extend(outer_subject_of.characteristics)
            else:
                product.subjectOf = outer_subject_of
        
        # Extract consumedIn information from outer element
        outer_consumed_in = None
        consumed_in_elem = outer_product_elem.find('hl7:consumedIn', self.NS)
        if consumed_in_elem is not None:
            substance_admin_elem = consumed_in_elem.find('hl7:substanceAdministration', self.NS)
            if substance_admin_elem is not None:
                route_code_elem = substance_admin_elem.find('hl7:routeCode', self.NS)
                if route_code_elem is not None:
                    route_code = self._extract_coded_element(route_code_elem)
                    outer_consumed_in = ConsumedIn(
                        substanceAdministration=SubstanceAdministration(routeCode=route_code)
                    )
        
        # Use outer consumedIn if available, otherwise keep inner
        if outer_consumed_in:
            product.consumedIn = outer_consumed_in
        
        return product
    
    def _extract_single_manufactured_product(self, product_elem: ET.Element) -> Optional[ManufacturedProduct]:
        """Extract information for a single manufactured product."""
        # Product code
        code = None
        code_elem = product_elem.find('hl7:code', self.NS)
        if code_elem is not None:
            code = self._extract_coded_element(code_elem)
        
        # Product name
        name = None
        name_elem = product_elem.find('hl7:name', self.NS)
        if name_elem is not None:
            name = self._extract_text_content(name_elem)
        
        # Form code
        form_code = None
        form_code_elem = product_elem.find('hl7:formCode', self.NS)
        if form_code_elem is not None:
            form_code = self._extract_coded_element(form_code_elem)
        
        # Generic medicine
        generic_medicines = []
        generic_elems = product_elem.findall('.//hl7:genericMedicine/hl7:name', self.NS)
        for generic_elem in generic_elems:
            if generic_elem.text:
                generic_medicines.append(generic_elem.text)
        
        # Ingredients
        ingredients = self._extract_ingredients(product_elem)
        
        # Packaging
        packaging = self._extract_packaging(product_elem)
        
        # Subject of (approvals, marketing acts, characteristics)
        # Note: This will be merged with outer subjectOf in _extract_complete_manufactured_product
        subject_of = self._extract_subject_of(product_elem)
        
        # Note: consumedIn is handled at the outer level in _extract_complete_manufactured_product
        
        return ManufacturedProduct(
            code=code,
            name=name,
            formCode=form_code,
            genericMedicine=generic_medicines,
            ingredients=ingredients,
            packaging=packaging,
            subjectOf=subject_of,
            consumedIn=None  # Will be set by _extract_complete_manufactured_product
        )
    
    def _extract_ingredients(self, product_elem: ET.Element) -> List[Ingredient]:
        """Extract ingredient information."""
        ingredients = []
        
        ingredient_elems = product_elem.findall('hl7:ingredient', self.NS)
        for ingredient_elem in ingredient_elems:
            class_code = ingredient_elem.get('classCode', '')
            
            # Quantity (for active ingredients)
            quantity = None
            quantity_elem = ingredient_elem.find('hl7:quantity', self.NS)
            if quantity_elem is not None:
                quantity = self._extract_quantity(quantity_elem)
            
            # Ingredient substance
            ingredient_substance = None
            substance_elem = ingredient_elem.find('hl7:ingredientSubstance', self.NS)
            if substance_elem is not None:
                # Code
                code = None
                code_elem = substance_elem.find('hl7:code', self.NS)
                if code_elem is not None:
                    code = self._extract_coded_element(code_elem)
                
                # Name
                name = None
                name_elem = substance_elem.find('hl7:name', self.NS)
                if name_elem is not None and name_elem.text:
                    name = name_elem.text
                
                # Active moiety
                active_moieties = []
                moiety_elems = substance_elem.findall('.//hl7:activeMoiety', self.NS)
                for moiety_elem in moiety_elems:
                    moiety_code_elem = moiety_elem.find('hl7:code', self.NS)
                    moiety_name_elem = moiety_elem.find('hl7:name', self.NS)
                    if moiety_code_elem is not None and moiety_name_elem is not None:
                        moiety_code = self._extract_coded_element(moiety_code_elem)
                        active_moieties.append(ActiveMoiety(
                            code=moiety_code,
                            name=moiety_name_elem.text
                        ))
                
                ingredient_substance = IngredientSubstance(
                    code=code,
                    name=name,
                    activeMoiety=active_moieties
                )
            
            ingredients.append(Ingredient(
                classCode=class_code,
                quantity=quantity,
                ingredientSubstance=ingredient_substance
            ))
        
        return ingredients
    
    def _extract_packaging(self, product_elem: ET.Element) -> List[Packaging]:
        """Extract packaging information."""
        packaging = []
        
        content_elems = product_elem.findall('hl7:asContent', self.NS)
        for content_elem in content_elems:
            # Quantity
            quantity = None
            quantity_elem = content_elem.find('hl7:quantity', self.NS)
            if quantity_elem is not None:
                quantity = self._extract_quantity(quantity_elem)
            
            # Container packaged product
            container_product = None
            container_elem = content_elem.find('hl7:containerPackagedProduct', self.NS)
            if container_elem is not None:
                code = None
                code_system = None
                code_elem = container_elem.find('hl7:code', self.NS)
                if code_elem is not None:
                    code = code_elem.get('code')
                    code_system = code_elem.get('codeSystem')
                
                form_code = None
                form_code_elem = container_elem.find('hl7:formCode', self.NS)
                if form_code_elem is not None:
                    form_code = self._extract_coded_element(form_code_elem)
                
                container_product = ContainerPackagedProduct(
                    code=code,
                    codeSystem=code_system,
                    formCode=form_code
                )
            
            packaging.append(Packaging(
                quantity=quantity,
                containerPackagedProduct=container_product
            ))
        
        return packaging
    
    def _extract_subject_of(self, product_elem: ET.Element) -> Optional[SubjectOf]:
        """Extract subjectOf information (approvals, marketing acts, characteristics)."""
        approvals = []
        marketing_acts = []
        characteristics = []
        
        # Approvals
        approval_elems = product_elem.findall('hl7:subjectOf/hl7:approval', self.NS)
        for approval_elem in approval_elems:
            approval_id = None
            id_elem = approval_elem.find('hl7:id', self.NS)
            if id_elem is not None:
                approval_id = Identifier(
                    root=id_elem.get('root'),
                    extension=id_elem.get('extension')
                )
            
            code = None
            code_elem = approval_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                code = self._extract_coded_element(code_elem)
            
            author = None
            author_elem = approval_elem.find('hl7:author', self.NS)
            if author_elem is not None:
                territory_elem = author_elem.find('.//hl7:territory/hl7:code', self.NS)
                if territory_elem is not None:
                    territory_code = self._extract_coded_element(territory_elem)
                    author = ApprovalAuthor(
                        territorialAuthority=TerritorialAuthority(territory=territory_code)
                    )
            
            approvals.append(Approval(id=approval_id, code=code, author=author))
        
        # Marketing acts
        marketing_elems = product_elem.findall('hl7:subjectOf/hl7:marketingAct', self.NS)
        for marketing_elem in marketing_elems:
            code = None
            code_elem = marketing_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                code = self._extract_coded_element(code_elem)
            
            status_code = None
            status_elem = marketing_elem.find('hl7:statusCode', self.NS)
            if status_elem is not None:
                status_code = StatusCode(code=status_elem.get('code'))
            
            effective_time = None
            effective_time_elem = marketing_elem.find('hl7:effectiveTime', self.NS)
            if effective_time_elem is not None:
                effective_time = self._extract_time_value(effective_time_elem)
            
            marketing_acts.append(MarketingAct(
                code=code,
                statusCode=status_code,
                effectiveTime=effective_time
            ))
        
        # Characteristics
        char_elems = product_elem.findall('hl7:subjectOf/hl7:characteristic', self.NS)
        for char_elem in char_elems:
            code = None
            code_elem = char_elem.find('hl7:code', self.NS)
            if code_elem is not None:
                code = self._extract_coded_element(code_elem)
            
            value = None
            value_elem = char_elem.find('hl7:value', self.NS)
            if value_elem is not None:
                xsi_type = value_elem.get('{http://www.w3.org/2001/XMLSchema-instance}type', '')
                
                if xsi_type == 'CE':
                    coded_value = self._extract_coded_element(value_elem)
                    value = CharacteristicValue(
                        xsi_type=xsi_type,
                        code=coded_value.code,
                        codeSystem=coded_value.codeSystem,
                        displayName=coded_value.displayName
                    )
                elif xsi_type == 'PQ':
                    value = CharacteristicValue(
                        xsi_type=xsi_type,
                        value=value_elem.get('value'),
                        unit=value_elem.get('unit')
                    )
                elif xsi_type == 'ST':
                    value = CharacteristicValue(
                        xsi_type=xsi_type,
                        text=value_elem.text
                    )
                elif xsi_type == 'INT':
                    value = CharacteristicValue(
                        xsi_type=xsi_type,
                        value=value_elem.get('value')
                    )
                else:
                    value = CharacteristicValue(xsi_type=xsi_type)
            
            if code:
                characteristics.append(Characteristic(code=code, value=value))
        
        if approvals or marketing_acts or characteristics:
            return SubjectOf(
                approvals=approvals,
                marketingActs=marketing_acts,
                characteristics=characteristics
            )
        
        return None
    
    def _extract_observation_media(self) -> List[ObservationMedia]:
        """Extract observation media (images, etc.)."""
        media_list = []
        
        media_elems = self.root.findall('.//hl7:observationMedia', self.NS)
        for media_elem in media_elems:
            media_id = media_elem.get('ID', '')
            
            text = None
            text_elem = media_elem.find('hl7:text', self.NS)
            if text_elem is not None and text_elem.text:
                text = text_elem.text
            
            value = None
            value_elem = media_elem.find('hl7:value', self.NS)
            if value_elem is not None:
                xsi_type = value_elem.get('{http://www.w3.org/2001/XMLSchema-instance}type', '')
                media_type = value_elem.get('mediaType', '')
                
                reference = ''
                ref_elem = value_elem.find('hl7:reference', self.NS)
                if ref_elem is not None:
                    reference = ref_elem.get('value', '')
                
                value = ObservationMediaValue(
                    xsi_type=xsi_type,
                    mediaType=media_type,
                    reference=reference
                )
            
            media_list.append(ObservationMedia(
                ID=media_id,
                text=text,
                value=value
            ))
        
        return media_list
    
    def _extract_free_text_sections(self) -> List[FreeTextSection]:
        """Extract free text sections from the document."""
        sections = []
        
        # Find all sections in the structured body
        section_elems = self.root.findall('.//hl7:section', self.NS)
        
        for section_elem in section_elems:
            # Skip the DLDE section as it's handled separately
            if section_elem.get('ID') == 'DLDE':
                continue
            
            section = self._extract_single_section(section_elem)
            if section:
                sections.append(section)
        
        return sections
    
    def _extract_single_section(self, section_elem: ET.Element) -> Optional[FreeTextSection]:
        """Extract information from a single section."""
        # Section ID
        id_root = None
        id_elem = section_elem.find('hl7:id', self.NS)
        if id_elem is not None:
            id_root = id_elem.get('root')
        
        # Section code
        code = None
        code_elem = section_elem.find('hl7:code', self.NS)
        if code_elem is not None:
            code = self._extract_coded_element(code_elem)
        
        # Title
        title = None
        title_elem = section_elem.find('hl7:title', self.NS)
        if title_elem is not None and title_elem.text:
            title = title_elem.text
        
        # Text content
        text_html = None
        text_plain = None
        list_items = []
        media_reference = None
        
        text_elem = section_elem.find('hl7:text', self.NS)
        if text_elem is not None:
            # Get raw HTML
            text_html = ET.tostring(text_elem, encoding='unicode', method='html')
            
            # Extract plain text
            text_plain = self._extract_plain_text(text_elem)
            
            # Extract list items if present
            list_items = self._extract_list_items(text_elem)
            
            # Check for media references
            media_refs = text_elem.findall('.//hl7:renderMultiMedia', self.NS)
            for media_ref in media_refs:
                ref_obj = media_ref.get('referencedObject')
                if ref_obj:
                    media_reference = ref_obj
                    break
        
        # Effective time
        effective_time = None
        effective_time_elem = section_elem.find('hl7:effectiveTime', self.NS)
        if effective_time_elem is not None:
            effective_time = self._extract_time_value(effective_time_elem)
        
        # Handle nested components (like warnings subsections)
        components = []
        component_elems = section_elem.findall('hl7:component/hl7:section', self.NS)
        for comp_elem in component_elems:
            comp_code = None
            comp_code_elem = comp_elem.find('hl7:code', self.NS)
            if comp_code_elem is not None:
                comp_code = self._extract_coded_element(comp_code_elem)
            
            comp_text_plain = None
            comp_text_html = None
            comp_list_items = []
            
            comp_text_elem = comp_elem.find('hl7:text', self.NS)
            if comp_text_elem is not None:
                comp_text_plain = self._extract_plain_text(comp_text_elem)
                comp_text_html = ET.tostring(comp_text_elem, encoding='unicode', method='html')
                comp_list_items = self._extract_list_items(comp_text_elem)
            
            components.append(SectionComponent(
                code=comp_code,
                text_plain=comp_text_plain,
                text_html=comp_text_html,
                list_items=comp_list_items
            ))
        
        return FreeTextSection(
            id_root=id_root,
            code=code,
            title=title,
            text_html=text_html,
            text_plain=text_plain,
            list_items=list_items,
            components=components,
            media_reference=media_reference,
            effectiveTime=effective_time
        )
    
    def _extract_coded_element(self, elem: ET.Element) -> CodedElement:
        """Extract code, codeSystem, and displayName from a coded element."""
        return CodedElement(
            code=elem.get('code', ''),
            codeSystem=elem.get('codeSystem'),
            displayName=elem.get('displayName')
        )
    
    def _extract_text_content(self, elem: ET.Element) -> TextContent:
        """Extract text content with HTML and plain text versions."""
        # Raw HTML content
        raw_html = ET.tostring(elem, encoding='unicode', method='html')
        # Remove the outer tag
        inner_html = re.sub(r'^<[^>]+>|</[^>]+>$', '', raw_html)
        
        # Plain text content
        plain_text = self._extract_plain_text(elem)
        
        # Check for suffix elements
        suffix = None
        suffix_elem = elem.find('hl7:suffix', self.NS)
        if suffix_elem is not None and suffix_elem.text:
            suffix = suffix_elem.text
        
        return TextContent(html=inner_html, text=plain_text, suffix=suffix)
    
    def _extract_time_value(self, elem: ET.Element) -> TimeValue:
        """Extract time value information."""
        value = elem.get('value')
        
        low = None
        low_elem = elem.find('hl7:low', self.NS)
        if low_elem is not None:
            low = TimeValue(value=low_elem.get('value'))
        
        return TimeValue(value=value, low=low)
    
    def _extract_quantity(self, elem: ET.Element) -> Quantity:
        """Extract quantity information."""
        numerator = None
        numerator_elem = elem.find('hl7:numerator', self.NS)
        if numerator_elem is not None:
            translation = None
            translation_elem = numerator_elem.find('hl7:translation', self.NS)
            if translation_elem is not None:
                translation = self._extract_coded_element(translation_elem)
            
            numerator = QuantityPart(
                value=numerator_elem.get('value', ''),
                unit=numerator_elem.get('unit'),
                translation=translation
            )
        
        denominator = None
        denominator_elem = elem.find('hl7:denominator', self.NS)
        if denominator_elem is not None:
            denominator = QuantityPart(
                value=denominator_elem.get('value', ''),
                unit=denominator_elem.get('unit')
            )
        
        return Quantity(numerator=numerator, denominator=denominator)
    
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
    parser = argparse.ArgumentParser(description='Convert FDA SPL XML to typed dataclass and JSON')
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
        output_file = Path(args.xml_file).with_suffix('_dataclass.json')
    
    try:
        # Convert XML to dataclass
        converter = SPLXMLToDataclassConverter(args.xml_file)
        spl_document = converter.convert()
        
        # Convert to JSON and write output
        if args.pretty:
            json_content = spl_document.to_json(indent=2)
        else:
            json_content = spl_document.to_json()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(json_content)
        
        print(f"Successfully converted '{args.xml_file}' to '{output_file}'")
        print(f"Document type: {type(spl_document).__name__}")
        print(f"Products found: {len(spl_document.manufactured_products)}")
        print(f"Text sections: {len(spl_document.free_text_sections)}")
        print(f"Media items: {len(spl_document.observation_media)}")
        
    except Exception as e:
        print(f"Error converting file: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()