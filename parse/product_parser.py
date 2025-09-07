"""
Product Information Extraction Parser.
Handles manufactured product details from SPL listing sections (LOINC 48780-1).
"""

import xml.etree.ElementTree as ET
from typing import Optional, List
import re

from base_parser import BaseParser, XMLUtils
from models import (
    ManufacturedProduct, CodedConcept, Quantity, 
    PackageInfo, MarketingInfo, ApprovalInfo, RouteOfAdministration
)


class ProductParser(BaseParser):
    """Parser for manufactured product information from SPL documents."""
    
    def __init__(self):
        super().__init__()
    
    def parse(self, subject_element: ET.Element) -> Optional[ManufacturedProduct]:
        """
        Parse manufactured product from subject element in SPL listing section.
        
        Args:
            subject_element: <subject> element containing manufacturedProduct
            
        Returns:
            ManufacturedProduct: Parsed product information or None if parsing fails
        """
        manufactured_product_element = XMLUtils.find_element(subject_element, "hl7:manufacturedProduct")
        if manufactured_product_element is None:
            self.add_error("No manufacturedProduct found in subject element")
            return None
        
        # Get the inner manufacturedProduct element
        inner_product = XMLUtils.find_element(manufactured_product_element, "hl7:manufacturedProduct")
        if inner_product is None:
            self.add_error("No inner manufacturedProduct found")
            return None
        
        product = ManufacturedProduct()
        
        # Extract product code (NDC)
        product.product_code = self._extract_product_code(inner_product)
        
        # Extract product name and suffix
        product.product_name, product.product_name_suffix = self._extract_product_name(inner_product)
        
        # Extract form code (dosage form)
        product.form_code = self._extract_form_code(inner_product)
        
        # Extract generic name
        product.generic_name = self._extract_generic_name(inner_product)
        
        # Extract ingredients (will be handled by ingredient parser)
        product.ingredients = []  # Placeholder - will be populated by ingredient parser
        
        # Extract packaging information
        product.package_info = self._extract_package_info(inner_product)
        
        # Extract marketing information
        product.marketing_info = self._extract_marketing_info(manufactured_product_element)
        
        # Extract approval information
        product.approval_info = self._extract_approval_info(manufactured_product_element)
        
        # Extract routes of administration
        product.routes_of_administration = self._extract_routes(manufactured_product_element)
        
        return product
    
    def _extract_product_code(self, product_element: ET.Element) -> Optional[CodedConcept]:
        """Extract product code (typically NDC)."""
        code_element = XMLUtils.find_element(product_element, "hl7:code")
        if code_element is not None:
            return XMLUtils.parse_coded_concept(code_element)
        return None
    
    def _extract_product_name(self, product_element: ET.Element) -> tuple[Optional[str], Optional[str]]:
        """Extract product name and suffix."""
        name_element = XMLUtils.find_element(product_element, "hl7:name")
        if name_element is None:
            return None, None
        
        # Extract main name text
        name_text = XMLUtils.get_text_content(name_element)
        
        # Extract suffix if present
        suffix_element = XMLUtils.find_element(name_element, "hl7:suffix")
        suffix_text = XMLUtils.get_text_content(suffix_element) if suffix_element else None
        
        return name_text, suffix_text
    
    def _extract_form_code(self, product_element: ET.Element) -> Optional[CodedConcept]:
        """Extract dosage form code."""
        form_code_element = XMLUtils.find_element(product_element, "hl7:formCode")
        if form_code_element is not None:
            return XMLUtils.parse_coded_concept(form_code_element)
        return None
    
    def _extract_generic_name(self, product_element: ET.Element) -> Optional[str]:
        """Extract generic medicine name."""
        generic_element = XMLUtils.find_element(product_element, "hl7:asEntityWithGeneric")
        if generic_element is None:
            return None
        
        generic_medicine = XMLUtils.find_element(generic_element, "hl7:genericMedicine")
        if generic_medicine is None:
            return None
        
        name_element = XMLUtils.find_element(generic_medicine, "hl7:name")
        return XMLUtils.get_text_content(name_element) if name_element else None
    
    def _extract_package_info(self, product_element: ET.Element) -> Optional[PackageInfo]:
        """Extract packaging information."""
        content_element = XMLUtils.find_element(product_element, "hl7:asContent")
        if content_element is None:
            return None
        
        package_info = PackageInfo()
        
        # Extract quantity
        quantity_element = XMLUtils.find_element(content_element, "hl7:quantity")
        if quantity_element is not None:
            package_info.quantity = self._parse_quantity(quantity_element)
        
        # Extract container information
        container_element = XMLUtils.find_element(content_element, "hl7:containerPackagedProduct")
        if container_element is not None:
            # Extract container code
            code_element = XMLUtils.find_element(container_element, "hl7:code")
            if code_element is not None:
                package_info.code = XMLUtils.parse_coded_concept(code_element)
            
            # Extract container form code
            form_code_element = XMLUtils.find_element(container_element, "hl7:formCode")
            if form_code_element is not None:
                package_info.form_code = XMLUtils.parse_coded_concept(form_code_element)
        
        return package_info
    
    def _extract_marketing_info(self, manufactured_product_element: ET.Element) -> Optional[MarketingInfo]:
        """Extract marketing information."""
        subject_of_elements = XMLUtils.find_all_elements(manufactured_product_element, "hl7:subjectOf")
        
        for subject_of in subject_of_elements:
            marketing_act = XMLUtils.find_element(subject_of, "hl7:marketingAct")
            if marketing_act is not None:
                marketing_info = MarketingInfo()
                
                # Extract marketing code
                code_element = XMLUtils.find_element(marketing_act, "hl7:code")
                if code_element is not None:
                    marketing_info.marketing_code = XMLUtils.parse_coded_concept(code_element)
                
                # Extract status code
                status_element = XMLUtils.find_element(marketing_act, "hl7:statusCode")
                if status_element is not None:
                    marketing_info.status_code = XMLUtils.get_attribute(status_element, "code")
                
                # Extract effective time
                effective_time = XMLUtils.find_element(marketing_act, "hl7:effectiveTime")
                if effective_time is not None:
                    low_element = XMLUtils.find_element(effective_time, "hl7:low")
                    high_element = XMLUtils.find_element(effective_time, "hl7:high")
                    
                    if low_element is not None:
                        marketing_info.effective_date_low = XMLUtils.get_attribute(low_element, "value")
                    if high_element is not None:
                        marketing_info.effective_date_high = XMLUtils.get_attribute(high_element, "value")
                
                return marketing_info
        
        return None
    
    def _extract_approval_info(self, manufactured_product_element: ET.Element) -> Optional[ApprovalInfo]:
        """Extract regulatory approval information."""
        subject_of_elements = XMLUtils.find_all_elements(manufactured_product_element, "hl7:subjectOf")
        
        for subject_of in subject_of_elements:
            approval_element = XMLUtils.find_element(subject_of, "hl7:approval")
            if approval_element is not None:
                approval_info = ApprovalInfo()
                
                # Extract approval ID
                id_element = XMLUtils.find_element(approval_element, "hl7:id")
                if id_element is not None:
                    approval_info.approval_id = XMLUtils.get_attribute(id_element, "extension")
                
                # Extract approval type code
                code_element = XMLUtils.find_element(approval_element, "hl7:code")
                if code_element is not None:
                    approval_info.approval_type = XMLUtils.parse_coded_concept(code_element)
                
                # Extract territorial authority
                author_element = XMLUtils.find_element(approval_element, "hl7:author")
                if author_element is not None:
                    territorial_auth = XMLUtils.find_element(author_element, "hl7:territorialAuthority")
                    if territorial_auth is not None:
                        territory = XMLUtils.find_element(territorial_auth, "hl7:territory")
                        if territory is not None:
                            code_element = XMLUtils.find_element(territory, "hl7:code")
                            if code_element is not None:
                                approval_info.territory_code = XMLUtils.get_attribute(code_element, "code")
                
                return approval_info
        
        return None
    
    def _extract_routes(self, manufactured_product_element: ET.Element) -> List[RouteOfAdministration]:
        """Extract routes of administration."""
        routes = []
        consumed_in_elements = XMLUtils.find_all_elements(manufactured_product_element, "hl7:consumedIn")
        
        for consumed_in in consumed_in_elements:
            substance_admin = XMLUtils.find_element(consumed_in, "hl7:substanceAdministration")
            if substance_admin is not None:
                route_code_element = XMLUtils.find_element(substance_admin, "hl7:routeCode")
                if route_code_element is not None:
                    route_code = XMLUtils.parse_coded_concept(route_code_element)
                    if route_code:
                        routes.append(RouteOfAdministration(route_code=route_code))
        
        return routes
    
    def _parse_quantity(self, quantity_element: ET.Element) -> Optional[Quantity]:
        """Parse quantity with numerator and denominator."""
        numerator_element = XMLUtils.find_element(quantity_element, "hl7:numerator")
        denominator_element = XMLUtils.find_element(quantity_element, "hl7:denominator")
        
        if numerator_element is None:
            return None
        
        # Extract numerator
        numerator_value_str = XMLUtils.get_attribute(numerator_element, "value")
        numerator_unit = XMLUtils.get_attribute(numerator_element, "unit")
        
        if not numerator_value_str:
            return None
        
        try:
            numerator_value = float(numerator_value_str)
        except ValueError:
            self.add_error(f"Invalid numerator value: {numerator_value_str}")
            return None
        
        # Extract denominator (optional)
        denominator_value = 1.0
        denominator_unit = ""
        
        if denominator_element is not None:
            denominator_value_str = XMLUtils.get_attribute(denominator_element, "value")
            denominator_unit = XMLUtils.get_attribute(denominator_element, "unit") or ""
            
            if denominator_value_str:
                try:
                    denominator_value = float(denominator_value_str)
                except ValueError:
                    self.add_error(f"Invalid denominator value: {denominator_value_str}")
                    denominator_value = 1.0
        
        return Quantity(
            numerator_value=numerator_value,
            numerator_unit=numerator_unit or "",
            denominator_value=denominator_value,
            denominator_unit=denominator_unit
        )


class NDCValidator:
    """Utility class for NDC (National Drug Code) validation and normalization."""
    
    # NDC patterns: 4-4-2, 5-3-2, 5-4-1
    NDC_PATTERNS = [
        re.compile(r'^\d{4}-\d{4}-\d{2}$'),  # 4-4-2
        re.compile(r'^\d{5}-\d{3}-\d{2}$'),  # 5-3-2  
        re.compile(r'^\d{5}-\d{4}-\d{1}$'),  # 5-4-1
    ]
    
    @classmethod
    def is_valid_ndc(cls, ndc: str) -> bool:
        """Check if NDC format is valid."""
        if not ndc:
            return False
        
        # Remove spaces and normalize
        ndc_clean = ndc.strip().replace(' ', '')
        
        # Check against known patterns
        return any(pattern.match(ndc_clean) for pattern in cls.NDC_PATTERNS)
    
    @classmethod
    def normalize_ndc(cls, ndc: str) -> Optional[str]:
        """Normalize NDC to standard format."""
        if not ndc:
            return None
        
        # Remove spaces, hyphens, and other non-digits
        digits_only = re.sub(r'[^\d]', '', ndc.strip())
        
        # NDC should be 10 or 11 digits
        if len(digits_only) == 10:
            # Convert to 5-4-1 format
            return f"{digits_only[:5]}-{digits_only[5:9]}-{digits_only[9:]}"
        elif len(digits_only) == 11:
            # Determine best format based on typical patterns
            # Most common is 5-4-2
            return f"{digits_only[:5]}-{digits_only[5:9]}-{digits_only[9:]}"
        else:
            return None
    
    @classmethod
    def extract_ndc_parts(cls, ndc: str) -> Optional[tuple[str, str, str]]:
        """Extract labeler, product, and package codes from NDC."""
        normalized = cls.normalize_ndc(ndc)
        if not normalized:
            return None
        
        parts = normalized.split('-')
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        return None