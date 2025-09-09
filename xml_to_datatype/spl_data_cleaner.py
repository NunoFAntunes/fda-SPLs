#!/usr/bin/env python3
"""
SPL Data Cleaning Pipeline

This module provides comprehensive cleaning and normalization for SPL documents
using the existing normalize tools. It cleans text content, normalizes dates,
standardizes units, validates NDC codes, and performs additional data cleanup.
"""

import sys
from pathlib import Path
from typing import Optional, List
import logging

# Add normalize package to path
sys.path.append(str(Path(__file__).parent.parent / 'normalize'))

from normalizers.date_normalizer import DateNormalizer
from normalizers.ndc_validator import NDCValidator
from normalizers.unit_normalizer import UnitNormalizer
from normalizers.dosage_form_normalizer import DosageFormNormalizer
from normalizers.route_normalizer import RouteNormalizer
from normalizers.strength_validator import StrengthValidator
from normalizers.duplicate_detector import DuplicateDetector
from normalizers.unii_validator import UNIIValidator
from text_processors.text_cleaner import TextCleaner

from spl_data_types import (
    SPLDocument, Document, Author, ManufacturedProduct, ObservationMedia,
    FreeTextSection, TextContent, TimeValue, Quantity, QuantityPart,
    Ingredient, IngredientSubstance, ActiveMoiety, Packaging, SubjectOf,
    Approval, MarketingAct, Characteristic, CharacteristicValue
)


class SPLDataCleaner:
    """Comprehensive cleaning pipeline for SPL documents."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def clean_spl_document(self, spl_document: SPLDocument) -> SPLDocument:
        """
        Apply comprehensive cleaning to an SPL document.
        
        Args:
            spl_document: Raw SPL document from XML conversion
            
        Returns:
            SPLDocument: Cleaned and normalized SPL document
        """
        self.logger.info("Starting SPL document cleaning pipeline")
        
        # Clean document metadata
        if spl_document.document:
            spl_document.document = self._clean_document(spl_document.document)
        
        # Clean author information
        if spl_document.author:
            spl_document.author = self._clean_author(spl_document.author)
        
        # Clean manufactured products
        cleaned_products = []
        for product in spl_document.manufactured_products:
            cleaned_product = self._clean_manufactured_product(product)
            if cleaned_product:
                cleaned_products.append(cleaned_product)
        spl_document.manufactured_products = cleaned_products
        
        # Detect and handle duplicate products
        if len(spl_document.manufactured_products) > 1:
            spl_document = self._handle_duplicate_products(spl_document)
        
        # Clean observation media
        cleaned_media = []
        for media in spl_document.observation_media:
            cleaned_media_item = self._clean_observation_media(media)
            if cleaned_media_item:
                cleaned_media.append(cleaned_media_item)
        spl_document.observation_media = cleaned_media
        
        # Clean free text sections
        cleaned_sections = []
        for section in spl_document.free_text_sections:
            cleaned_section = self._clean_free_text_section(section)
            if cleaned_section:
                cleaned_sections.append(cleaned_section)
        spl_document.free_text_sections = cleaned_sections
        
        self.logger.info("SPL document cleaning pipeline completed")
        return spl_document
    
    def _clean_document(self, document: Document) -> Document:
        """Clean document metadata."""
        # Clean title text content
        if document.title:
            document.title = self._clean_text_content(document.title)
        
        # Normalize effective time
        if document.effectiveTime:
            document.effectiveTime = self._clean_time_value(document.effectiveTime)
        
        return document
    
    def _clean_author(self, author: Author) -> Author:
        """Clean author information."""
        # Clean organization name if present
        if (author.assignedEntity and 
            author.assignedEntity.representedOrganization and 
            author.assignedEntity.representedOrganization.name):
            
            org_name = author.assignedEntity.representedOrganization.name
            cleaned_name = TextCleaner.clean_clinical_text(org_name)
            author.assignedEntity.representedOrganization.name = cleaned_name
        
        return author
    
    def _clean_manufactured_product(self, product: ManufacturedProduct) -> Optional[ManufacturedProduct]:
        """Clean manufactured product data."""
        # Clean product name
        if product.name:
            product.name = self._clean_text_content(product.name)
        
        # Normalize dosage form
        if product.formCode and product.formCode.displayName:
            normalized_form = DosageFormNormalizer.normalize_dosage_form(product.formCode.displayName)
            if normalized_form:
                product.formCode.displayName = normalized_form
                self.logger.debug(f"Normalized dosage form: {normalized_form}")
        
        # Normalize route of administration
        if (product.consumedIn and product.consumedIn.substanceAdministration and 
            product.consumedIn.substanceAdministration.routeCode):
            route_code = product.consumedIn.substanceAdministration.routeCode
            if route_code.displayName:
                normalized_route = RouteNormalizer.normalize_route(route_code.displayName)
                if normalized_route:
                    route_code.displayName = normalized_route
                    self.logger.debug(f"Normalized route: {normalized_route}")
        
        # Validate dosage form and route compatibility
        if (product.formCode and product.consumedIn and 
            product.consumedIn.substanceAdministration):
            self._validate_form_route_compatibility(product)
        
        # Clean generic medicine names
        cleaned_generic = []
        for generic_name in product.genericMedicine:
            cleaned = TextCleaner.clean_clinical_text(generic_name)
            if TextCleaner.has_meaningful_content(cleaned):
                cleaned_generic.append(cleaned)
        product.genericMedicine = cleaned_generic
        
        # Clean ingredients
        cleaned_ingredients = []
        for ingredient in product.ingredients:
            cleaned_ingredient = self._clean_ingredient(ingredient)
            if cleaned_ingredient:
                cleaned_ingredients.append(cleaned_ingredient)
        product.ingredients = cleaned_ingredients
        
        # Clean packaging
        cleaned_packaging = []
        for package in product.packaging:
            cleaned_package = self._clean_packaging(package)
            if cleaned_package:
                cleaned_packaging.append(cleaned_package)
        product.packaging = cleaned_packaging
        
        # Clean subject of data
        if product.subjectOf:
            product.subjectOf = self._clean_subject_of(product.subjectOf)
        
        return product
    
    def _clean_ingredient(self, ingredient: Ingredient) -> Optional[Ingredient]:
        """Clean ingredient data."""
        # Clean quantity
        if ingredient.quantity:
            ingredient.quantity = self._clean_quantity(ingredient.quantity)
        
        # Clean ingredient substance
        if ingredient.ingredientSubstance:
            ingredient.ingredientSubstance = self._clean_ingredient_substance(ingredient.ingredientSubstance)
        
        return ingredient
    
    def _clean_ingredient_substance(self, substance: IngredientSubstance) -> IngredientSubstance:
        """Clean ingredient substance data."""
        # Clean substance name
        if substance.name:
            cleaned_name = TextCleaner.clean_clinical_text(substance.name)
            substance.name = cleaned_name if TextCleaner.has_meaningful_content(cleaned_name) else substance.name
        
        # Validate UNII code if present
        if substance.code and substance.code.code:
            # Only validate if it looks like a UNII (10 characters, alphanumeric)
            code_clean = substance.code.code.strip().upper()
            if len(code_clean) == 10 and code_clean.isalnum():
                if UNIIValidator.validate_unii(code_clean):
                    substance.code.code = code_clean
                    self.logger.debug(f"Validated UNII: {code_clean}")
                else:
                    # Still keep the code but log for manual review
                    self.logger.debug(f"UNII format valid but may need verification: {code_clean}")
                    substance.code.code = code_clean
            else:
                # Not a UNII format, keep as-is (might be other identifier system)
                self.logger.debug(f"Non-UNII substance code: {substance.code.code}")
        
        # Clean active moieties
        cleaned_moieties = []
        for moiety in substance.activeMoiety:
            if moiety.name:
                cleaned_name = TextCleaner.clean_clinical_text(moiety.name)
                if TextCleaner.has_meaningful_content(cleaned_name):
                    moiety.name = cleaned_name
                    cleaned_moieties.append(moiety)
            else:
                cleaned_moieties.append(moiety)
        substance.activeMoiety = cleaned_moieties
        
        return substance
    
    def _clean_packaging(self, packaging: Packaging) -> Optional[Packaging]:
        """Clean packaging data."""
        # Clean quantity
        if packaging.quantity:
            packaging.quantity = self._clean_quantity(packaging.quantity)
        
        return packaging
    
    def _clean_subject_of(self, subject_of: SubjectOf) -> SubjectOf:
        """Clean subject of data (approvals, marketing acts, characteristics)."""
        # Clean marketing acts
        cleaned_marketing_acts = []
        for marketing_act in subject_of.marketingActs:
            cleaned_act = self._clean_marketing_act(marketing_act)
            if cleaned_act:
                cleaned_marketing_acts.append(cleaned_act)
        subject_of.marketingActs = cleaned_marketing_acts
        
        return subject_of
    
    def _clean_marketing_act(self, marketing_act: MarketingAct) -> Optional[MarketingAct]:
        """Clean marketing act data."""
        # Normalize effective time
        if marketing_act.effectiveTime:
            marketing_act.effectiveTime = self._clean_time_value(marketing_act.effectiveTime)
        
        return marketing_act
    
    def _clean_observation_media(self, media: ObservationMedia) -> Optional[ObservationMedia]:
        """Clean observation media data."""
        # Clean text content
        if media.text:
            cleaned_text = TextCleaner.clean_clinical_text(media.text)
            media.text = cleaned_text if TextCleaner.has_meaningful_content(cleaned_text) else media.text
        
        return media
    
    def _clean_free_text_section(self, section: FreeTextSection) -> Optional[FreeTextSection]:
        """Clean free text section data."""
        # Clean title
        if section.title:
            section.title = TextCleaner.clean_clinical_text(section.title)
        
        # Clean text content
        if section.text_plain:
            section.text_plain = TextCleaner.clean_clinical_text(section.text_plain)
        
        if section.text_html:
            section.text_html = TextCleaner.clean_clinical_text(section.text_html)
        
        # Clean list items
        cleaned_list_items = []
        for item in section.list_items:
            cleaned_item = TextCleaner.clean_clinical_text(item)
            if TextCleaner.has_meaningful_content(cleaned_item):
                cleaned_list_items.append(cleaned_item)
        section.list_items = cleaned_list_items
        
        # Clean effective time
        if section.effectiveTime:
            section.effectiveTime = self._clean_time_value(section.effectiveTime)
        
        # Only keep section if it has meaningful content
        has_content = (
            (section.text_plain and TextCleaner.has_meaningful_content(section.text_plain)) or
            (section.text_html and TextCleaner.has_meaningful_content(section.text_html)) or
            len(section.list_items) > 0
        )
        
        return section if has_content else None
    
    def _clean_text_content(self, text_content: TextContent) -> TextContent:
        """Clean TextContent object."""
        if text_content.text:
            text_content.text = TextCleaner.clean_clinical_text(text_content.text)
        
        if text_content.html:
            text_content.html = TextCleaner.clean_clinical_text(text_content.html)
        
        if text_content.suffix:
            text_content.suffix = TextCleaner.clean_clinical_text(text_content.suffix)
        
        return text_content
    
    def _clean_time_value(self, time_value: TimeValue) -> TimeValue:
        """Clean and normalize time values."""
        if time_value.value:
            normalized_date = DateNormalizer.normalize_date(time_value.value)
            if normalized_date:
                time_value.value = normalized_date
        
        if time_value.low and time_value.low.value:
            normalized_date = DateNormalizer.normalize_date(time_value.low.value)
            if normalized_date:
                time_value.low.value = normalized_date
        
        return time_value
    
    def _clean_quantity(self, quantity: Quantity) -> Quantity:
        """Clean and normalize quantity values."""
        if quantity.numerator:
            quantity.numerator = self._clean_quantity_part(quantity.numerator)
        
        if quantity.denominator:
            quantity.denominator = self._clean_quantity_part(quantity.denominator)
        
        return quantity
    
    def _clean_quantity_part(self, quantity_part: QuantityPart) -> QuantityPart:
        """Clean and normalize quantity part."""
        if quantity_part.unit:
            normalized_unit = UnitNormalizer.normalize_unit(quantity_part.unit)
            if normalized_unit:
                quantity_part.unit = normalized_unit
        
        # Validate strength values
        if quantity_part.value and quantity_part.unit:
            validation = StrengthValidator.validate_strength(
                quantity_part.value, quantity_part.unit
            )
            if not validation['is_valid']:
                for error in validation['errors']:
                    self.logger.warning(f"Strength validation error: {error}")
            if validation['warnings']:
                for warning in validation['warnings']:
                    self.logger.debug(f"Strength validation warning: {warning}")
        
        return quantity_part
    
    def _validate_and_clean_ndc(self, ndc_code: str) -> Optional[str]:
        """Validate and normalize NDC codes."""
        if not ndc_code:
            return None
        
        if NDCValidator.validate_ndc(ndc_code):
            return NDCValidator.normalize_ndc(ndc_code)
        
        return None
    
    def _remove_empty_fields(self, obj):
        """Remove empty fields from objects (generic cleanup)."""
        if hasattr(obj, '__dict__'):
            for attr_name, attr_value in list(obj.__dict__.items()):
                if attr_value is None or attr_value == "" or attr_value == []:
                    delattr(obj, attr_name)
                elif isinstance(attr_value, list):
                    # Recursively clean list items
                    cleaned_list = []
                    for item in attr_value:
                        if item is not None and item != "":
                            self._remove_empty_fields(item)
                            cleaned_list.append(item)
                    setattr(obj, attr_name, cleaned_list)
                else:
                    self._remove_empty_fields(attr_value)
        
        return obj
    
    def _validate_form_route_compatibility(self, product: ManufacturedProduct):
        """Validate compatibility between dosage form and route."""
        form_name = product.formCode.displayName if product.formCode else ""
        route_name = (product.consumedIn.substanceAdministration.routeCode.displayName 
                     if product.consumedIn and product.consumedIn.substanceAdministration 
                     and product.consumedIn.substanceAdministration.routeCode else "")
        
        if form_name and route_name:
            is_compatible = DosageFormNormalizer.get_route_dosage_compatibility(form_name, route_name)
            if not is_compatible:
                self.logger.warning(f"Incompatible dosage form '{form_name}' and route '{route_name}'")
    
    def _handle_duplicate_products(self, spl_document: SPLDocument) -> SPLDocument:
        """Detect and handle duplicate products within the document."""
        # Convert products to dict format for duplicate detection
        product_dicts = []
        for product in spl_document.manufactured_products:
            product_dict = {
                'name': product.name.text if product.name else "",
                'dosage_form': product.formCode.displayName if product.formCode else "",
                'route': (product.consumedIn.substanceAdministration.routeCode.displayName
                         if product.consumedIn and product.consumedIn.substanceAdministration
                         and product.consumedIn.substanceAdministration.routeCode else ""),
                'ingredients': []
            }
            
            for ingredient in product.ingredients:
                if ingredient.ingredientSubstance:
                    ingredient_dict = {
                        'class_code': ingredient.classCode,
                        'ingredient_substance': {
                            'name': ingredient.ingredientSubstance.name or ""
                        }
                    }
                    product_dict['ingredients'].append(ingredient_dict)
            
            product_dicts.append(product_dict)
        
        # Find duplicates
        duplicates = DuplicateDetector.find_duplicates(product_dicts)
        
        if duplicates:
            self.logger.info(f"Found {len(duplicates)} potential duplicate groups")
            
            # Handle duplicates based on confidence
            indices_to_remove = set()
            for duplicate_group in duplicates:
                recommendation = DuplicateDetector.get_deduplication_recommendation(duplicate_group)
                
                if recommendation['action'] == 'merge_or_remove':
                    # Keep the primary product, mark others for removal
                    primary_idx = recommendation.get('suggested_primary', 0)
                    group_indices = duplicate_group['indices']
                    
                    for idx in group_indices:
                        if idx != group_indices[primary_idx]:
                            indices_to_remove.add(idx)
                    
                    self.logger.info(f"Removing duplicate products, keeping primary at index {group_indices[primary_idx]}")
                
                elif recommendation['action'] == 'flag_for_review':
                    self.logger.warning(f"Potential duplicates flagged for manual review: {duplicate_group['indices']}")
            
            # Remove flagged duplicates
            if indices_to_remove:
                filtered_products = []
                for i, product in enumerate(spl_document.manufactured_products):
                    if i not in indices_to_remove:
                        filtered_products.append(product)
                
                spl_document.manufactured_products = filtered_products
                self.logger.info(f"Removed {len(indices_to_remove)} duplicate products")
        
        return spl_document


# Factory function for easy import
def create_spl_cleaner() -> SPLDataCleaner:
    """Create and return an SPL data cleaner instance."""
    return SPLDataCleaner()