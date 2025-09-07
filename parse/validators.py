"""
Validation utilities for SPL data models.
Provides comprehensive validation for parsed SPL documents and their components.
"""

from typing import List, Optional, Set, Tuple, Dict, Any
import re
from datetime import datetime
import logging

from models import (
    SPLDocument, SPLSection, ManufacturedProduct, Ingredient, 
    CodedConcept, Quantity, Organization, SectionType, IngredientType
)


class ValidationError:
    """Represents a validation error with severity and context."""
    
    def __init__(self, message: str, severity: str = "error", field: Optional[str] = None, context: Optional[str] = None):
        self.message = message
        self.severity = severity  # "error", "warning", "info"
        self.field = field
        self.context = context
        self.timestamp = datetime.now()
    
    def __str__(self) -> str:
        context_str = f" in {self.context}" if self.context else ""
        field_str = f" (field: {self.field})" if self.field else ""
        return f"[{self.severity.upper()}]{context_str}{field_str}: {self.message}"


class ValidationResult:
    """Contains the result of validation with errors and warnings."""
    
    def __init__(self):
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationError] = []
        self.info: List[ValidationError] = []
    
    def add_error(self, message: str, field: Optional[str] = None, context: Optional[str] = None):
        """Add a validation error."""
        self.errors.append(ValidationError(message, "error", field, context))
    
    def add_warning(self, message: str, field: Optional[str] = None, context: Optional[str] = None):
        """Add a validation warning."""
        self.warnings.append(ValidationError(message, "warning", field, context))
    
    def add_info(self, message: str, field: Optional[str] = None, context: Optional[str] = None):
        """Add validation info."""
        self.info.append(ValidationError(message, "info", field, context))
    
    def is_valid(self) -> bool:
        """Return True if no errors exist."""
        return len(self.errors) == 0
    
    def get_all_messages(self) -> List[ValidationError]:
        """Get all validation messages."""
        return self.errors + self.warnings + self.info
    
    def __str__(self) -> str:
        messages = []
        if self.errors:
            messages.append(f"Errors: {len(self.errors)}")
        if self.warnings:
            messages.append(f"Warnings: {len(self.warnings)}")
        if self.info:
            messages.append(f"Info: {len(self.info)}")
        return f"ValidationResult({', '.join(messages)})"


class BaseValidator:
    """Base class for validators."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_required_field(self, value: Optional[str], field_name: str, context: str, result: ValidationResult):
        """Validate that a required field is present and not empty."""
        if not value or not value.strip():
            result.add_error(f"Required field '{field_name}' is missing or empty", field_name, context)
    
    def validate_uuid_format(self, value: Optional[str], field_name: str, context: str, result: ValidationResult):
        """Validate UUID format."""
        if not value:
            return
        
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
        if not uuid_pattern.match(value):
            result.add_error(f"Field '{field_name}' is not a valid UUID format", field_name, context)
    
    def validate_date_format(self, value: Optional[str], field_name: str, context: str, result: ValidationResult):
        """Validate date format (YYYYMMDD)."""
        if not value:
            return
        
        date_pattern = re.compile(r'^\d{8}$')
        if not date_pattern.match(value):
            result.add_warning(f"Field '{field_name}' does not match expected date format YYYYMMDD", field_name, context)
        else:
            # Try to parse as date
            try:
                datetime.strptime(value, '%Y%m%d')
            except ValueError:
                result.add_error(f"Field '{field_name}' is not a valid date", field_name, context)


class CodedConceptValidator(BaseValidator):
    """Validator for CodedConcept instances."""
    
    # Known code systems
    KNOWN_CODE_SYSTEMS = {
        "2.16.840.1.113883.6.1",     # LOINC
        "2.16.840.1.113883.6.69",    # NDC
        "2.16.840.1.113883.3.26.1.1", # NCI Thesaurus
        "2.16.840.1.113883.4.9",     # FDA UNII
        "2.16.840.1.113883.5.28",    # ISO Country Codes
        "2.16.840.1.113883.3.150",   # FDA Application Number
    }
    
    def validate(self, concept: CodedConcept, context: str = "") -> ValidationResult:
        """Validate a CodedConcept."""
        result = ValidationResult()
        
        self.validate_required_field(concept.code, "code", context, result)
        self.validate_required_field(concept.code_system, "code_system", context, result)
        
        if concept.code_system and concept.code_system not in self.KNOWN_CODE_SYSTEMS:
            result.add_warning(f"Unknown code system: {concept.code_system}", "code_system", context)
        
        return result


class QuantityValidator(BaseValidator):
    """Validator for Quantity instances."""
    
    def validate(self, quantity: Quantity, context: str = "") -> ValidationResult:
        """Validate a Quantity."""
        result = ValidationResult()
        
        if quantity.numerator_value <= 0:
            result.add_error("Numerator value must be positive", "numerator_value", context)
        
        if quantity.denominator_value <= 0:
            result.add_error("Denominator value must be positive", "denominator_value", context)
        
        if not quantity.numerator_unit:
            result.add_warning("Numerator unit is missing", "numerator_unit", context)
        
        return result


class IngredientValidator(BaseValidator):
    """Validator for Ingredient instances."""
    
    def validate(self, ingredient: Ingredient, context: str = "") -> ValidationResult:
        """Validate an Ingredient."""
        result = ValidationResult()
        
        if not ingredient.substance_name:
            result.add_error("Ingredient substance name is required", "substance_name", context)
        
        if ingredient.substance_code:
            coded_validator = CodedConceptValidator()
            coded_result = coded_validator.validate(ingredient.substance_code, f"{context}.substance_code")
            result.errors.extend(coded_result.errors)
            result.warnings.extend(coded_result.warnings)
        
        if ingredient.type == IngredientType.ACTIVE and not ingredient.quantity:
            result.add_warning("Active ingredient missing quantity information", "quantity", context)
        
        if ingredient.quantity:
            quantity_validator = QuantityValidator()
            quantity_result = quantity_validator.validate(ingredient.quantity, f"{context}.quantity")
            result.errors.extend(quantity_result.errors)
            result.warnings.extend(quantity_result.warnings)
        
        return result


class ManufacturedProductValidator(BaseValidator):
    """Validator for ManufacturedProduct instances."""
    
    def validate(self, product: ManufacturedProduct, context: str = "") -> ValidationResult:
        """Validate a ManufacturedProduct."""
        result = ValidationResult()
        
        if not product.product_name:
            result.add_error("Product name is required", "product_name", context)
        
        if not product.ingredients:
            result.add_warning("Product has no ingredients", "ingredients", context)
        else:
            # Validate each ingredient
            ingredient_validator = IngredientValidator()
            for i, ingredient in enumerate(product.ingredients):
                ingredient_result = ingredient_validator.validate(ingredient, f"{context}.ingredients[{i}]")
                result.errors.extend(ingredient_result.errors)
                result.warnings.extend(ingredient_result.warnings)
            
            # Check for at least one active ingredient
            active_ingredients = [ing for ing in product.ingredients if ing.type == IngredientType.ACTIVE]
            if not active_ingredients:
                result.add_warning("Product has no active ingredients", "ingredients", context)
        
        if product.product_code:
            coded_validator = CodedConceptValidator()
            coded_result = coded_validator.validate(product.product_code, f"{context}.product_code")
            result.errors.extend(coded_result.errors)
            result.warnings.extend(coded_result.warnings)
        
        return result


class SPLSectionValidator(BaseValidator):
    """Validator for SPLSection instances."""
    
    def validate(self, section: SPLSection, context: str = "") -> ValidationResult:
        """Validate an SPLSection."""
        result = ValidationResult()
        
        self.validate_required_field(section.section_id, "section_id", context, result)
        self.validate_uuid_format(section.section_id, "section_id", context, result)
        
        if section.effective_time:
            self.validate_date_format(section.effective_time, "effective_time", context, result)
        
        if section.section_code:
            coded_validator = CodedConceptValidator()
            coded_result = coded_validator.validate(section.section_code, f"{context}.section_code")
            result.errors.extend(coded_result.errors)
            result.warnings.extend(coded_result.warnings)
        
        # Manufactured product validation is now handled at document level
        
        # Validate subsections recursively
        for i, subsection in enumerate(section.subsections):
            subsection_result = self.validate(subsection, f"{context}.subsections[{i}]")
            result.errors.extend(subsection_result.errors)
            result.warnings.extend(subsection_result.warnings)
        
        return result


class SPLDocumentValidator(BaseValidator):
    """Validator for complete SPL documents."""
    
    def validate(self, document: SPLDocument) -> ValidationResult:
        """Validate an SPL document."""
        result = ValidationResult()
        context = "SPLDocument"
        
        # Validate required document fields
        self.validate_required_field(document.document_id, "document_id", context, result)
        self.validate_required_field(document.set_id, "set_id", context, result)
        self.validate_required_field(document.version_number, "version_number", context, result)
        
        # Validate UUIDs
        self.validate_uuid_format(document.document_id, "document_id", context, result)
        self.validate_uuid_format(document.set_id, "set_id", context, result)
        
        # Validate effective time
        if document.effective_time:
            self.validate_date_format(document.effective_time, "effective_time", context, result)
        
        # Validate document code
        if document.document_code:
            coded_validator = CodedConceptValidator()
            coded_result = coded_validator.validate(document.document_code, f"{context}.document_code")
            result.errors.extend(coded_result.errors)
            result.warnings.extend(coded_result.warnings)
        
        # Validate sections
        if not document.sections:
            result.add_warning("Document has no sections", "sections", context)
        else:
            section_validator = SPLSectionValidator()
            section_ids = set()
            
            for i, section in enumerate(document.sections):
                section_context = f"{context}.sections[{i}]"
                section_result = section_validator.validate(section, section_context)
                result.errors.extend(section_result.errors)
                result.warnings.extend(section_result.warnings)
                
                # Check for duplicate section IDs
                if section.section_id in section_ids:
                    result.add_error(f"Duplicate section ID: {section.section_id}", "section_id", section_context)
                else:
                    section_ids.add(section.section_id)
        
        # Business logic validations
        self._validate_business_rules(document, result)
        
        return result
    
    def _validate_business_rules(self, document: SPLDocument, result: ValidationResult):
        """Validate business-specific rules for SPL documents."""
        context = "SPLDocument.business_rules"
        
        # Check for required section types in drug products
        required_sections = {SectionType.ACTIVE_INGREDIENT, SectionType.WARNINGS}
        from base_parser import SectionTypeMapper
        found_sections = set()
        for section in document.sections:
            if section.section_code and section.section_code.code:
                section_type = SectionTypeMapper.get_section_type(section.section_code.code)
                if section_type:
                    found_sections.add(section_type)
        
        missing_sections = required_sections - found_sections
        if missing_sections:
            missing_names = [section.value for section in missing_sections]
            result.add_warning(f"Missing recommended sections: {', '.join(missing_names)}", "sections", context)
        
        # Check for products
        products = document.get_manufactured_products()
        if not products:
            result.add_warning("Document contains no manufactured products", "manufactured_products", context)
        
        # Check for active ingredients
        active_ingredients = document.get_active_ingredients()
        if not active_ingredients:
            result.add_warning("Document contains no active ingredients", "active_ingredients", context)


class ValidationSummary:
    """Provides summary statistics for validation results."""
    
    @staticmethod
    def generate_summary(result: ValidationResult) -> Dict[str, Any]:
        """Generate a summary of validation results."""
        return {
            "is_valid": result.is_valid(),
            "error_count": len(result.errors),
            "warning_count": len(result.warnings),
            "info_count": len(result.info),
            "total_issues": len(result.get_all_messages()),
            "errors": [str(error) for error in result.errors],
            "warnings": [str(warning) for warning in result.warnings],
            "info": [str(info) for info in result.info]
        }
    
    @staticmethod
    def print_summary(result: ValidationResult, title: str = "Validation Summary"):
        """Print a formatted validation summary."""
        print(f"\n{title}")
        print("=" * len(title))
        print(f"Valid: {'✓' if result.is_valid() else '✗'}")
        print(f"Errors: {len(result.errors)}")
        print(f"Warnings: {len(result.warnings)}")
        print(f"Info: {len(result.info)}")
        
        if result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  • {error}")
        
        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"  • {warning}")
        
        if result.info:
            print("\nInfo:")
            for info in result.info:
                print(f"  • {info}")