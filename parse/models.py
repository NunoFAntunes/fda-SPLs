"""
Core data models for SPL (Structured Product Labeling) document parsing.
These models represent the hierarchical structure of FDA drug labeling data.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum


class SectionType(Enum):
    """Common SPL section types based on LOINC codes."""
    SPL_LISTING = "48780-1"
    ACTIVE_INGREDIENT = "55106-9"
    PURPOSE = "55105-1"
    WARNINGS = "34071-1"
    DO_NOT_USE = "50570-1"
    ASK_DOCTOR = "50569-3"
    WHEN_USING = "50567-7"
    STOP_USE = "50566-9"
    PREGNANCY_BREASTFEEDING = "53414-9"
    KEEP_OUT_OF_REACH = "50565-1"
    INDICATIONS_USAGE = "34067-9"
    INACTIVE_INGREDIENT = "51727-6"
    UNCLASSIFIED = "42229-5"


class IngredientType(Enum):
    """Types of ingredients in pharmaceutical products."""
    ACTIVE = "ACTIM"
    INACTIVE = "IACT"


@dataclass
class CodedConcept:
    """Represents a coded concept with code, code system, and display name."""
    code: str
    code_system: str
    display_name: Optional[str] = None


@dataclass
class Quantity:
    """Represents a quantity with numerator and denominator."""
    numerator_value: float
    numerator_unit: str
    denominator_value: float = 1.0
    denominator_unit: str = ""


@dataclass
class Organization:
    """Represents an organization entity."""
    id_extension: Optional[str] = None
    id_root: Optional[str] = None
    name: Optional[str] = None


@dataclass
class Ingredient:
    """Represents a drug ingredient (active or inactive)."""
    type: IngredientType
    substance_code: Optional[CodedConcept] = None
    substance_name: Optional[str] = None
    quantity: Optional[Quantity] = None
    active_moiety: Optional['Ingredient'] = None


@dataclass
class PackageInfo:
    """Represents packaging information."""
    code: Optional[CodedConcept] = None
    form_code: Optional[CodedConcept] = None
    quantity: Optional[Quantity] = None


@dataclass
class MarketingInfo:
    """Represents drug marketing information."""
    marketing_code: Optional[CodedConcept] = None
    status_code: Optional[str] = None
    effective_date_low: Optional[str] = None
    effective_date_high: Optional[str] = None


@dataclass
class ApprovalInfo:
    """Represents regulatory approval information."""
    approval_id: Optional[str] = None
    approval_type: Optional[CodedConcept] = None
    territory_code: Optional[str] = None


@dataclass
class RouteOfAdministration:
    """Represents route of administration."""
    route_code: Optional[CodedConcept] = None


@dataclass
class ManufacturedProduct:
    """Represents a manufactured pharmaceutical product."""
    product_code: Optional[CodedConcept] = None
    product_name: Optional[str] = None
    product_name_suffix: Optional[str] = None
    form_code: Optional[CodedConcept] = None
    generic_name: Optional[str] = None
    ingredients: List[Ingredient] = field(default_factory=list)
    package_info: Optional[PackageInfo] = None
    marketing_info: Optional[MarketingInfo] = None
    approval_info: Optional[ApprovalInfo] = None
    routes_of_administration: List[RouteOfAdministration] = field(default_factory=list)


@dataclass
class MediaReference:
    """Represents a media reference (images, etc.)."""
    media_id: str
    media_type: str
    reference_value: str
    description: Optional[str] = None


@dataclass
class SPLSection:
    """Represents a section within an SPL document."""
    section_id: str
    section_code: Optional[CodedConcept] = None
    text_content: Optional[str] = None
    effective_time: Optional[str] = None
    media_references: List[MediaReference] = field(default_factory=list)
    subsections: List['SPLSection'] = field(default_factory=list)


@dataclass
class DocumentAuthor:
    """Represents the document author information."""
    organizations: List[Organization] = field(default_factory=list)
    time: Optional[str] = None


@dataclass
class SPLDocument:
    """
    Root model representing a complete SPL document.
    Maps to the top-level <document> element in SPL XML.
    """
    # Document identification
    document_id: str
    set_id: str
    version_number: str
    
    # Document metadata
    document_code: Optional[CodedConcept] = None
    effective_time: Optional[str] = None
    
    # Document structure
    author: Optional[DocumentAuthor] = None
    manufactured_product: Optional[ManufacturedProduct] = None
    sections: List[SPLSection] = field(default_factory=list)
    
    # Raw XML for reference
    raw_xml: Optional[str] = None
    
    # Processing metadata
    processed_at: Optional[datetime] = None
    processing_errors: List[str] = field(default_factory=list)

    def get_sections_by_type(self, section_type: SectionType) -> List[SPLSection]:
        """Get all sections of a specific type."""
        target_code = section_type.value if isinstance(section_type, SectionType) else section_type
        return [section for section in self.sections 
                if section.section_code and section.section_code.code == target_code]
    
    def get_manufactured_products(self) -> List[ManufacturedProduct]:
        """Extract all manufactured products from the document."""
        products = []
        if self.manufactured_product:
            products.append(self.manufactured_product)
        return products
    
    def get_active_ingredients(self) -> List[Ingredient]:
        """Extract all active ingredients from manufactured products."""
        ingredients = []
        for product in self.get_manufactured_products():
            ingredients.extend([ing for ing in product.ingredients if ing.type == IngredientType.ACTIVE])
        return ingredients
    
    def get_section_text_by_type(self, section_type: SectionType) -> Optional[str]:
        """Get the text content of the first section of a specific type."""
        sections = self.get_sections_by_type(section_type)
        return sections[0].text_content if sections else None


# Phase 3: Normalized Data Models

@dataclass
class ExtractedAdverseEffect:
    """Represents an extracted adverse effect with ICD-10 mapping."""
    effect_text: str
    icd10_code: Optional[str] = None
    icd10_description: Optional[str] = None
    confidence_score: float = 0.0
    severity: Optional[str] = None  # mild, moderate, severe


@dataclass
class ExtractedDosing:
    """Represents extracted dosing information."""
    dose_amount: Optional[str] = None
    dose_unit: Optional[str] = None
    frequency: Optional[str] = None
    duration: Optional[str] = None
    route: Optional[str] = None
    population: Optional[str] = None
    confidence_score: float = 0.0


@dataclass
class ExtractedContraindication:
    """Represents extracted contraindication information."""
    condition: str
    population: Optional[str] = None
    severity: Optional[str] = None  # absolute, relative
    confidence_score: float = 0.0


@dataclass
class ExtractedDrugInteraction:
    """Represents extracted drug interaction information."""
    interacting_substance: str
    interaction_type: Optional[str] = None
    severity: Optional[str] = None  # minor, moderate, major
    mechanism: Optional[str] = None
    clinical_effect: Optional[str] = None
    confidence_score: float = 0.0


@dataclass
class ExtractedWarning:
    """Represents extracted warning information."""
    warning_text: str
    risk_level: Optional[str] = None  # low, moderate, high
    population: Optional[str] = None
    clinical_context: Optional[str] = None
    confidence_score: float = 0.0


@dataclass
class NormalizedSPLSection:
    """Enhanced SPL section with extracted clinical knowledge."""
    # Original section data
    original_section: SPLSection
    
    # Normalized text
    cleaned_text: Optional[str] = None
    
    # Extracted clinical knowledge
    adverse_effects: List[ExtractedAdverseEffect] = field(default_factory=list)
    dosing_info: List[ExtractedDosing] = field(default_factory=list)
    contraindications: List[ExtractedContraindication] = field(default_factory=list)
    drug_interactions: List[ExtractedDrugInteraction] = field(default_factory=list)
    warnings: List[ExtractedWarning] = field(default_factory=list)
    
    # Processing metadata
    processing_confidence: float = 0.0
    extracted_at: Optional[datetime] = None
    extraction_method: Optional[str] = None
    needs_human_review: bool = False


@dataclass
class NormalizedSPLDocument:
    """Enhanced SPL document with normalized data and extracted clinical knowledge."""
    # Original document data
    original_document: SPLDocument
    
    # Normalized metadata
    normalized_effective_date: Optional[str] = None  # ISO-8601 format
    validated_ndcs: List[str] = field(default_factory=list)
    normalized_units: Dict[str, str] = field(default_factory=dict)  # original -> UCUM
    
    # Enhanced sections
    normalized_sections: List[NormalizedSPLSection] = field(default_factory=list)
    
    # Document-level extracted knowledge summary
    all_adverse_effects: List[ExtractedAdverseEffect] = field(default_factory=list)
    all_dosing_info: List[ExtractedDosing] = field(default_factory=list)
    all_contraindications: List[ExtractedContraindication] = field(default_factory=list)
    all_drug_interactions: List[ExtractedDrugInteraction] = field(default_factory=list)
    all_warnings: List[ExtractedWarning] = field(default_factory=list)
    
    # Processing metadata
    normalized_at: Optional[datetime] = None
    processing_errors: List[str] = field(default_factory=list)
    overall_confidence: float = 0.0
    human_review_flags: List[str] = field(default_factory=list)
    
    def get_high_confidence_extractions(self, min_confidence: float = 0.8) -> Dict[str, List]:
        """Get all extractions above specified confidence threshold."""
        return {
            'adverse_effects': [ae for ae in self.all_adverse_effects if ae.confidence_score >= min_confidence],
            'dosing_info': [di for di in self.all_dosing_info if di.confidence_score >= min_confidence],
            'contraindications': [ci for ci in self.all_contraindications if ci.confidence_score >= min_confidence],
            'drug_interactions': [di for di in self.all_drug_interactions if di.confidence_score >= min_confidence],
            'warnings': [w for w in self.all_warnings if w.confidence_score >= min_confidence]
        }
    
    def get_sections_needing_review(self) -> List[NormalizedSPLSection]:
        """Get sections flagged for human review."""
        return [section for section in self.normalized_sections if section.needs_human_review]