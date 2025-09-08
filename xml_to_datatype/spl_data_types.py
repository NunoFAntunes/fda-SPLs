#!/usr/bin/env python3
"""
FDA SPL Data Types

This module defines Python dataclasses for FDA Structured Product Labeling (SPL) documents.
These dataclasses provide type safety and structure for converting XML to Python objects.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
import json
from datetime import datetime


@dataclass
class SourceFile:
    """Information about the source file."""
    path: str
    filecite: str


@dataclass
class Identifier:
    """HL7 identifier with root and optional extension."""
    root: str
    extension: Optional[str] = None


@dataclass
class CodedElement:
    """HL7 coded element with code, codeSystem, and displayName."""
    code: str
    codeSystem: Optional[str] = None
    displayName: Optional[str] = None


@dataclass
class TextContent:
    """Text content with HTML and plain text versions."""
    html: Optional[str] = None
    text: Optional[str] = None
    suffix: Optional[str] = None


@dataclass
class TimeValue:
    """HL7 time value with optional low value for ranges."""
    value: Optional[str] = None
    low: Optional['TimeValue'] = None


@dataclass
class Quantity:
    """HL7 quantity with numerator and denominator."""
    numerator: Optional['QuantityPart'] = None
    denominator: Optional['QuantityPart'] = None


@dataclass
class QuantityPart:
    """Part of a quantity (numerator or denominator)."""
    value: str
    unit: Optional[str] = None
    translation: Optional[CodedElement] = None


@dataclass
class ActiveMoiety:
    """Active moiety information for ingredients."""
    code: CodedElement
    name: str


@dataclass
class IngredientSubstance:
    """Substance information for ingredients."""
    code: Optional[CodedElement] = None
    name: Optional[str] = None
    activeMoiety: List[ActiveMoiety] = field(default_factory=list)


@dataclass
class Ingredient:
    """Ingredient information (active or inactive)."""
    classCode: str
    quantity: Optional[Quantity] = None
    ingredientSubstance: Optional[IngredientSubstance] = None


@dataclass
class ContainerPackagedProduct:
    """Container packaging information."""
    code: Optional[str] = None
    codeSystem: Optional[str] = None
    formCode: Optional[CodedElement] = None


@dataclass
class Packaging:
    """Product packaging information."""
    quantity: Optional[Quantity] = None
    containerPackagedProduct: Optional[ContainerPackagedProduct] = None


@dataclass
class TerritorialAuthority:
    """Territorial authority for approvals."""
    territory: CodedElement


@dataclass
class ApprovalAuthor:
    """Author information for approvals."""
    territorialAuthority: TerritorialAuthority


@dataclass
class Approval:
    """Product approval information."""
    id: Optional[Identifier] = None
    code: Optional[CodedElement] = None
    author: Optional[ApprovalAuthor] = None


@dataclass
class StatusCode:
    """Simple status code."""
    code: str


@dataclass
class MarketingAct:
    """Marketing act information."""
    code: Optional[CodedElement] = None
    statusCode: Optional[StatusCode] = None
    effectiveTime: Optional[TimeValue] = None


@dataclass
class CharacteristicValue:
    """Value for a characteristic with different types."""
    xsi_type: str
    code: Optional[str] = None
    codeSystem: Optional[str] = None
    displayName: Optional[str] = None
    value: Optional[str] = None
    unit: Optional[str] = None
    text: Optional[str] = None


@dataclass
class Characteristic:
    """Product characteristic (color, shape, size, etc.)."""
    code: CodedElement
    value: Optional[CharacteristicValue] = None


@dataclass
class SubstanceAdministration:
    """Administration route information."""
    routeCode: CodedElement


@dataclass
class ConsumedIn:
    """Information about how the product is consumed."""
    substanceAdministration: SubstanceAdministration


@dataclass
class SubjectOf:
    """Subject of relationships (approvals, marketing acts, characteristics)."""
    approvals: List[Approval] = field(default_factory=list)
    marketingActs: List[MarketingAct] = field(default_factory=list)
    characteristics: List[Characteristic] = field(default_factory=list)


@dataclass
class ManufacturedProduct:
    """Manufactured product information."""
    code: Optional[CodedElement] = None
    name: Optional[TextContent] = None
    formCode: Optional[CodedElement] = None
    genericMedicine: List[str] = field(default_factory=list)
    ingredients: List[Ingredient] = field(default_factory=list)
    packaging: List[Packaging] = field(default_factory=list)
    subjectOf: Optional[SubjectOf] = None
    consumedIn: Optional[ConsumedIn] = None


@dataclass
class AssignedOrganization:
    """Empty assigned organization placeholder."""
    pass


@dataclass
class AssignedEntity:
    """Assigned entity with organization."""
    assignedOrganization: AssignedOrganization = field(default_factory=AssignedOrganization)


@dataclass
class RepresentedOrganization:
    """Organization information."""
    id: Optional[Identifier] = None
    name: Optional[str] = None
    assignedEntity: AssignedEntity = field(default_factory=AssignedEntity)


@dataclass
class AuthorAssignedEntity:
    """Author assigned entity information."""
    representedOrganization: RepresentedOrganization


@dataclass
class Author:
    """Document author information."""
    time: Optional[Any] = None
    assignedEntity: Optional[AuthorAssignedEntity] = None


@dataclass
class Document:
    """Main document information."""
    id: Identifier
    code: CodedElement
    title: TextContent
    effectiveTime: Optional[TimeValue] = None
    setId: Optional[Identifier] = None
    versionNumber: Optional[TimeValue] = None


@dataclass
class ObservationMediaValue:
    """Value for observation media."""
    xsi_type: str
    mediaType: str
    reference: str


@dataclass
class ObservationMedia:
    """Observation media (images, documents, etc.)."""
    ID: str
    text: Optional[str] = None
    value: Optional[ObservationMediaValue] = None


@dataclass
class SectionComponent:
    """Component of a section (like warnings subsections)."""
    code: Optional[CodedElement] = None
    text_plain: Optional[str] = None
    text_html: Optional[str] = None
    list_items: List[str] = field(default_factory=list)


@dataclass
class FreeTextSection:
    """Free text section from the document."""
    id_root: Optional[str] = None
    code: Optional[CodedElement] = None
    title: Optional[str] = None
    text_html: Optional[str] = None
    text_plain: Optional[str] = None
    list_items: List[str] = field(default_factory=list)
    components: List[SectionComponent] = field(default_factory=list)
    media_reference: Optional[str] = None
    effectiveTime: Optional[TimeValue] = None


@dataclass
class Notes:
    """Conversion notes and metadata."""
    mapping_rules: str
    sources_consulted: List[str]


@dataclass
class SPLDocument:
    """Complete FDA SPL document structure."""
    source_file: SourceFile
    document: Document
    author: Optional[Author] = None
    manufactured_products: List[ManufacturedProduct] = field(default_factory=list)
    observation_media: List[ObservationMedia] = field(default_factory=list)
    free_text_sections: List[FreeTextSection] = field(default_factory=list)
    notes: Optional[Notes] = None
    original_xml_file: Optional[SourceFile] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the dataclass to a dictionary for JSON serialization."""
        return _dataclass_to_dict(self)

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert the dataclass to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SPLDocument':
        """Create an SPLDocument from a dictionary."""
        return _dict_to_dataclass(cls, data)

    @classmethod
    def from_json(cls, json_str: str) -> 'SPLDocument':
        """Create an SPLDocument from a JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


def _dataclass_to_dict(obj: Any) -> Any:
    """Recursively convert dataclass instances to dictionaries."""
    if hasattr(obj, '__dataclass_fields__'):
        # This is a dataclass
        result = {}
        for field_name, field_def in obj.__dataclass_fields__.items():
            value = getattr(obj, field_name)
            if value is not None:
                result[field_name] = _dataclass_to_dict(value)
        return result
    elif isinstance(obj, list):
        return [_dataclass_to_dict(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: _dataclass_to_dict(value) for key, value in obj.items()}
    else:
        return obj


def _dict_to_dataclass(cls, data: Dict[str, Any]) -> Any:
    """Recursively convert dictionaries to dataclass instances."""
    if not hasattr(cls, '__dataclass_fields__'):
        return data
    
    field_values = {}
    for field_name, field_def in cls.__dataclass_fields__.items():
        if field_name in data:
            field_type = field_def.type
            field_value = data[field_name]
            
            # Handle Optional types
            if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
                # Get the non-None type from Optional[T]
                non_none_types = [t for t in field_type.__args__ if t is not type(None)]
                if non_none_types:
                    field_type = non_none_types[0]
            
            # Handle List types
            if hasattr(field_type, '__origin__') and field_type.__origin__ is list:
                item_type = field_type.__args__[0]
                field_values[field_name] = [_dict_to_dataclass(item_type, item) for item in field_value]
            # Handle dataclass types
            elif hasattr(field_type, '__dataclass_fields__'):
                field_values[field_name] = _dict_to_dataclass(field_type, field_value)
            else:
                field_values[field_name] = field_value
    
    return cls(**field_values)


# Type aliases for convenience
SPLDocumentDict = Dict[str, Any]
SPLDocumentJSON = str