"""
Parser Factory - Creates and manages parser instances with configuration.
Provides factory pattern for parser instantiation and dependency injection.
"""

from typing import Dict, Type, Optional, Any, List
from enum import Enum
import logging

from .base_parser import BaseParser
from .spl_document_parser import SPLDocumentParser
from .section_parser import SectionParser
from .product_parser import ProductParser
from .ingredient_parser import IngredientParser
from .clinical_section_parser import ClinicalSectionParser
from .models import SectionType


class ParserType(Enum):
    """Available parser types."""
    SPL_DOCUMENT = "spl_document"
    SECTION = "section"
    PRODUCT = "product"
    INGREDIENT = "ingredient"
    CLINICAL_SECTION = "clinical_section"


class ParserConfiguration:
    """Configuration class for parser instances."""
    
    def __init__(self):
        self.strict_validation = True
        self.ignore_unknown_sections = False
        self.max_errors_per_parser = 100
        self.cache_substance_mappings = True
        self.normalize_text = True
        self.extract_media_references = True
        self.parse_subsections_recursively = True
        self.section_type_overrides: Dict[str, SectionType] = {}
        self.custom_section_mappings: Dict[str, str] = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'strict_validation': self.strict_validation,
            'ignore_unknown_sections': self.ignore_unknown_sections,
            'max_errors_per_parser': self.max_errors_per_parser,
            'cache_substance_mappings': self.cache_substance_mappings,
            'normalize_text': self.normalize_text,
            'extract_media_references': self.extract_media_references,
            'parse_subsections_recursively': self.parse_subsections_recursively,
            'section_type_overrides': {k: v.value for k, v in self.section_type_overrides.items()},
            'custom_section_mappings': self.custom_section_mappings
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ParserConfiguration':
        """Create configuration from dictionary."""
        config = cls()
        for key, value in config_dict.items():
            if hasattr(config, key):
                setattr(config, key, value)
        return config


class ParserFactory:
    """
    Factory for creating and managing parser instances.
    Provides centralized configuration and parser lifecycle management.
    """
    
    def __init__(self, config: Optional[ParserConfiguration] = None):
        self.config = config or ParserConfiguration()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._parser_cache: Dict[ParserType, BaseParser] = {}
        self._parser_registry = self._initialize_parser_registry()
    
    def create_parser(self, parser_type: ParserType, use_cache: bool = True) -> BaseParser:
        """
        Create or retrieve a parser instance.
        
        Args:
            parser_type: Type of parser to create
            use_cache: Whether to use cached instances
            
        Returns:
            BaseParser: Configured parser instance
        """
        if use_cache and parser_type in self._parser_cache:
            return self._parser_cache[parser_type]
        
        parser_class = self._parser_registry.get(parser_type)
        if not parser_class:
            raise ValueError(f"Unknown parser type: {parser_type}")
        
        try:
            parser = parser_class()
            self._configure_parser(parser, parser_type)
            
            if use_cache:
                self._parser_cache[parser_type] = parser
            
            self.logger.debug(f"Created {parser_type.value} parser")
            return parser
            
        except Exception as e:
            self.logger.error(f"Failed to create {parser_type.value} parser: {str(e)}")
            raise
    
    def create_spl_document_parser(self) -> SPLDocumentParser:
        """Create a configured SPL document parser."""
        return self.create_parser(ParserType.SPL_DOCUMENT)
    
    def create_section_parser(self) -> SectionParser:
        """Create a configured section parser."""
        return self.create_parser(ParserType.SECTION)
    
    def create_product_parser(self) -> ProductParser:
        """Create a configured product parser."""
        return self.create_parser(ParserType.PRODUCT)
    
    def create_ingredient_parser(self) -> IngredientParser:
        """Create a configured ingredient parser."""
        return self.create_parser(ParserType.INGREDIENT)
    
    def create_clinical_section_parser(self) -> ClinicalSectionParser:
        """Create a configured clinical section parser."""
        return self.create_parser(ParserType.CLINICAL_SECTION)
    
    def create_parser_suite(self) -> Dict[ParserType, BaseParser]:
        """Create a complete suite of all parser types."""
        return {
            parser_type: self.create_parser(parser_type)
            for parser_type in ParserType
        }
    
    def _configure_parser(self, parser: BaseParser, parser_type: ParserType) -> None:
        """Apply configuration to a parser instance."""
        # Configure parser-specific settings based on type
        if parser_type == ParserType.SPL_DOCUMENT:
            self._configure_document_parser(parser)
        elif parser_type == ParserType.SECTION:
            self._configure_section_parser(parser)
        elif parser_type == ParserType.PRODUCT:
            self._configure_product_parser(parser)
        elif parser_type == ParserType.INGREDIENT:
            self._configure_ingredient_parser(parser)
        elif parser_type == ParserType.CLINICAL_SECTION:
            self._configure_clinical_parser(parser)
    
    def _configure_document_parser(self, parser: SPLDocumentParser) -> None:
        """Configure SPL document parser."""
        # Document parser configurations can be set here
        pass
    
    def _configure_section_parser(self, parser: SectionParser) -> None:
        """Configure section parser."""
        # Section parser configurations
        if hasattr(parser, 'parse_subsections_recursively'):
            parser.parse_subsections_recursively = self.config.parse_subsections_recursively
    
    def _configure_product_parser(self, parser: ProductParser) -> None:
        """Configure product parser."""
        # Product parser configurations
        pass
    
    def _configure_ingredient_parser(self, parser: IngredientParser) -> None:
        """Configure ingredient parser."""
        if hasattr(parser, 'substance_cache') and self.config.cache_substance_mappings:
            # Enable substance caching
            pass
    
    def _configure_clinical_parser(self, parser: ClinicalSectionParser) -> None:
        """Configure clinical section parser."""
        # Clinical parser configurations
        pass
    
    def _initialize_parser_registry(self) -> Dict[ParserType, Type[BaseParser]]:
        """Initialize the registry of parser types to classes."""
        return {
            ParserType.SPL_DOCUMENT: SPLDocumentParser,
            ParserType.SECTION: SectionParser,
            ParserType.PRODUCT: ProductParser,
            ParserType.INGREDIENT: IngredientParser,
            ParserType.CLINICAL_SECTION: ClinicalSectionParser,
        }
    
    def clear_cache(self) -> None:
        """Clear the parser cache."""
        self._parser_cache.clear()
        self.logger.debug("Parser cache cleared")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about the parser cache."""
        return {
            'cached_parsers': [parser_type.value for parser_type in self._parser_cache.keys()],
            'cache_size': len(self._parser_cache),
            'available_types': [parser_type.value for parser_type in ParserType]
        }


class PresetConfigurations:
    """Predefined configurations for common use cases."""
    
    @staticmethod
    def development_config() -> ParserConfiguration:
        """Configuration optimized for development and testing."""
        config = ParserConfiguration()
        config.strict_validation = False
        config.ignore_unknown_sections = True
        config.max_errors_per_parser = 50
        return config
    
    @staticmethod
    def production_config() -> ParserConfiguration:
        """Configuration optimized for production use."""
        config = ParserConfiguration()
        config.strict_validation = True
        config.ignore_unknown_sections = False
        config.max_errors_per_parser = 200
        config.cache_substance_mappings = True
        return config
    
    @staticmethod
    def fast_parsing_config() -> ParserConfiguration:
        """Configuration optimized for speed."""
        config = ParserConfiguration()
        config.extract_media_references = False
        config.normalize_text = False
        config.parse_subsections_recursively = False
        config.cache_substance_mappings = False
        return config
    
    @staticmethod
    def comprehensive_config() -> ParserConfiguration:
        """Configuration for maximum data extraction."""
        config = ParserConfiguration()
        config.strict_validation = True
        config.extract_media_references = True
        config.normalize_text = True
        config.parse_subsections_recursively = True
        config.cache_substance_mappings = True
        return config


class ParserManager:
    """High-level manager for coordinating multiple parsers."""
    
    def __init__(self, config: Optional[ParserConfiguration] = None):
        self.factory = ParserFactory(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parsing_stats = {
            'documents_parsed': 0,
            'sections_parsed': 0,
            'products_parsed': 0,
            'ingredients_parsed': 0,
            'errors_encountered': 0
        }
    
    def parse_complete_document(self, xml_source: str) -> Any:
        """
        Parse a complete SPL document using coordinated parsers.
        
        Args:
            xml_source: SPL XML content
            
        Returns:
            Parsed SPL document with full enhancement
        """
        try:
            # Create document parser
            doc_parser = self.factory.create_spl_document_parser()
            
            # Parse document
            document = doc_parser.parse(xml_source)
            
            # Enhance sections using section parser
            section_parser = self.factory.create_section_parser()
            enhanced_sections = []
            
            for section in document.sections:
                # This would require refactoring to pass the original XML elements
                # For now, sections are already parsed by the document parser
                enhanced_sections.append(section)
            
            document.sections = enhanced_sections
            
            # Update statistics
            self.parsing_stats['documents_parsed'] += 1
            self.parsing_stats['sections_parsed'] += len(document.sections)
            self.parsing_stats['errors_encountered'] += len(document.processing_errors)
            
            return document
            
        except Exception as e:
            self.logger.error(f"Failed to parse complete document: {str(e)}")
            self.parsing_stats['errors_encountered'] += 1
            raise
    
    def get_parsing_statistics(self) -> Dict[str, Any]:
        """Get parsing statistics."""
        return self.parsing_stats.copy()
    
    def reset_statistics(self) -> None:
        """Reset parsing statistics."""
        for key in self.parsing_stats:
            self.parsing_stats[key] = 0
    
    def get_factory_info(self) -> Dict[str, Any]:
        """Get information about the parser factory."""
        return {
            'configuration': self.factory.config.to_dict(),
            'cache_info': self.factory.get_cache_info(),
            'available_parsers': [pt.value for pt in ParserType]
        }


# Global factory instance for convenience
default_factory = ParserFactory(PresetConfigurations.production_config())


def get_default_factory() -> ParserFactory:
    """Get the default parser factory instance."""
    return default_factory


def create_parser(parser_type: ParserType, config: Optional[ParserConfiguration] = None) -> BaseParser:
    """
    Convenience function to create a parser with optional custom configuration.
    
    Args:
        parser_type: Type of parser to create
        config: Optional custom configuration
        
    Returns:
        BaseParser: Configured parser instance
    """
    if config:
        factory = ParserFactory(config)
        return factory.create_parser(parser_type, use_cache=False)
    else:
        return default_factory.create_parser(parser_type)


def create_parser_manager(config_name: str = "production") -> ParserManager:
    """
    Create a parser manager with a preset configuration.
    
    Args:
        config_name: Name of preset configuration (development, production, fast, comprehensive)
        
    Returns:
        ParserManager: Configured parser manager
    """
    config_map = {
        "development": PresetConfigurations.development_config,
        "production": PresetConfigurations.production_config,
        "fast": PresetConfigurations.fast_parsing_config,
        "comprehensive": PresetConfigurations.comprehensive_config,
    }
    
    config_func = config_map.get(config_name, PresetConfigurations.production_config)
    config = config_func()
    
    return ParserManager(config)