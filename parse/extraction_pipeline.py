"""
Extraction Pipeline - End-to-end workflow orchestration for SPL parsing.
Coordinates all parsing components with configuration management and monitoring.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum

from models import SPLDocument, SectionType
from spl_document_parser import SPLDocumentParser, SPLParseResult
from parser_factory import ParserManager, ParserConfiguration, PresetConfigurations
from batch_processor import BatchProcessor, BatchJob, BatchResult
from validators import SPLDocumentValidator, ValidationResult
from section_parser import SectionAnalyzer


class PipelineStage(Enum):
    """Pipeline processing stages."""
    INITIALIZATION = "initialization"
    PARSING = "parsing"
    ENHANCEMENT = "enhancement"
    VALIDATION = "validation"
    POST_PROCESSING = "post_processing"
    OUTPUT = "output"
    COMPLETED = "completed"


@dataclass
class PipelineConfiguration:
    """Configuration for the extraction pipeline."""
    # Parser configuration
    parser_config: Optional[ParserConfiguration] = None
    
    # Processing options
    enable_validation: bool = True
    enable_section_enhancement: bool = True
    enable_ingredient_parsing: bool = True
    enable_clinical_text_processing: bool = True
    
    # Performance options
    max_workers: int = 4
    use_multiprocessing: bool = False
    batch_size: int = 100
    
    # Output options
    output_format: str = "json"  # json, jsonl, csv
    save_raw_documents: bool = True
    save_validation_reports: bool = True
    save_processing_metrics: bool = True
    
    # Quality control
    min_success_rate: float = 0.80
    max_error_rate: float = 0.20
    stop_on_validation_failure: bool = False
    
    # Monitoring
    enable_progress_reporting: bool = True
    log_level: str = "INFO"
    
    def __post_init__(self):
        if self.parser_config is None:
            self.parser_config = PresetConfigurations.production_config()


@dataclass
class ProcessingMetrics:
    """Metrics for pipeline processing."""
    total_files: int = 0
    successful_parses: int = 0
    failed_parses: int = 0
    validation_passes: int = 0
    validation_failures: int = 0
    total_sections: int = 0
    total_products: int = 0
    total_ingredients: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    processing_time: float = 0.0
    stage_timings: Dict[PipelineStage, float] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 0.0
        return (self.successful_parses / self.total_files) * 100
    
    @property
    def validation_rate(self) -> float:
        if self.successful_parses == 0:
            return 0.0
        return (self.validation_passes / self.successful_parses) * 100


@dataclass 
class PipelineResult:
    """Result of pipeline execution."""
    pipeline_id: str
    configuration: PipelineConfiguration
    metrics: ProcessingMetrics
    documents: List[SPLDocument] = field(default_factory=list)
    validation_results: Dict[str, ValidationResult] = field(default_factory=dict)
    processing_errors: List[str] = field(default_factory=list)
    stage_timings: Dict[PipelineStage, float] = field(default_factory=dict)
    output_files: List[str] = field(default_factory=list)


class ExtractionPipeline:
    """
    End-to-end extraction pipeline for SPL documents.
    Orchestrates parsing, validation, and output generation.
    """
    
    def __init__(self, config: Optional[PipelineConfiguration] = None):
        self.config = config or PipelineConfiguration()
        self.pipeline_id = f"pipeline_{int(datetime.now().timestamp())}"
        self.logger = self._setup_logger()
        
        # Initialize components
        self.parser_manager = ParserManager(self.config.parser_config)
        self.batch_processor = BatchProcessor(self.parser_manager)
        self.validator = SPLDocumentValidator()
        
        # State tracking
        self.current_stage = PipelineStage.INITIALIZATION
        self.metrics = ProcessingMetrics()
        self.progress_callbacks: List[Callable] = []
    
    def add_progress_callback(self, callback: Callable[[str, PipelineStage, Dict], None]):
        """Add a progress callback function."""
        self.progress_callbacks.append(callback)
    
    def process_files(self, file_paths: List[str], 
                     output_directory: Optional[str] = None) -> PipelineResult:
        """
        Process a list of SPL files through the complete pipeline.
        
        Args:
            file_paths: List of SPL file paths to process
            output_directory: Optional directory for output files
            
        Returns:
            PipelineResult: Complete pipeline execution results
        """
        self.logger.info(f"Starting pipeline {self.pipeline_id} with {len(file_paths)} files")
        
        # Initialize metrics
        self.metrics = ProcessingMetrics(
            total_files=len(file_paths),
            start_time=datetime.now()
        )
        
        result = PipelineResult(
            pipeline_id=self.pipeline_id,
            configuration=self.config,
            metrics=self.metrics
        )
        
        try:
            # Stage 1: Initialization
            self._execute_stage(PipelineStage.INITIALIZATION, lambda: self._stage_initialization())
            
            # Stage 2: Parsing
            documents, errors = self._execute_stage(
                PipelineStage.PARSING, 
                lambda: self._stage_parsing(file_paths)
            )
            result.documents = documents
            result.processing_errors.extend(errors)
            
            # Stage 3: Enhancement (if enabled)
            if self.config.enable_section_enhancement:
                enhanced_docs = self._execute_stage(
                    PipelineStage.ENHANCEMENT,
                    lambda: self._stage_enhancement(documents)
                )
                result.documents = enhanced_docs
            
            # Stage 4: Validation (if enabled)
            if self.config.enable_validation:
                validation_results = self._execute_stage(
                    PipelineStage.VALIDATION,
                    lambda: self._stage_validation(result.documents)
                )
                result.validation_results = validation_results
            
            # Stage 5: Post-processing
            processed_docs = self._execute_stage(
                PipelineStage.POST_PROCESSING,
                lambda: self._stage_post_processing(result.documents)
            )
            result.documents = processed_docs
            
            # Stage 6: Output generation
            if output_directory:
                output_files = self._execute_stage(
                    PipelineStage.OUTPUT,
                    lambda: self._stage_output(result, output_directory)
                )
                result.output_files = output_files
            
            # Finalize
            self._execute_stage(PipelineStage.COMPLETED, lambda: self._stage_completion(result))
            
            self.logger.info(f"Pipeline completed successfully: {result.metrics.success_rate:.1f}% success rate")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            result.processing_errors.append(str(e))
            raise
        
        return result
    
    def process_directory(self, directory: str, pattern: str = "*.xml",
                         output_directory: Optional[str] = None) -> PipelineResult:
        """Process all matching files in a directory."""
        directory_path = Path(directory)
        file_paths = list(str(p) for p in directory_path.glob(pattern))
        
        if not file_paths:
            raise ValueError(f"No files found matching pattern '{pattern}' in {directory}")
        
        self.logger.info(f"Found {len(file_paths)} files in {directory}")
        return self.process_files(file_paths, output_directory)
    
    def _execute_stage(self, stage: PipelineStage, stage_func: Callable) -> Any:
        """Execute a pipeline stage with timing and error handling."""
        self.current_stage = stage
        self.logger.debug(f"Executing stage: {stage.value}")
        
        start_time = datetime.now()
        
        # Notify progress callbacks
        self._notify_progress(stage, {})
        
        try:
            result = stage_func()
            
            # Record timing
            elapsed = (datetime.now() - start_time).total_seconds()
            if not hasattr(self, 'stage_timings'):
                self.stage_timings = {}
            self.stage_timings[stage] = elapsed
            
            self.logger.debug(f"Stage {stage.value} completed in {elapsed:.2f}s")
            return result
            
        except Exception as e:
            self.logger.error(f"Stage {stage.value} failed: {str(e)}")
            raise
    
    def _stage_initialization(self):
        """Initialize pipeline components and validate configuration."""
        self.logger.info("Initializing pipeline components")
        
        # Validate configuration
        if self.config.min_success_rate < 0 or self.config.min_success_rate > 1:
            raise ValueError("min_success_rate must be between 0 and 1")
        
        # Initialize parser components
        self.parser_manager.reset_statistics()
    
    def _stage_parsing(self, file_paths: List[str]) -> tuple[List[SPLDocument], List[str]]:
        """Parse all SPL files."""
        self.logger.info(f"Parsing {len(file_paths)} files")
        
        # Create batch job
        batch_job = BatchJob(
            job_id=f"{self.pipeline_id}_parsing",
            file_paths=file_paths,
            max_workers=self.config.max_workers,
            use_multiprocessing=self.config.use_multiprocessing
        )
        
        # Process batch
        def progress_callback(completed, total, failed, elapsed):
            progress_data = {
                'completed': completed,
                'total': total,
                'failed': failed,
                'elapsed': elapsed
            }
            self._notify_progress(PipelineStage.PARSING, progress_data)
        
        batch_result = self.batch_processor.process_batch(batch_job, progress_callback)
        
        # Extract documents and update metrics
        documents = []
        errors = []
        
        for result in batch_result.results:
            if result.success and result.document:
                documents.append(result.document)
                self.metrics.successful_parses += 1
            else:
                self.metrics.failed_parses += 1
                if result.error_message:
                    errors.append(f"{result.file_path}: {result.error_message}")
        
        # Update metrics
        self.metrics.total_sections = sum(len(doc.sections) for doc in documents)
        self.metrics.total_products = sum(len(doc.get_manufactured_products()) for doc in documents)
        self.metrics.total_ingredients = sum(len(doc.get_active_ingredients()) for doc in documents)
        
        return documents, errors
    
    def _stage_enhancement(self, documents: List[SPLDocument]) -> List[SPLDocument]:
        """Enhance documents with additional parsing."""
        self.logger.info(f"Enhancing {len(documents)} documents")
        
        # This would involve re-parsing sections with enhanced parsers
        # For now, documents are already enhanced during initial parsing
        return documents
    
    def _stage_validation(self, documents: List[SPLDocument]) -> Dict[str, ValidationResult]:
        """Validate all parsed documents."""
        self.logger.info(f"Validating {len(documents)} documents")
        
        validation_results = {}
        
        for doc in documents:
            try:
                result = self.validator.validate(doc)
                validation_results[doc.document_id] = result
                
                if result.is_valid():
                    self.metrics.validation_passes += 1
                else:
                    self.metrics.validation_failures += 1
                    
                    if self.config.stop_on_validation_failure:
                        raise ValueError(f"Document {doc.document_id} failed validation with {len(result.errors)} errors")
                        
            except Exception as e:
                self.logger.error(f"Validation failed for {doc.document_id}: {str(e)}")
                self.metrics.validation_failures += 1
        
        return validation_results
    
    def _stage_post_processing(self, documents: List[SPLDocument]) -> List[SPLDocument]:
        """Apply post-processing to documents."""
        self.logger.info(f"Post-processing {len(documents)} documents")
        
        processed_docs = []
        for doc in documents:
            try:
                # Apply any post-processing logic
                processed_doc = self._apply_post_processing(doc)
                processed_docs.append(processed_doc)
            except Exception as e:
                self.logger.error(f"Post-processing failed for {doc.document_id}: {str(e)}")
                processed_docs.append(doc)  # Keep original if processing fails
        
        return processed_docs
    
    def _stage_output(self, result: PipelineResult, output_directory: str) -> List[str]:
        """Generate output files."""
        self.logger.info(f"Generating output files in {output_directory}")
        
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        
        output_files = []
        
        # Save documents
        if self.config.save_raw_documents:
            docs_file = output_path / f"{self.pipeline_id}_documents.json"
            self._save_documents(result.documents, docs_file)
            output_files.append(str(docs_file))
        
        # Save validation reports
        if self.config.save_validation_reports and result.validation_results:
            validation_file = output_path / f"{self.pipeline_id}_validation.json"
            self._save_validation_results(result.validation_results, validation_file)
            output_files.append(str(validation_file))
        
        # Save processing metrics
        if self.config.save_processing_metrics:
            metrics_file = output_path / f"{self.pipeline_id}_metrics.json"
            self._save_metrics(result.metrics, metrics_file)
            output_files.append(str(metrics_file))
        
        # Save summary report
        summary_file = output_path / f"{self.pipeline_id}_summary.json"
        self._save_summary(result, summary_file)
        output_files.append(str(summary_file))
        
        return output_files
    
    def _stage_completion(self, result: PipelineResult):
        """Finalize pipeline execution."""
        self.metrics.end_time = datetime.now()
        self.metrics.processing_time = (self.metrics.end_time - self.metrics.start_time).total_seconds()
        
        self.current_stage = PipelineStage.COMPLETED
        
        # Final progress notification
        final_data = {
            'success_rate': self.metrics.success_rate,
            'validation_rate': self.metrics.validation_rate,
            'total_time': self.metrics.processing_time
        }
        self._notify_progress(PipelineStage.COMPLETED, final_data)
    
    def _apply_post_processing(self, document: SPLDocument) -> SPLDocument:
        """Apply post-processing transformations to a document."""
        # Example post-processing operations
        
        # Ensure processing timestamp is set
        if not document.processed_at:
            document.processed_at = datetime.now()
        
        # Calculate document metrics
        section_analysis = SectionAnalyzer.analyze_section_distribution(document.sections)
        
        # Could add analysis results to document metadata
        # For now, just return the document as-is
        return document
    
    def _save_documents(self, documents: List[SPLDocument], file_path: Path):
        """Save documents to JSON file."""
        with open(file_path, 'w') as f:
            json.dump([asdict(doc) for doc in documents], f, indent=2, default=str)
    
    def _save_validation_results(self, validation_results: Dict[str, ValidationResult], file_path: Path):
        """Save validation results to JSON file."""
        serializable_results = {}
        for doc_id, result in validation_results.items():
            serializable_results[doc_id] = {
                'is_valid': result.is_valid(),
                'error_count': len(result.errors),
                'warning_count': len(result.warnings),
                'errors': [str(error) for error in result.errors],
                'warnings': [str(warning) for warning in result.warnings]
            }
        
        with open(file_path, 'w') as f:
            json.dump(serializable_results, f, indent=2)
    
    def _save_metrics(self, metrics: ProcessingMetrics, file_path: Path):
        """Save processing metrics to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(asdict(metrics), f, indent=2, default=str)
    
    def _save_summary(self, result: PipelineResult, file_path: Path):
        """Save pipeline summary to JSON file."""
        summary = {
            'pipeline_id': result.pipeline_id,
            'execution_time': result.metrics.processing_time,
            'total_files': result.metrics.total_files,
            'success_rate': result.metrics.success_rate,
            'validation_rate': result.metrics.validation_rate,
            'total_sections': result.metrics.total_sections,
            'total_products': result.metrics.total_products,
            'total_ingredients': result.metrics.total_ingredients,
            'stage_timings': {stage.value: timing for stage, timing in getattr(self, 'stage_timings', {}).items()},
            'output_files': result.output_files,
            'error_count': len(result.processing_errors)
        }
        
        with open(file_path, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def _notify_progress(self, stage: PipelineStage, data: Dict):
        """Notify progress callbacks."""
        for callback in self.progress_callbacks:
            try:
                callback(self.pipeline_id, stage, data)
            except Exception:
                # Don't let callback errors stop processing
                pass
    
    def _setup_logger(self) -> logging.Logger:
        """Setup pipeline logger."""
        logger = logging.getLogger(f"ExtractionPipeline.{self.pipeline_id}")
        logger.setLevel(getattr(logging, self.config.log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger


def create_pipeline(config_name: str = "production") -> ExtractionPipeline:
    """
    Create a preconfigured extraction pipeline.
    
    Args:
        config_name: Configuration preset name
        
    Returns:
        ExtractionPipeline: Configured pipeline instance
    """
    config_map = {
        "development": lambda: PipelineConfiguration(
            parser_config=PresetConfigurations.development_config(),
            max_workers=2,
            enable_validation=False,
            log_level="DEBUG"
        ),
        "production": lambda: PipelineConfiguration(
            parser_config=PresetConfigurations.production_config(),
            max_workers=4,
            enable_validation=True,
            log_level="INFO"
        ),
        "fast": lambda: PipelineConfiguration(
            parser_config=PresetConfigurations.fast_parsing_config(),
            max_workers=8,
            enable_validation=False,
            enable_section_enhancement=False,
            log_level="WARNING"
        ),
        "comprehensive": lambda: PipelineConfiguration(
            parser_config=PresetConfigurations.comprehensive_config(),
            max_workers=2,
            enable_validation=True,
            save_validation_reports=True,
            log_level="DEBUG"
        )
    }
    
    config_func = config_map.get(config_name, config_map["production"])
    config = config_func()
    
    return ExtractionPipeline(config)