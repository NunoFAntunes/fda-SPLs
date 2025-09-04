"""
Batch Processing Infrastructure.
Handles parallel processing of multiple SPL files with progress tracking and error recovery.
"""

import os
import json
import time
from pathlib import Path
from typing import List, Dict, Optional, Callable, Any, Iterator, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, Future
from dataclasses import dataclass, asdict
from datetime import datetime
import logging
import threading
from queue import Queue

from .spl_document_parser import SPLDocumentParser, SPLParseResult, parse_spl_file
from .parser_factory import ParserManager, PresetConfigurations
from .models import SPLDocument


@dataclass
class BatchJob:
    """Represents a batch processing job."""
    job_id: str
    file_paths: List[str]
    output_directory: Optional[str] = None
    max_workers: int = 4
    use_multiprocessing: bool = False
    continue_on_error: bool = True
    save_results: bool = True
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class ProcessingResult:
    """Result of processing a single file."""
    file_path: str
    success: bool
    document_id: Optional[str] = None
    processing_time: float = 0.0
    error_message: Optional[str] = None
    warnings: List[str] = None
    document: Optional[SPLDocument] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


@dataclass
class BatchResult:
    """Result of a complete batch processing job."""
    job_id: str
    total_files: int
    successful: int
    failed: int
    total_time: float
    results: List[ProcessingResult]
    started_at: datetime
    completed_at: datetime
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.successful / self.total_files) * 100


class ProgressTracker:
    """Thread-safe progress tracking for batch operations."""
    
    def __init__(self, total_items: int):
        self.total_items = total_items
        self.completed_items = 0
        self.failed_items = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.callbacks: List[Callable] = []
    
    def add_callback(self, callback: Callable[[int, int, int, float], None]):
        """Add a progress callback function."""
        self.callbacks.append(callback)
    
    def update(self, success: bool = True):
        """Update progress counters."""
        with self.lock:
            self.completed_items += 1
            if not success:
                self.failed_items += 1
            
            # Call callbacks
            elapsed_time = time.time() - self.start_time
            for callback in self.callbacks:
                try:
                    callback(self.completed_items, self.total_items, self.failed_items, elapsed_time)
                except Exception:
                    pass  # Don't let callback errors stop processing
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress information."""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            progress_pct = (self.completed_items / self.total_items * 100) if self.total_items > 0 else 0
            
            return {
                'completed': self.completed_items,
                'total': self.total_items,
                'failed': self.failed_items,
                'progress_percent': progress_pct,
                'elapsed_time': elapsed_time,
                'items_per_second': self.completed_items / elapsed_time if elapsed_time > 0 else 0
            }


class BatchProcessor:
    """Batch processor for SPL documents with parallel processing capabilities."""
    
    def __init__(self, parser_manager: Optional[ParserManager] = None):
        self.parser_manager = parser_manager or ParserManager(PresetConfigurations.production_config())
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_jobs: Dict[str, BatchJob] = {}
        self.results_cache: Dict[str, BatchResult] = {}
    
    def process_batch(self, job: BatchJob, 
                     progress_callback: Optional[Callable] = None) -> BatchResult:
        """
        Process a batch of SPL files.
        
        Args:
            job: Batch job configuration
            progress_callback: Optional callback for progress updates
            
        Returns:
            BatchResult: Results of batch processing
        """
        self.logger.info(f"Starting batch job {job.job_id} with {len(job.file_paths)} files")
        start_time = datetime.now()
        
        # Initialize progress tracking
        progress_tracker = ProgressTracker(len(job.file_paths))
        if progress_callback:
            progress_tracker.add_callback(progress_callback)
        
        # Store active job
        self.active_jobs[job.job_id] = job
        
        try:
            # Process files
            if job.use_multiprocessing:
                results = self._process_with_multiprocessing(job, progress_tracker)
            else:
                results = self._process_with_threading(job, progress_tracker)
            
            # Calculate statistics
            successful = sum(1 for r in results if r.success)
            failed = len(results) - successful
            total_time = (datetime.now() - start_time).total_seconds()
            
            # Create batch result
            batch_result = BatchResult(
                job_id=job.job_id,
                total_files=len(job.file_paths),
                successful=successful,
                failed=failed,
                total_time=total_time,
                results=results,
                started_at=start_time,
                completed_at=datetime.now()
            )
            
            # Cache result
            self.results_cache[job.job_id] = batch_result
            
            # Save results if requested
            if job.save_results and job.output_directory:
                self._save_batch_results(batch_result, job.output_directory)
            
            self.logger.info(f"Batch job {job.job_id} completed: {successful}/{len(results)} successful")
            
            return batch_result
            
        except Exception as e:
            self.logger.error(f"Batch job {job.job_id} failed: {str(e)}")
            raise
        finally:
            # Clean up
            self.active_jobs.pop(job.job_id, None)
    
    def _process_with_threading(self, job: BatchJob, progress_tracker: ProgressTracker) -> List[ProcessingResult]:
        """Process files using thread-based parallelism."""
        results = []
        
        with ThreadPoolExecutor(max_workers=job.max_workers) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(self._process_single_file, file_path): file_path
                for file_path in job.file_paths
            }
            
            # Collect results
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    progress_tracker.update(result.success)
                except Exception as e:
                    # Create error result
                    error_result = ProcessingResult(
                        file_path=file_path,
                        success=False,
                        error_message=str(e)
                    )
                    results.append(error_result)
                    progress_tracker.update(False)
                    
                    if not job.continue_on_error:
                        self.logger.error(f"Stopping batch due to error in {file_path}: {str(e)}")
                        break
        
        return results
    
    def _process_with_multiprocessing(self, job: BatchJob, progress_tracker: ProgressTracker) -> List[ProcessingResult]:
        """Process files using process-based parallelism."""
        results = []
        
        with ProcessPoolExecutor(max_workers=job.max_workers) as executor:
            # Submit all jobs using the global function
            future_to_file = {
                executor.submit(_process_file_worker, file_path): file_path
                for file_path in job.file_paths
            }
            
            # Collect results
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    progress_tracker.update(result.success)
                except Exception as e:
                    error_result = ProcessingResult(
                        file_path=file_path,
                        success=False,
                        error_message=str(e)
                    )
                    results.append(error_result)
                    progress_tracker.update(False)
                    
                    if not job.continue_on_error:
                        break
        
        return results
    
    def _process_single_file(self, file_path: str) -> ProcessingResult:
        """Process a single SPL file."""
        start_time = time.time()
        
        try:
            # Parse the file
            parse_result = parse_spl_file(file_path)
            processing_time = time.time() - start_time
            
            # Create result
            result = ProcessingResult(
                file_path=file_path,
                success=parse_result.success,
                document_id=parse_result.document.document_id if parse_result.document else None,
                processing_time=processing_time,
                error_message='; '.join(parse_result.errors) if parse_result.errors else None,
                warnings=parse_result.errors if parse_result.success else [],
                document=parse_result.document if parse_result.success else None
            )
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                file_path=file_path,
                success=False,
                processing_time=processing_time,
                error_message=str(e)
            )
    
    def _save_batch_results(self, batch_result: BatchResult, output_directory: str):
        """Save batch results to files."""
        output_path = Path(output_directory)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save summary
        summary_file = output_path / f"{batch_result.job_id}_summary.json"
        summary_data = {
            'job_id': batch_result.job_id,
            'total_files': batch_result.total_files,
            'successful': batch_result.successful,
            'failed': batch_result.failed,
            'success_rate': batch_result.success_rate,
            'total_time': batch_result.total_time,
            'started_at': batch_result.started_at.isoformat(),
            'completed_at': batch_result.completed_at.isoformat()
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        # Save detailed results
        results_file = output_path / f"{batch_result.job_id}_results.jsonl"
        with open(results_file, 'w') as f:
            for result in batch_result.results:
                result_data = asdict(result)
                # Remove document object for serialization
                result_data.pop('document', None)
                f.write(json.dumps(result_data) + '\n')
        
        # Save successful documents
        if any(r.document for r in batch_result.results if r.success):
            docs_dir = output_path / f"{batch_result.job_id}_documents"
            docs_dir.mkdir(exist_ok=True)
            
            for result in batch_result.results:
                if result.success and result.document:
                    doc_file = docs_dir / f"{result.document.document_id}.json"
                    with open(doc_file, 'w') as f:
                        json.dump(result.document.__dict__, f, indent=2, default=str)
    
    def create_job_from_directory(self, directory: str, pattern: str = "*.xml", 
                                job_id: Optional[str] = None) -> BatchJob:
        """Create a batch job from files in a directory."""
        directory_path = Path(directory)
        file_paths = list(str(p) for p in directory_path.glob(pattern))
        
        if not file_paths:
            raise ValueError(f"No files found matching pattern '{pattern}' in {directory}")
        
        job_id = job_id or f"batch_{int(time.time())}"
        
        return BatchJob(
            job_id=job_id,
            file_paths=file_paths
        )
    
    def get_job_progress(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get progress information for an active job."""
        if job_id in self.active_jobs:
            # This would require more sophisticated tracking
            # For now, return basic info
            return {
                'job_id': job_id,
                'status': 'active',
                'total_files': len(self.active_jobs[job_id].file_paths)
            }
        elif job_id in self.results_cache:
            result = self.results_cache[job_id]
            return {
                'job_id': job_id,
                'status': 'completed',
                'total_files': result.total_files,
                'successful': result.successful,
                'failed': result.failed,
                'success_rate': result.success_rate
            }
        else:
            return None
    
    def get_batch_result(self, job_id: str) -> Optional[BatchResult]:
        """Get cached batch result."""
        return self.results_cache.get(job_id)
    
    def clear_cache(self, job_id: Optional[str] = None):
        """Clear result cache."""
        if job_id:
            self.results_cache.pop(job_id, None)
        else:
            self.results_cache.clear()


def _process_file_worker(file_path: str) -> ProcessingResult:
    """Worker function for multiprocessing - must be at module level."""
    processor = BatchProcessor()
    return processor._process_single_file(file_path)


class BatchStatistics:
    """Utility for analyzing batch processing results."""
    
    @staticmethod
    def analyze_results(batch_result: BatchResult) -> Dict[str, Any]:
        """Analyze batch results and provide insights."""
        results = batch_result.results
        
        # Processing time statistics
        processing_times = [r.processing_time for r in results if r.processing_time > 0]
        avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        # Error analysis
        error_types = {}
        for result in results:
            if not result.success and result.error_message:
                error_type = result.error_message.split(':')[0]
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        # File size analysis (if files exist)
        file_sizes = []
        for result in results:
            try:
                if os.path.exists(result.file_path):
                    file_sizes.append(os.path.getsize(result.file_path))
            except:
                pass
        
        return {
            'success_rate': batch_result.success_rate,
            'avg_processing_time': avg_time,
            'max_processing_time': max(processing_times) if processing_times else 0,
            'min_processing_time': min(processing_times) if processing_times else 0,
            'total_processing_time': sum(processing_times),
            'files_per_second': batch_result.total_files / batch_result.total_time if batch_result.total_time > 0 else 0,
            'common_errors': sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5],
            'avg_file_size': sum(file_sizes) / len(file_sizes) if file_sizes else 0,
            'total_files_size': sum(file_sizes)
        }
    
    @staticmethod
    def compare_batches(batch_results: List[BatchResult]) -> Dict[str, Any]:
        """Compare multiple batch results."""
        if not batch_results:
            return {}
        
        success_rates = [br.success_rate for br in batch_results]
        processing_times = [br.total_time for br in batch_results]
        
        return {
            'batch_count': len(batch_results),
            'avg_success_rate': sum(success_rates) / len(success_rates),
            'best_success_rate': max(success_rates),
            'worst_success_rate': min(success_rates),
            'avg_batch_time': sum(processing_times) / len(processing_times),
            'total_files_processed': sum(br.total_files for br in batch_results),
            'total_successful': sum(br.successful for br in batch_results)
        }