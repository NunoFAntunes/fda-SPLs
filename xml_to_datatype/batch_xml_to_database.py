#!/usr/bin/env python3
"""
Batch XML to Database Processor

This script traverses a folder structure and processes all XML files,
converting them to the PostgreSQL database using the xml_to_database script.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import concurrent.futures
import threading
import time
import logging
from datetime import datetime
import json

from xml_to_database import SPLDatabaseInserter, DatabaseConfig
from spl_xml_to_dataclass import SPLXMLToDataclassConverter
from spl_data_cleaner import create_spl_cleaner


class BatchProcessingStats:
    """Track processing statistics."""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.total_files = 0
        self.processed_files = 0
        self.successful_files = 0
        self.failed_files = 0
        self.skipped_files = 0
        self.start_time = None
        self.end_time = None
        self.errors: List[Dict[str, Any]] = []
        self.duplicates: List[str] = []
    
    def increment_processed(self):
        with self.lock:
            self.processed_files += 1
    
    def increment_successful(self):
        with self.lock:
            self.successful_files += 1
    
    def increment_failed(self, file_path: str, error: str):
        with self.lock:
            self.failed_files += 1
            self.errors.append({
                'file': file_path,
                'error': error,
                'timestamp': datetime.now().isoformat()
            })
    
    def increment_skipped(self, file_path: str, reason: str):
        with self.lock:
            self.skipped_files += 1
            if reason == "duplicate":
                self.duplicates.append(file_path)
    
    def get_summary(self) -> Dict[str, Any]:
        with self.lock:
            elapsed_time = None
            if self.start_time and self.end_time:
                elapsed_time = (self.end_time - self.start_time).total_seconds()
            
            return {
                'total_files': self.total_files,
                'processed_files': self.processed_files,
                'successful_files': self.successful_files,
                'failed_files': self.failed_files,
                'skipped_files': self.skipped_files,
                'duplicates_count': len(self.duplicates),
                'elapsed_time_seconds': elapsed_time,
                'processing_rate': self.processed_files / elapsed_time if elapsed_time and elapsed_time > 0 else 0,
                'success_rate': (self.successful_files / self.processed_files * 100) if self.processed_files > 0 else 0
            }


class BatchXMLProcessor:
    """Processes XML files in batches with multiprocessing support."""
    
    def __init__(self, db_config: DatabaseConfig, max_workers: int = 4, 
                 skip_duplicates: bool = True, continue_on_error: bool = True,
                 enable_cleaning: bool = True, json_output_dir: Optional[str] = None):
        self.db_config = db_config
        self.max_workers = max_workers
        self.skip_duplicates = skip_duplicates
        self.continue_on_error = continue_on_error
        self.enable_cleaning = enable_cleaning
        self.json_output_dir = Path(json_output_dir) if json_output_dir else None
        self.stats = BatchProcessingStats()
        self.logger = logging.getLogger(__name__)
        self.processed_documents: set = set()
        
        # Initialize data cleaner if cleaning is enabled
        self.data_cleaner = create_spl_cleaner() if enable_cleaning else None
        
        # Ensure JSON output directory exists if specified
        if self.json_output_dir:
            self.json_output_dir.mkdir(parents=True, exist_ok=True)
        
    def find_xml_files(self, folder_path: Path) -> List[Path]:
        """Find all XML files in the folder structure."""
        xml_files = []
        
        self.logger.info(f"Scanning for XML files in: {folder_path}")
        
        for xml_file in folder_path.rglob("*.xml"):
            if xml_file.is_file():
                xml_files.append(xml_file)
        
        self.logger.info(f"Found {len(xml_files)} XML files")
        return xml_files
    
    def is_document_already_processed(self, document_id: str) -> bool:
        """Check if document is already in database."""
        if not self.skip_duplicates:
            return False
            
        try:
            inserter = SPLDatabaseInserter(self.db_config, enable_cleaning=False)  # No cleaning for duplicate check
            if not inserter.connect():
                return False
            
            cursor = inserter.connection.cursor()
            cursor.execute("SELECT 1 FROM documents WHERE document_id_root = %s LIMIT 1", (document_id,))
            result = cursor.fetchone()
            cursor.close()
            inserter.disconnect()
            
            return result is not None
            
        except Exception as e:
            self.logger.warning(f"Error checking for duplicate document {document_id}: {e}")
            return False
    
    def process_single_file(self, xml_file: Path) -> Dict[str, Any]:
        """Process a single XML file."""
        result = {
            'file': str(xml_file),
            'success': False,
            'error': None,
            'document_id': None,
            'products_count': 0,
            'processing_time': 0,
            'skipped': False,
            'skip_reason': None
        }
        
        start_time = time.time()
        
        try:
            # Convert XML to dataclass first to get document ID
            self.logger.debug(f"Converting XML: {xml_file}")
            converter = SPLXMLToDataclassConverter(str(xml_file))
            spl_document = converter.convert()
            
            result['document_id'] = spl_document.document.id.root
            result['products_count'] = len(spl_document.manufactured_products)
            
            # Check for duplicates
            if self.is_document_already_processed(spl_document.document.id.root):
                result['skipped'] = True
                result['skip_reason'] = 'duplicate'
                self.stats.increment_skipped(str(xml_file), "duplicate")
                self.logger.debug(f"Skipping duplicate document: {spl_document.document.id.root}")
                return result
            
            # Apply data cleaning pipeline if enabled
            if self.enable_cleaning and self.data_cleaner:
                self.logger.debug(f"Applying data cleaning pipeline to: {xml_file}")
                spl_document = self.data_cleaner.clean_spl_document(spl_document)
            
            # Save JSON if output directory specified
            if self.json_output_dir:
                self._save_json(spl_document, xml_file)
            
            # Insert into database
            inserter = SPLDatabaseInserter(self.db_config, enable_cleaning=False, json_output_dir=None)  # Cleaning already done
            if not inserter.connect():
                raise Exception("Failed to connect to database")
            
            try:
                success = inserter.insert_spl_document(spl_document)
                if success:
                    result['success'] = True
                    self.stats.increment_successful()
                    self.processed_documents.add(spl_document.document.id.root)
                    self.logger.debug(f"Successfully processed: {xml_file}")
                else:
                    raise Exception("Database insertion failed")
            finally:
                inserter.disconnect()
                
        except Exception as e:
            result['error'] = str(e)
            self.stats.increment_failed(str(xml_file), str(e))
            self.logger.error(f"Error processing {xml_file}: {e}")
            
            if not self.continue_on_error:
                raise
        
        finally:
            result['processing_time'] = time.time() - start_time
            self.stats.increment_processed()
        
        return result
    
    def _save_json(self, spl_doc, xml_file: Path):
        """Save SPL document as JSON file."""
        if not self.json_output_dir:
            return
        
        try:
            # Generate JSON filename based on original XML filename
            json_filename = xml_file.stem + '.json'
            json_path = self.json_output_dir / json_filename
            
            # Convert to JSON
            json_content = spl_doc.to_json(indent=2)
            
            # Write JSON file
            with open(json_path, 'w', encoding='utf-8') as f:
                f.write(json_content)
            
            self.logger.debug(f"Saved JSON: {json_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to save JSON for {xml_file}: {e}")
    
    def process_files_sequential(self, xml_files: List[Path]) -> List[Dict[str, Any]]:
        """Process files sequentially (single-threaded)."""
        results = []
        
        for i, xml_file in enumerate(xml_files, 1):
            self.logger.info(f"Processing {i}/{len(xml_files)}: {xml_file.name}")
            result = self.process_single_file(xml_file)
            results.append(result)
            
            # Progress update
            if i % 10 == 0 or i == len(xml_files):
                summary = self.stats.get_summary()
                self.logger.info(f"Progress: {i}/{len(xml_files)} files, "
                               f"{summary['successful_files']} successful, "
                               f"{summary['failed_files']} failed, "
                               f"{summary['skipped_files']} skipped")
        
        return results
    
    def process_files_parallel(self, xml_files: List[Path]) -> List[Dict[str, Any]]:
        """Process files in parallel using thread pool."""
        results = []
        completed = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(self.process_single_file, xml_file): xml_file 
                            for xml_file in xml_files}
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_file):
                xml_file = future_to_file[future]
                completed += 1
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        self.logger.debug(f"✓ {xml_file.name}")
                    elif result['skipped']:
                        self.logger.debug(f"⊘ {xml_file.name} (duplicate)")
                    else:
                        self.logger.warning(f"✗ {xml_file.name}: {result['error']}")
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {xml_file}: {e}")
                    results.append({
                        'file': str(xml_file),
                        'success': False,
                        'error': f"Unexpected error: {e}",
                        'document_id': None,
                        'products_count': 0,
                        'processing_time': 0,
                        'skipped': False,
                        'skip_reason': None
                    })
                
                # Progress update
                if completed % 10 == 0 or completed == len(xml_files):
                    summary = self.stats.get_summary()
                    self.logger.info(f"Progress: {completed}/{len(xml_files)} files, "
                                   f"{summary['successful_files']} successful, "
                                   f"{summary['failed_files']} failed, "
                                   f"{summary['skipped_files']} skipped")
        
        return results
    
    def process_folder(self, folder_path: Path, parallel: bool = True) -> Dict[str, Any]:
        """Process all XML files in a folder."""
        self.logger.info(f"Starting batch processing of folder: {folder_path}")
        self.stats.start_time = datetime.now()
        
        # Find all XML files
        xml_files = self.find_xml_files(folder_path)
        self.stats.total_files = len(xml_files)
        
        if not xml_files:
            self.logger.warning("No XML files found in the specified folder")
            return self.stats.get_summary()
        
        # Process files
        if parallel and self.max_workers > 1:
            self.logger.info(f"Processing files in parallel with {self.max_workers} workers")
            results = self.process_files_parallel(xml_files)
        else:
            self.logger.info("Processing files sequentially")
            results = self.process_files_sequential(xml_files)
        
        self.stats.end_time = datetime.now()
        
        # Generate final summary
        summary = self.stats.get_summary()
        self.logger.info("=" * 60)
        self.logger.info("BATCH PROCESSING COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Total files found: {summary['total_files']}")
        self.logger.info(f"Files processed: {summary['processed_files']}")
        self.logger.info(f"Successfully imported: {summary['successful_files']}")
        self.logger.info(f"Failed: {summary['failed_files']}")
        self.logger.info(f"Skipped (duplicates): {summary['skipped_files']}")
        self.logger.info(f"Success rate: {summary['success_rate']:.1f}%")
        self.logger.info(f"Processing time: {summary['elapsed_time_seconds']:.1f} seconds")
        self.logger.info(f"Processing rate: {summary['processing_rate']:.2f} files/second")
        
        return {
            'summary': summary,
            'results': results,
            'errors': self.stats.errors,
            'duplicates': self.stats.duplicates
        }
    
    def save_report(self, results: Dict[str, Any], output_file: Path):
        """Save processing report to JSON file."""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"Processing report saved to: {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save report: {e}")


def main():
    """Main function to handle command line arguments and run batch processing."""
    parser = argparse.ArgumentParser(description='Batch process FDA SPL XML files to database with enhanced cleaning and JSON export')
    parser.add_argument('folder', help='Input folder path containing XML files')
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--database', default='fda_spls', help='Database name (default: fda_spls)')
    parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', default='postgres', help='Database password (default: postgres)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4, use 1 for sequential)')
    parser.add_argument('--no-skip-duplicates', action='store_true', help='Process duplicate documents (default: skip)')
    parser.add_argument('--stop-on-error', action='store_true', help='Stop processing on first error (default: continue)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--report', help='Save processing report to JSON file')
    parser.add_argument('--no-cleaning', action='store_true', 
                       help='Disable data cleaning pipeline (faster but less normalized data)')
    parser.add_argument('--json-output', help='Directory to save JSON files (optional)')
    parser.add_argument('--cleaning-only', action='store_true',
                       help='Enable detailed cleaning logs for debugging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if (args.verbose or args.cleaning_only) else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('batch_processing.log', encoding='utf-8')
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Validate input folder
    folder_path = Path(args.folder)
    if not folder_path.exists():
        logger.error(f"Input folder '{args.folder}' not found.")
        sys.exit(1)
    
    if not folder_path.is_dir():
        logger.error(f"Input path '{args.folder}' is not a directory.")
        sys.exit(1)
    
    try:
        # Setup database configuration
        db_config = DatabaseConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        # Test database connection
        logger.info("Testing database connection...")
        inserter = SPLDatabaseInserter(db_config, enable_cleaning=False)
        if not inserter.connect():
            logger.error("Failed to connect to database")
            sys.exit(1)
        inserter.disconnect()
        logger.info("Database connection successful")
        
        # Setup enhanced processing options
        enable_cleaning = not args.no_cleaning
        cleaning_status = "enabled" if enable_cleaning else "disabled"
        logger.info(f"Data cleaning pipeline: {cleaning_status}")
        
        if args.json_output:
            logger.info(f"JSON files will be saved to: {args.json_output}")
        
        # Setup batch processor
        processor = BatchXMLProcessor(
            db_config=db_config,
            max_workers=args.workers,
            skip_duplicates=not args.no_skip_duplicates,
            continue_on_error=not args.stop_on_error,
            enable_cleaning=enable_cleaning,
            json_output_dir=args.json_output
        )
        
        # Process folder
        results = processor.process_folder(folder_path, parallel=(args.workers > 1))
        
        # Save report if requested
        if args.report:
            processor.save_report(results, Path(args.report))
        
        # Print error summary
        if results['summary']['failed_files'] > 0:
            logger.error("\n" + "=" * 60)
            logger.error("PROCESSING ERRORS:")
            logger.error("=" * 60)
            for error in results['errors'][-10:]:  # Show last 10 errors
                logger.error(f"FILE: {error['file']}")
                logger.error(f"ERROR: {error['error']}")
                logger.error("-" * 40)
            
            if len(results['errors']) > 10:
                logger.error(f"... and {len(results['errors']) - 10} more errors (see report for full list)")
        
        # Exit with appropriate code
        if results['summary']['failed_files'] > 0:
            logger.warning("Some files failed to process. Check logs for details.")
            sys.exit(1)
        else:
            logger.info("All files processed successfully!")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()