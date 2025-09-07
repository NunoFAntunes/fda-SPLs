"""
Full bulk import of all SPL data into the database.
Production-ready bulk processing with comprehensive reporting.
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parse.spl_document_parser import parse_spl_file
from parse.database.spl_document_mapper import SPLDocumentMapper, process_spl_document
from parse.database.db_connection import initialize_database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'bulk_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class BulkImportProcessor:
    """Production bulk import processor with comprehensive monitoring."""
    
    def __init__(self, data_directory: str = "../data/extracted", max_workers: int = 4):
        self.data_directory = Path(data_directory)
        self.max_workers = max_workers
        
        # Initialize database
        self.db = initialize_database()
        
        # Processing statistics
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_time': 0,
            'start_time': None,
            'end_time': None,
        }
        
        self.results = []
        self.failed_files = []
    
    def find_all_xml_files(self) -> List[Path]:
        """Find all XML files in the data directory."""
        if not self.data_directory.exists():
            logger.error(f"Data directory not found: {self.data_directory}")
            return []
        
        logger.info(f"Scanning for XML files in: {self.data_directory}")
        xml_files = list(self.data_directory.glob("**/*.xml"))
        
        logger.info(f"Found {len(xml_files)} XML files")
        return xml_files
    
    def process_single_file(self, xml_file: Path) -> Dict[str, Any]:
        """Process a single SPL XML file."""
        start_time = time.time()
        result = {
            'file_path': str(xml_file),
            'file_name': xml_file.name,
            'success': False,
            'skipped': False,
            'spl_id': None,
            'error': None,
            'processing_time': 0,
            'sections_count': 0,
            'products_count': 0,
        }
        
        try:
            # Parse SPL document
            parse_result = parse_spl_file(str(xml_file))
            
            if parse_result.success and parse_result.document:
                document = parse_result.document
                result['spl_id'] = document.document_id
                result['sections_count'] = len(document.sections)
                result['products_count'] = len(document.get_manufactured_products())
                
                # Insert into database
                db_result = process_spl_document(document)
                
                if db_result.success:
                    result['success'] = True
                elif db_result.skipped:
                    result['skipped'] = True
                    result['success'] = True  # Count as success
                    result['error'] = f"Skipped: {db_result.reason}"
                else:
                    result['error'] = db_result.error
                    
            else:
                result['error'] = '; '.join(parse_result.errors) if parse_result.errors else "Parse failed"
        
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Exception processing {xml_file}: {e}")
        
        result['processing_time'] = time.time() - start_time
        return result
    
    def process_all_files(self, limit: int = None) -> Dict[str, Any]:
        """Process all XML files with parallel execution."""
        logger.info("Starting full bulk SPL import...")
        self.stats['start_time'] = datetime.now()
        
        # Find all files
        all_files = self.find_all_xml_files()
        
        if limit:
            all_files = all_files[:limit]
            logger.info(f"Limited to first {limit} files for testing")
        
        self.stats['total_files'] = len(all_files)
        
        if not all_files:
            logger.warning("No XML files found to process")
            return self.get_summary()
        
        logger.info(f"Processing {len(all_files)} files with {self.max_workers} workers...")
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(self.process_single_file, file_path): file_path 
                            for file_path in all_files}
            
            # Process completed tasks with progress reporting
            for i, future in enumerate(as_completed(future_to_file)):
                try:
                    result = future.result()
                    self.results.append(result)
                    
                    # Update statistics
                    if result['success']:
                        if result['skipped']:
                            self.stats['skipped'] += 1
                        else:
                            self.stats['successful'] += 1
                    else:
                        self.stats['failed'] += 1
                        self.failed_files.append(result)
                    
                    # Progress reporting every 100 files or at end
                    if (i + 1) % 100 == 0 or (i + 1) == len(all_files):
                        progress = (i + 1) / len(all_files) * 100
                        logger.info(f"Progress: {i+1}/{len(all_files)} ({progress:.1f}%) - "
                                  f"Success: {self.stats['successful']}, "
                                  f"Failed: {self.stats['failed']}, "
                                  f"Skipped: {self.stats['skipped']}")
                
                except Exception as e:
                    logger.error(f"Error processing future: {e}")
                    self.stats['failed'] += 1
        
        self.stats['end_time'] = datetime.now()
        self.stats['total_time'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info("Bulk processing completed!")
        return self.get_summary()
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate processing summary."""
        total_processed = self.stats['successful'] + self.stats['failed'] + self.stats['skipped']
        
        return {
            'statistics': self.stats.copy(),
            'success_rate': (self.stats['successful'] + self.stats['skipped']) / total_processed * 100 if total_processed > 0 else 0,
            'avg_time_per_file': self.stats['total_time'] / total_processed if total_processed > 0 else 0,
            'files_per_second': total_processed / self.stats['total_time'] if self.stats['total_time'] > 0 else 0,
            'failed_files': self.failed_files[:10],  # First 10 failures for debugging
            'total_failed_count': len(self.failed_files),
        }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get current database statistics."""
        try:
            stats = {}
            
            queries = [
                ("medications", "SELECT COUNT(*) as count FROM medications"),
                ("ingredients", "SELECT COUNT(*) as count FROM ingredients"),
                ("spl_sections", "SELECT COUNT(*) as count FROM spl_sections"),
                ("indications", "SELECT COUNT(*) as count FROM indications"),
                ("additional_ndc_codes", "SELECT COUNT(*) as count FROM additional_ndc_codes"),
            ]
            
            for table, query in queries:
                results = self.db.execute_query(query)
                if results:
                    stats[table] = results[0]['count']
            
            # Get some sample data
            results = self.db.execute_query("""
                SELECT brand_name, manufacturer, ndc_code, product_form
                FROM medications 
                WHERE brand_name IS NOT NULL 
                ORDER BY processed_at DESC
                LIMIT 5
            """)
            
            stats['sample_medications'] = [dict(r) for r in results] if results else []
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def save_results(self, summary: Dict[str, Any]) -> str:
        """Save processing results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"bulk_import_results_{timestamp}.json"
        
        # Prepare data for JSON serialization
        json_data = {
            'summary': summary,
            'database_stats': self.get_database_stats(),
            'processing_results': self.results[:100],  # First 100 for file size
            'failed_files_sample': self.failed_files[:20],  # First 20 failures
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        logger.info(f"Results saved to: {results_file}")
        return results_file


def main():
    """Main execution function."""
    print("FDA SPL Full Bulk Import")
    print("=" * 50)
    
    # Check database connection first
    print("Testing database connection...")
    try:
        db = initialize_database()
        
        # Check current state
        results = db.execute_query("SELECT COUNT(*) as count FROM medications")
        if results:
            current_count = results[0]['count']
            print(f"[OK] Database connected. Current medications: {current_count}")
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return False
    
    # Initialize processor
    processor = BulkImportProcessor(max_workers=6)  # Use 6 workers for better performance
    
    print(f"Data directory: {processor.data_directory}")
    
    # Option to test with limited files first
    test_mode = input("\nTest with first 50 files only? (y/N): ").lower().strip() == 'y'
    limit = 50 if test_mode else None
    
    if test_mode:
        print("Running in test mode with first 50 files...")
    else:
        print("Running full bulk import of ALL files...")
        confirm = input("This will process all XML files. Continue? (y/N): ").lower().strip()
        if confirm != 'y':
            print("Import cancelled.")
            return False
    
    # Start processing
    print(f"\nStarting bulk import at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    summary = processor.process_all_files(limit=limit)
    
    # Display results
    print("\n" + "=" * 60)
    print("BULK IMPORT SUMMARY")
    print("=" * 60)
    
    stats = summary['statistics']
    print(f"📊 Total files processed: {stats['total_files']}")
    print(f"✅ Successful: {stats['successful']}")
    print(f"⊝ Skipped: {stats['skipped']}")
    print(f"❌ Failed: {stats['failed']}")
    print(f"📈 Success rate: {summary['success_rate']:.1f}%")
    print(f"⏱️ Total time: {stats['total_time']:.1f} seconds")
    print(f"⚡ Processing speed: {summary['files_per_second']:.2f} files/second")
    print(f"⌚ Average time per file: {summary['avg_time_per_file']:.3f} seconds")
    
    # Database statistics
    print(f"\n📊 Final Database Contents:")
    db_stats = processor.get_database_stats()
    for table, count in db_stats.items():
        if isinstance(count, int):
            print(f"  - {table}: {count:,} records")
    
    # Sample medications
    if db_stats.get('sample_medications'):
        print(f"\n💊 Sample Medications (most recent):")
        for med in db_stats['sample_medications']:
            brand = med['brand_name'] or 'Unknown'
            mfg = med['manufacturer'] or 'Unknown'
            ndc = med['ndc_code'] or 'N/A'
            form = med['product_form'] or 'N/A'
            print(f"  - {brand} by {mfg} (NDC: {ndc}, Form: {form})")
    
    # Failed files info
    if summary['total_failed_count'] > 0:
        print(f"\n❌ Failed Files ({summary['total_failed_count']} total):")
        for failure in summary['failed_files']:
            print(f"  - {failure['file_name']}: {failure['error']}")
        
        if summary['total_failed_count'] > 10:
            print(f"  ... and {summary['total_failed_count'] - 10} more failures")
    
    # Save detailed results
    results_file = processor.save_results(summary)
    print(f"\n💾 Detailed results saved to: {results_file}")
    
    # Final status
    success_threshold = 80.0  # 80% success rate threshold
    overall_success = summary['success_rate'] >= success_threshold
    
    if overall_success:
        print(f"\n🎉 [SUCCESS] Bulk import completed successfully!")
        print(f"The FDA SPL database is ready for LLM-based queries.")
    else:
        print(f"\n⚠️ [PARTIAL SUCCESS] Import completed with {summary['success_rate']:.1f}% success rate.")
        print(f"Consider investigating failed files for data quality improvements.")
    
    return overall_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)