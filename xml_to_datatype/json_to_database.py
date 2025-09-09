#!/usr/bin/env python3
"""
JSON to Database Converter

This script reads FDA SPL JSON documents from a source folder and 
inserts them into a PostgreSQL database.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional
import logging
from datetime import datetime
import json

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2.extensions import connection as Connection
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not available. Database functionality will be limited.")

from spl_data_types import SPLDocument
from xml_to_database import DatabaseConfig, SPLDatabaseInserter


class JSONToDatabaseBatchProcessor:
    """Handles batch processing of JSON files to database."""
    
    def __init__(self, source_folder: str, db_config: DatabaseConfig):
        self.source_folder = Path(source_folder)
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        self.inserter = SPLDatabaseInserter(db_config)
    
    def find_json_files(self) -> List[Path]:
        """Find all JSON files in the source folder recursively."""
        json_files = []
        if not self.source_folder.exists():
            self.logger.error(f"Source folder does not exist: {self.source_folder}")
            return json_files
        
        # Look for JSON files recursively
        json_patterns = ['*.json', '*.JSON']
        for pattern in json_patterns:
            json_files.extend(self.source_folder.rglob(pattern))
        
        self.logger.info(f"Found {len(json_files)} JSON files in {self.source_folder}")
        return json_files
    
    def load_json_file(self, json_file: Path) -> Optional[SPLDocument]:
        """Load and parse a JSON file into an SPLDocument."""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Convert JSON data back to SPLDocument
            spl_document = SPLDocument.from_dict(json_data)
            return spl_document
            
        except Exception as e:
            self.logger.error(f"Error loading JSON file {json_file}: {e}")
            return None
    
    def process_file(self, json_file: Path) -> bool:
        """Process a single JSON file and insert into database."""
        try:
            self.logger.info(f"Processing: {json_file}")
            
            # Load JSON document
            spl_document = self.load_json_file(json_file)
            if not spl_document:
                return False
            
            # Insert into database
            success = self.inserter.insert_spl_document(spl_document)
            if success:
                self.logger.info(f"Successfully inserted: {json_file}")
                return True
            else:
                self.logger.error(f"Failed to insert: {json_file}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error processing {json_file}: {e}")
            return False
    
    def process_all(self) -> tuple[int, int]:
        """Process all JSON files and insert into database."""
        # Check if psycopg2 is available
        if not PSYCOPG2_AVAILABLE:
            self.logger.error("psycopg2 not available. Cannot connect to database.")
            return 0, 0
            
        # Connect to database
        if not self.inserter.connect():
            self.logger.error("Failed to connect to database")
            return 0, 0
        
        try:
            json_files = self.find_json_files()
            
            if not json_files:
                self.logger.warning("No JSON files found to process")
                return 0, 0
            
            success_count = 0
            failure_count = 0
            
            for json_file in json_files:
                if self.process_file(json_file):
                    success_count += 1
                else:
                    failure_count += 1
            
            self.logger.info(f"Processing complete: {success_count} succeeded, {failure_count} failed")
            return success_count, failure_count
            
        finally:
            self.inserter.disconnect()


def main():
    """Main function to handle command line arguments and run processing."""
    parser = argparse.ArgumentParser(description='Insert FDA SPL JSON files into database')
    parser.add_argument('source_folder', help='Source folder containing JSON files')
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--database', default='fda_spls', help='Database name (default: fda_spls)')
    parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', default='postgres', help='Database password (default: postgres)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--log-file', help='Log file path (optional)')
    parser.add_argument('--continue-on-error', action='store_true', 
                       help='Continue processing even if some files fail')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    if args.log_file:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(args.log_file),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            level=log_level,
            format=log_format
        )
    
    logger = logging.getLogger(__name__)
    
    # Validate source folder
    if not Path(args.source_folder).exists():
        logger.error(f"Source folder '{args.source_folder}' not found.")
        sys.exit(1)
    
    try:
        # Setup database connection
        db_config = DatabaseConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        # Start processing
        logger.info(f"Starting batch processing from {args.source_folder}")
        start_time = datetime.now()
        
        processor = JSONToDatabaseBatchProcessor(args.source_folder, db_config)
        success_count, failure_count = processor.process_all()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print summary
        print(f"\nBatch processing completed in {duration}")
        print(f"Successfully processed: {success_count} files")
        print(f"Failed processing: {failure_count} files")
        
        if failure_count > 0:
            print("Check the log for details on failed processing.")
            if not args.continue_on_error:
                sys.exit(1)
        else:
            print("All files processed successfully!")
    
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()