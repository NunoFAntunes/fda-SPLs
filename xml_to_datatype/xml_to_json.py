#!/usr/bin/env python3
"""
XML to JSON Converter

This script converts FDA SPL XML documents in a source folder to JSON format
and saves them to a destination folder.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional
import logging
from datetime import datetime
import json

from spl_xml_to_dataclass import SPLXMLToDataclassConverter
from spl_data_cleaner import create_spl_cleaner


class XMLToJSONBatchConverter:
    """Handles batch conversion of XML files to JSON format."""
    
    def __init__(self, source_folder: str, destination_folder: str, enable_cleaning: bool = True):
        self.source_folder = Path(source_folder)
        self.destination_folder = Path(destination_folder)
        self.enable_cleaning = enable_cleaning
        self.logger = logging.getLogger(__name__)
        
        # Initialize data cleaner if cleaning is enabled
        self.data_cleaner = create_spl_cleaner() if enable_cleaning else None
        
        # Ensure destination folder exists
        self.destination_folder.mkdir(parents=True, exist_ok=True)
    
    def find_xml_files(self) -> List[Path]:
        """Find all XML files in the source folder recursively."""
        xml_files = []
        if not self.source_folder.exists():
            self.logger.error(f"Source folder does not exist: {self.source_folder}")
            return xml_files
        
        # Look for XML files recursively
        xml_patterns = ['*.xml', '*.XML']
        for pattern in xml_patterns:
            xml_files.extend(self.source_folder.rglob(pattern))
        
        self.logger.info(f"Found {len(xml_files)} XML files in {self.source_folder}")
        return xml_files
    
    def convert_file(self, xml_file: Path) -> bool:
        """Convert a single XML file to JSON."""
        try:
            # Generate JSON file path
            relative_path = xml_file.relative_to(self.source_folder)
            json_file = self.destination_folder / relative_path.with_suffix('.json')
            
            # Ensure the JSON file's parent directory exists
            json_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert XML to dataclass
            self.logger.info(f"Converting: {xml_file}")
            converter = SPLXMLToDataclassConverter(str(xml_file))
            spl_document = converter.convert()
            
            # Apply data cleaning pipeline if enabled
            if self.enable_cleaning and self.data_cleaner:
                self.logger.debug(f"Applying data cleaning pipeline to: {xml_file}")
                spl_document = self.data_cleaner.clean_spl_document(spl_document)
            
            # Convert to JSON
            json_content = spl_document.to_json(indent=2)
            
            # Write JSON file
            with open(json_file, 'w', encoding='utf-8') as f:
                f.write(json_content)
            
            self.logger.info(f"Successfully converted to: {json_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error converting {xml_file}: {e}")
            return False
    
    def convert_all(self) -> tuple[int, int]:
        """Convert all XML files to JSON format."""
        xml_files = self.find_xml_files()
        
        if not xml_files:
            self.logger.warning("No XML files found to convert")
            return 0, 0
        
        success_count = 0
        failure_count = 0
        
        for xml_file in xml_files:
            if self.convert_file(xml_file):
                success_count += 1
            else:
                failure_count += 1
        
        self.logger.info(f"Conversion complete: {success_count} succeeded, {failure_count} failed")
        return success_count, failure_count


def main():
    """Main function to handle command line arguments and run conversion."""
    parser = argparse.ArgumentParser(description='Convert FDA SPL XML files to JSON format')
    parser.add_argument('source_folder', help='Source folder containing XML files')
    parser.add_argument('destination_folder', help='Destination folder for JSON files')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--log-file', help='Log file path (optional)')
    parser.add_argument('--no-cleaning', action='store_true', 
                       help='Disable data cleaning pipeline (faster but less normalized data)')
    parser.add_argument('--cleaning-only', action='store_true',
                       help='Enable detailed cleaning logs for debugging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if (args.verbose or args.cleaning_only) else logging.INFO
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
        # Start conversion
        enable_cleaning = not args.no_cleaning
        cleaning_status = "enabled" if enable_cleaning else "disabled"
        logger.info(f"Starting batch conversion from {args.source_folder} to {args.destination_folder}")
        logger.info(f"Data cleaning pipeline: {cleaning_status}")
        start_time = datetime.now()
        
        converter = XMLToJSONBatchConverter(args.source_folder, args.destination_folder, enable_cleaning)
        success_count, failure_count = converter.convert_all()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print summary
        print(f"\nBatch conversion completed in {duration}")
        print(f"Successfully converted: {success_count} files")
        print(f"Failed conversions: {failure_count} files")
        
        if failure_count > 0:
            print("Check the log for details on failed conversions.")
            sys.exit(1)
        else:
            print("All files converted successfully!")
    
    except Exception as e:
        logger.error(f"Error during batch conversion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()