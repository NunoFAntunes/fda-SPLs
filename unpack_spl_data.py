#!/usr/bin/env python3
"""
SPL Data Unpacking Script

This script unpacks the nested ZIP structure from DailyMed SPL releases:
1. Unpacks outer ZIP files (dm_spl_release_*.zip)
2. Discovers folders within (prescription, otc, other, etc.)
3. Unpacks inner ZIP files within each folder
4. Organizes the final XML files in a structured directory

Usage:
    python unpack_spl_data.py --input-dir data/raw --output-dir data/extracted
"""

import argparse
import logging
import shutil
import sys
import zipfile
from pathlib import Path
from typing import List, Optional, Set

import tqdm


def setup_logging(level=logging.INFO, log_file=None):
    """Set up logging configuration"""
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


logger = logging.getLogger(__name__)


class SPLUnpacker:
    """Handles unpacking of nested SPL ZIP files"""
    
    def __init__(self, input_dir: Path, output_dir: Path, temp_dir: Optional[Path] = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.temp_dir = temp_dir or (self.output_dir / "temp")
        
        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Track processed files to avoid duplicates
        self.processed_files: Set[str] = set()
    
    def find_outer_zip_files(self) -> List[Path]:
        """Find all SPL release ZIP files to unpack"""
        patterns = [
            "dm_spl_release_*.zip",
            "spl_release_*.zip"
        ]
        
        zip_files = []
        for pattern in patterns:
            zip_files.extend(self.input_dir.glob(pattern))
        
        # Sort for consistent processing order
        zip_files.sort()
        logger.info(f"Found {len(zip_files)} outer ZIP files to process")
        
        return zip_files
    
    def extract_outer_zip(self, zip_path: Path) -> Optional[Path]:
        """Extract outer ZIP file to temporary directory"""
        extract_path = self.temp_dir / zip_path.stem
        
        # Skip if already extracted
        if extract_path.exists() and any(extract_path.iterdir()):
            logger.info(f"Skipping {zip_path.name} - already extracted")
            return extract_path
        
        extract_path.mkdir(parents=True, exist_ok=True)
        
        try:
            logger.info(f"Extracting outer ZIP: {zip_path.name}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get total files for progress bar
                total_files = len(zip_ref.filelist)
                
                with tqdm.tqdm(total=total_files, desc=f"Extracting {zip_path.name}") as pbar:
                    for member in zip_ref.filelist:
                        zip_ref.extract(member, extract_path)
                        pbar.update(1)
            
            logger.info(f"Successfully extracted {zip_path.name}")
            return extract_path
            
        except zipfile.BadZipFile as e:
            logger.error(f"Bad ZIP file {zip_path.name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to extract {zip_path.name}: {e}")
            return None
    
    def find_category_folders(self, extracted_path: Path) -> List[Path]:
        """Find category folders (prescription, otc, other, etc.) in extracted directory"""
        category_folders = []
        
        for item in extracted_path.iterdir():
            if item.is_dir():
                # Check if this folder contains ZIP files
                zip_files = list(item.glob("*.zip"))
                if zip_files:
                    category_folders.append(item)
                    logger.debug(f"Found category folder: {item.name} with {len(zip_files)} ZIP files")
        
        return category_folders
    
    def extract_inner_zips(self, category_folder: Path, parent_zip_name: str) -> int:
        """Extract all inner ZIP files from a category folder"""
        zip_files = list(category_folder.glob("*.zip"))
        extracted_count = 0
        
        if not zip_files:
            logger.warning(f"No ZIP files found in {category_folder.name}")
            return 0
        
        # Create output directory for this category
        category_output = self.output_dir / parent_zip_name / category_folder.name
        category_output.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Extracting {len(zip_files)} inner ZIP files from {category_folder.name}")
        
        with tqdm.tqdm(zip_files, desc=f"Processing {category_folder.name}") as pbar:
            for zip_file in pbar:
                pbar.set_postfix(file=zip_file.name[:30] + "...")
                
                # Skip if already processed
                if zip_file.name in self.processed_files:
                    logger.debug(f"Skipping {zip_file.name} - already processed")
                    continue
                
                # Create individual folder for this ZIP's contents
                zip_output = category_output / zip_file.stem
                
                if zip_output.exists() and any(zip_output.iterdir()):
                    logger.debug(f"Skipping {zip_file.name} - already extracted")
                    self.processed_files.add(zip_file.name)
                    extracted_count += 1
                    continue
                
                zip_output.mkdir(parents=True, exist_ok=True)
                
                try:
                    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                        zip_ref.extractall(zip_output)
                    
                    self.processed_files.add(zip_file.name)
                    extracted_count += 1
                    
                except zipfile.BadZipFile as e:
                    logger.warning(f"Bad inner ZIP file {zip_file.name}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to extract {zip_file.name}: {e}")
        
        logger.info(f"Successfully extracted {extracted_count} inner ZIP files from {category_folder.name}")
        return extracted_count
    
    def cleanup_temp_files(self):
        """Clean up temporary extraction files"""
        if self.temp_dir.exists():
            logger.info(f"Cleaning up temporary files in {self.temp_dir}")
            shutil.rmtree(self.temp_dir)
    
    def get_extraction_stats(self) -> dict:
        """Get statistics about the extracted files"""
        stats = {
            'total_xml_files': 0,
            'categories': {},
            'parent_zips': []
        }
        
        if not self.output_dir.exists():
            return stats
        
        # Count files by category
        for parent_dir in self.output_dir.iterdir():
            if parent_dir.is_dir() and parent_dir.name != "temp":
                stats['parent_zips'].append(parent_dir.name)
                
                for category_dir in parent_dir.iterdir():
                    if category_dir.is_dir():
                        category_name = category_dir.name
                        xml_count = len(list(category_dir.rglob("*.xml")))
                        
                        stats['total_xml_files'] += xml_count
                        if category_name not in stats['categories']:
                            stats['categories'][category_name] = 0
                        stats['categories'][category_name] += xml_count
        
        return stats
    
    def unpack_all(self, cleanup=True) -> bool:
        """Main method to unpack all SPL data"""
        logger.info(f"Starting SPL data unpacking")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        
        try:
            # Find all outer ZIP files
            outer_zips = self.find_outer_zip_files()
            
            if not outer_zips:
                logger.warning("No SPL release ZIP files found")
                return False
            
            total_extracted = 0
            
            # Process each outer ZIP file
            for outer_zip in outer_zips:
                logger.info(f"\n--- Processing {outer_zip.name} ---")
                
                # Extract outer ZIP
                extracted_path = self.extract_outer_zip(outer_zip)
                if not extracted_path:
                    continue
                
                # Find category folders
                category_folders = self.find_category_folders(extracted_path)
                
                if not category_folders:
                    logger.warning(f"No category folders found in {outer_zip.name}")
                    continue
                
                # Extract inner ZIPs from each category
                for category_folder in category_folders:
                    extracted_count = self.extract_inner_zips(category_folder, outer_zip.stem)
                    total_extracted += extracted_count
            
            # Show final statistics
            stats = self.get_extraction_stats()
            logger.info(f"\n=== Extraction Complete ===")
            logger.info(f"Total inner ZIP files extracted: {total_extracted}")
            logger.info(f"Total XML files: {stats['total_xml_files']}")
            logger.info(f"Categories processed: {list(stats['categories'].keys())}")
            
            for category, count in stats['categories'].items():
                logger.info(f"  {category}: {count} XML files")
            
            return True
            
        except Exception as e:
            logger.error(f"Unpacking failed: {e}")
            return False
        
        finally:
            if cleanup:
                self.cleanup_temp_files()


def main():
    parser = argparse.ArgumentParser(description='Unpack nested SPL ZIP files from DailyMed')
    parser.add_argument('--input-dir', type=Path, default=Path('data/raw'),
                       help='Directory containing downloaded SPL ZIP files')
    parser.add_argument('--output-dir', type=Path, default=Path('data/extracted'),
                       help='Directory to extract files to')
    parser.add_argument('--temp-dir', type=Path,
                       help='Temporary directory for extraction (default: output-dir/temp)')
    parser.add_argument('--log-file', type=Path,
                       help='Log file path (optional)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--no-cleanup', action='store_true',
                       help='Keep temporary files after extraction')
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level, args.log_file)
    
    logger.info("Starting SPL data unpacking process")
    
    try:
        # Create unpacker
        unpacker = SPLUnpacker(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            temp_dir=args.temp_dir
        )
        
        # Perform unpacking
        success = unpacker.unpack_all(cleanup=not args.no_cleanup)
        
        if success:
            logger.info("SPL data unpacking completed successfully")
            sys.exit(0)
        else:
            logger.error("SPL data unpacking failed")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("Unpacking interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unpacking failed: {e}")
        if args.verbose:
            logger.exception("Full error details:")
        sys.exit(1)


if __name__ == '__main__':
    main()