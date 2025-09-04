#!/usr/bin/env python3
"""
Production ingestion script for downloading SPL data from DailyMed.
This script can be run as a scheduled job to keep SPL data up to date.
"""
import argparse
import logging
import sys
from pathlib import Path

from ingest import DailyMedDownloader, IngestionConfig, VersionTracker


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


def main():
    parser = argparse.ArgumentParser(description='Download FDA SPL data from DailyMed')
    parser.add_argument('--download-dir', type=Path, default=Path('data/raw'),
                       help='Directory to store downloaded files')
    parser.add_argument('--metadata-file', type=Path, default=Path('data/metadata/downloads.json'),
                       help='File to store download metadata')
    parser.add_argument('--force', action='store_true',
                       help='Force re-download of existing files')
    parser.add_argument('--max-concurrent', type=int, default=3,
                       help='Maximum concurrent downloads')
    parser.add_argument('--timeout', type=int, default=300,
                       help='Download timeout in seconds')
    parser.add_argument('--log-file', type=Path,
                       help='Log file path (optional)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--dry-run', action='store_true',
                       help='Discover files but do not download')
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level, args.log_file)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting SPL ingestion process")
    
    try:
        # Create configuration
        config = IngestionConfig(
            download_dir=args.download_dir,
            max_concurrent_downloads=args.max_concurrent,
            timeout=args.timeout
        )
        
        # Create downloader and tracker
        downloader = DailyMedDownloader(config)
        tracker = VersionTracker(args.metadata_file)
        
        # Clean up metadata for missing files
        tracker.cleanup_missing_files()
        
        if args.dry_run:
            logger.info("Running in dry-run mode - discovering files only")
            bulk_files = downloader.discover_bulk_files()
            
            print(f"\nDiscovered {len(bulk_files)} SPL bulk files:")
            for i, bulk_file in enumerate(bulk_files, 1):
                print(f"{i:3}. {bulk_file.filename}")
                if bulk_file.size:
                    print(f"     Size: {bulk_file.size:,} bytes")
                if bulk_file.last_modified:
                    print(f"     Modified: {bulk_file.last_modified}")
            
            # Show current stats
            stats = tracker.get_stats()
            print(f"\nCurrent tracking stats:")
            print(f"  Files tracked: {stats['total_files']}")
            print(f"  Total size: {stats['total_size']:,} bytes")
            print(f"  Latest download: {stats['latest_download']}")
            
        else:
            # Download all files
            logger.info(f"Downloading files to: {config.download_dir}")
            downloaded = downloader.download_all(force=args.force)
            
            # Record downloads in tracker
            for metadata in downloaded:
                tracker.record_download(metadata)
            
            # Show final stats
            stats = tracker.get_stats()
            logger.info(f"Ingestion complete. Downloaded {len(downloaded)} files")
            logger.info(f"Total files tracked: {stats['total_files']}")
            logger.info(f"Total size: {stats['total_size']:,} bytes")
        
    except KeyboardInterrupt:
        logger.info("Ingestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        if args.verbose:
            logger.exception("Full error details:")
        sys.exit(1)
    
    logger.info("SPL ingestion process completed successfully")


if __name__ == '__main__':
    main()