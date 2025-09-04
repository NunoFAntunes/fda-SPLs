#!/usr/bin/env python3
"""
Test script for the SPL ingestion component.
This script demonstrates how to use the ingestion module to download SPL data.
"""
import logging
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest.downloader import DailyMedDownloader
from ingest.models import IngestionConfig
from ingest.tracker import VersionTracker


def setup_logging(level=logging.INFO):
    """Set up logging configuration"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('ingestion.log')
        ]
    )


def test_discovery():
    """Test the bulk file discovery functionality"""
    print("=" * 60)
    print("TESTING BULK FILE DISCOVERY")
    print("=" * 60)
    
    config = IngestionConfig(
        download_dir=Path("../data/test_raw"),
        max_concurrent_downloads=1  # Conservative for testing
    )
    
    downloader = DailyMedDownloader(config)
    
    print(f"Discovering files from: {config.base_url}")
    try:
        bulk_files = downloader.discover_bulk_files()
        
        if bulk_files:
            print(f"\nFound {len(bulk_files)} bulk files:")
            for i, bulk_file in enumerate(bulk_files[:10]):  # Show first 10
                print(f"{i+1:2}. {bulk_file.filename}")
                print(f"     URL: {bulk_file.url}")
            
            if len(bulk_files) > 10:
                print(f"     ... and {len(bulk_files) - 10} more files")
        else:
            print("No bulk files found!")
            
    except Exception as e:
        print(f"Error during discovery: {e}")
        return False
    
    return True


def test_metadata_fetch():
    """Test fetching file metadata"""
    print("\n" + "=" * 60)
    print("TESTING METADATA FETCH")
    print("=" * 60)
    
    config = IngestionConfig(download_dir=Path("../data/test_raw"))
    downloader = DailyMedDownloader(config)
    
    try:
        bulk_files = downloader.discover_bulk_files()
        if not bulk_files:
            print("No files to test metadata fetch")
            return False
        
        # Test metadata for first file
        test_file = bulk_files[0]
        print(f"Fetching metadata for: {test_file.filename}")
        
        last_modified, size = downloader.get_file_metadata(test_file.url)
        
        print(f"Last Modified: {last_modified}")
        print(f"Size: {size:,} bytes" if size else "Size: Unknown")
        
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return False
    
    return True


def test_single_download():
    """Test downloading a single file"""
    print("\n" + "=" * 60)
    print("TESTING SINGLE FILE DOWNLOAD")
    print("=" * 60)
    
    config = IngestionConfig(
        download_dir=Path("../data/test_raw"),
        chunk_size=4096  # Smaller chunks for testing
    )
    downloader = DailyMedDownloader(config)
    tracker = VersionTracker(Path("../data/test_metadata/downloads.json"))
    
    try:
        bulk_files = downloader.discover_bulk_files()
        if not bulk_files:
            print("No files available for download test")
            return False
        
        # Find the smallest file for testing
        print("Fetching file sizes...")
        for bulk_file in bulk_files[:5]:  # Check first 5 files
            last_modified, size = downloader.get_file_metadata(bulk_file.url)
            bulk_file.last_modified = last_modified
            bulk_file.size = size
        
        # Sort by size and pick the smallest
        sized_files = [f for f in bulk_files[:5] if f.size is not None]
        if not sized_files:
            print("No files with size information available")
            return False
            
        test_file = min(sized_files, key=lambda x: x.size)
        print(f"Downloading smallest file: {test_file.filename} ({test_file.size:,} bytes)")
        
        # Download the file
        metadata = downloader.download_file(test_file, force=True)
        
        if metadata:
            print(f"[SUCCESS] Downloaded: {metadata.filename}")
            print(f"   Size: {metadata.file_size:,} bytes")
            print(f"   MD5: {metadata.md5_hash}")
            print(f"   Path: {metadata.local_path}")
            
            # Record in tracker
            tracker.record_download(metadata)
            
            # Test version tracking
            print(f"   Version: {tracker.get_file_metadata(metadata.filename).get('version', 'Unknown')}")
            
            return True
        else:
            print("[FAILED] Download failed")
            return False
            
    except Exception as e:
        print(f"Error during download: {e}")
        return False


def test_version_tracking():
    """Test the version tracking functionality"""
    print("\n" + "=" * 60)
    print("TESTING VERSION TRACKING")
    print("=" * 60)
    
    tracker = VersionTracker(Path("../data/test_metadata/downloads.json"))
    
    # Get stats
    stats = tracker.get_stats()
    print(f"Tracked files: {stats['total_files']}")
    print(f"Total size: {stats['total_size']:,} bytes")
    print(f"Latest download: {stats['latest_download']}")
    
    # Show all tracked files
    files = tracker.get_all_files()
    if files:
        print("\nTracked files:")
        for file_meta in files:
            print(f"  - {file_meta['filename']} (v{file_meta.get('version', '?')})")
            print(f"    Size: {file_meta.get('file_size', 0):,} bytes")
            print(f"    Downloaded: {file_meta.get('download_time', 'Unknown')}")
    
    return True


def run_full_test():
    """Run all tests"""
    print("SPL INGESTION COMPONENT TEST")
    print("=" * 60)
    
    tests = [
        ("Discovery", test_discovery),
        ("Metadata Fetch", test_metadata_fetch),
        ("Single Download", test_single_download),
        ("Version Tracking", test_version_tracking),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            print(f"\nRunning {test_name} test...")
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "[PASSED]" if passed else "[FAILED]"
        print(f"{test_name:20} {status}")
    
    total_passed = sum(results.values())
    print(f"\nOverall: {total_passed}/{len(results)} tests passed")
    
    return total_passed == len(results)


if __name__ == "__main__":
    setup_logging()
    
    # Create necessary directories
    Path("../data/test_raw").mkdir(parents=True, exist_ok=True)
    Path("../data/test_metadata").mkdir(parents=True, exist_ok=True)
    
    success = run_full_test()
    sys.exit(0 if success else 1)