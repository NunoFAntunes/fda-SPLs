"""
SPL Ingestion Module

This module provides functionality to download and track FDA Structured Product Labels (SPLs)
from DailyMed's bulk data resources.

Key components:
- DailyMedDownloader: Downloads SPL bulk files from DailyMed
- VersionTracker: Tracks file versions and changes using MD5 hashes
- IngestionConfig: Configuration for the ingestion process
"""

from .downloader import DailyMedDownloader
from .models import DownloadMetadata, IngestionConfig, SPLBulkFile
from .tracker import VersionTracker

__all__ = [
    'DailyMedDownloader',
    'VersionTracker',
    'IngestionConfig',
    'DownloadMetadata',
    'SPLBulkFile'
]

__version__ = '1.0.0'