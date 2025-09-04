import hashlib
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests
from lxml import html
from tqdm import tqdm

from .models import DownloadMetadata, IngestionConfig, SPLBulkFile


logger = logging.getLogger(__name__)


class DailyMedDownloader:
    """Downloads SPL bulk files from DailyMed"""
    
    def __init__(self, config: Optional[IngestionConfig] = None):
        self.config = config or IngestionConfig()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent
        })
        
        # Ensure download directory exists
        self.config.download_dir.mkdir(parents=True, exist_ok=True)
    
    def discover_bulk_files(self) -> List[SPLBulkFile]:
        """Discover available SPL bulk files from DailyMed"""
        logger.info(f"Discovering bulk files from {self.config.base_url}")
        
        try:
            response = self.session.get(
                self.config.base_url,
                timeout=self.config.timeout
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch bulk files page: {e}")
            raise
        
        # Parse HTML to find bulk file links
        tree = html.fromstring(response.content)
        bulk_files = []
        
        # Look for links in the "Full Releases" section specifically
        # Find the Full Releases section heading
        full_releases_section = tree.xpath('//h3[contains(text(), "Full Releases")]/..')
        
        if not full_releases_section:
            # Fallback: look for any .zip links with SPL-related keywords
            zip_links = tree.xpath('//a[@href[contains(., ".zip")]]')
            logger.info("Full Releases section not found, using fallback link discovery")
        else:
            # Look for .zip links within the Full Releases section
            zip_links = full_releases_section[0].xpath('.//a[@href[contains(., ".zip")]]')
            logger.info(f"Found Full Releases section with {len(zip_links)} zip links")
        
        for link in zip_links:
            href = link.get('href')
            if not href:
                continue
                
            # Convert relative URLs to absolute
            if href.startswith('/'):
                url = urljoin('https://dailymed.nlm.nih.gov', href)
            elif not href.startswith('http'):
                url = urljoin(self.config.base_url, href)
            else:
                url = href
            
            filename = Path(urlparse(url).path).name
            
            # Filter for SPL release files - look for the specific patterns from DailyMed
            if any(keyword in filename.lower() for keyword in [
                'dm_spl_release_human_rx',     # Human prescription
                'dm_spl_release_human_otc',    # Human OTC
                'dm_spl_release_homeopathic',  # Homeopathic
                'dm_spl_release_animal',       # Animal
                'dm_spl_release_remainder',    # Remainder
                'spl_release'                  # General SPL releases
            ]):
                bulk_files.append(SPLBulkFile(
                    filename=filename,
                    url=url
                ))
                logger.debug(f"Added bulk file: {filename}")
            else:
                logger.debug(f"Skipped file (not SPL release): {filename}")
        
        # Remove duplicates by filename (since both HTTP and FTP links exist for each file)
        seen_filenames = set()
        unique_bulk_files = []
        for bulk_file in bulk_files:
            if bulk_file.filename not in seen_filenames:
                seen_filenames.add(bulk_file.filename)
                unique_bulk_files.append(bulk_file)
                
        logger.info(f"Discovered {len(unique_bulk_files)} unique bulk files (filtered from {len(bulk_files)} total links)")
        return unique_bulk_files
    
    def get_file_metadata(self, url: str) -> tuple[Optional[datetime], Optional[int]]:
        """Get file metadata without downloading"""
        try:
            response = self.session.head(url, timeout=self.config.timeout)
            response.raise_for_status()
            
            last_modified = None
            if 'Last-Modified' in response.headers:
                last_modified = datetime.strptime(
                    response.headers['Last-Modified'],
                    '%a, %d %b %Y %H:%M:%S %Z'
                )
            
            size = None
            if 'Content-Length' in response.headers:
                size = int(response.headers['Content-Length'])
            
            return last_modified, size
            
        except requests.RequestException as e:
            logger.warning(f"Failed to get metadata for {url}: {e}")
            return None, None
    
    def calculate_md5(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def download_file(self, bulk_file: SPLBulkFile, force: bool = False) -> Optional[DownloadMetadata]:
        """Download a single bulk file with progress tracking"""
        local_path = self.config.download_dir / bulk_file.filename
        
        # Check if file already exists and skip if not forced
        if local_path.exists() and not force:
            logger.info(f"File {bulk_file.filename} already exists, skipping")
            # Calculate hash of existing file
            md5_hash = self.calculate_md5(local_path)
            return DownloadMetadata(
                filename=bulk_file.filename,
                url=bulk_file.url,
                download_time=datetime.fromtimestamp(local_path.stat().st_mtime),
                file_size=local_path.stat().st_size,
                md5_hash=md5_hash,
                local_path=local_path
            )
        
        logger.info(f"Downloading {bulk_file.filename} from {bulk_file.url}")
        
        for attempt in range(self.config.retry_attempts):
            try:
                response = self.session.get(
                    bulk_file.url, 
                    stream=True,
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                
                # Get file size for progress bar
                total_size = int(response.headers.get('Content-Length', 0))
                
                # Download with progress bar
                with open(local_path, 'wb') as f:
                    with tqdm(
                        total=total_size,
                        unit='iB',
                        unit_scale=True,
                        desc=bulk_file.filename
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=self.config.chunk_size):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                # Calculate hash
                md5_hash = self.calculate_md5(local_path)
                
                metadata = DownloadMetadata(
                    filename=bulk_file.filename,
                    url=bulk_file.url,
                    download_time=datetime.now(),
                    file_size=local_path.stat().st_size,
                    md5_hash=md5_hash,
                    local_path=local_path
                )
                
                logger.info(f"Successfully downloaded {bulk_file.filename} ({metadata.file_size:,} bytes)")
                return metadata
                
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {bulk_file.filename}: {e}")
                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error(f"Failed to download {bulk_file.filename} after {self.config.retry_attempts} attempts")
                    return None
        
        return None
    
    def download_all(self, force: bool = False) -> List[DownloadMetadata]:
        """Discover and download all available bulk files"""
        bulk_files = self.discover_bulk_files()
        
        if not bulk_files:
            logger.warning("No bulk files found to download")
            return []
        
        # Update file metadata
        logger.info("Fetching file metadata...")
        for bulk_file in bulk_files:
            last_modified, size = self.get_file_metadata(bulk_file.url)
            bulk_file.last_modified = last_modified
            bulk_file.size = size
        
        # Download files
        downloaded = []
        for bulk_file in bulk_files:
            metadata = self.download_file(bulk_file, force=force)
            if metadata:
                downloaded.append(metadata)
        
        logger.info(f"Downloaded {len(downloaded)} out of {len(bulk_files)} files")
        return downloaded