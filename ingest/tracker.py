import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import DownloadMetadata


logger = logging.getLogger(__name__)


class VersionTracker:
    """Tracks versions and changes of downloaded SPL files"""
    
    def __init__(self, metadata_file: Path = None):
        self.metadata_file = metadata_file or Path("data/metadata/downloads.json")
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        self._metadata: Dict[str, Dict] = {}
        self.load_metadata()
    
    def load_metadata(self):
        """Load existing metadata from file"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)
                    # Convert datetime strings back to datetime objects
                    for filename, metadata in data.items():
                        if 'download_time' in metadata:
                            metadata['download_time'] = datetime.fromisoformat(metadata['download_time'])
                    self._metadata = data
                logger.info(f"Loaded metadata for {len(self._metadata)} files")
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load metadata file: {e}")
                self._metadata = {}
        else:
            logger.info("No existing metadata file found, starting fresh")
    
    def save_metadata(self):
        """Save metadata to file"""
        try:
            # Convert datetime objects to strings for JSON serialization
            serializable_data = {}
            for filename, metadata in self._metadata.items():
                serializable_metadata = metadata.copy()
                if 'download_time' in serializable_metadata and isinstance(serializable_metadata['download_time'], datetime):
                    serializable_metadata['download_time'] = serializable_metadata['download_time'].isoformat()
                if 'local_path' in serializable_metadata and isinstance(serializable_metadata['local_path'], Path):
                    serializable_metadata['local_path'] = str(serializable_metadata['local_path'])
                serializable_data[filename] = serializable_metadata
            
            with open(self.metadata_file, 'w') as f:
                json.dump(serializable_data, f, indent=2, default=str)
            logger.info(f"Saved metadata to {self.metadata_file}")
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def record_download(self, metadata: DownloadMetadata):
        """Record a successful download"""
        self._metadata[metadata.filename] = {
            'filename': metadata.filename,
            'url': metadata.url,
            'download_time': metadata.download_time,
            'file_size': metadata.file_size,
            'md5_hash': metadata.md5_hash,
            'local_path': str(metadata.local_path),
            'version': self.get_next_version(metadata.filename)
        }
        self.save_metadata()
        logger.info(f"Recorded download for {metadata.filename}")
    
    def get_next_version(self, filename: str) -> int:
        """Get the next version number for a file"""
        if filename in self._metadata:
            return self._metadata[filename].get('version', 1) + 1
        return 1
    
    def has_file_changed(self, metadata: DownloadMetadata) -> bool:
        """Check if a file has changed since last download"""
        if metadata.filename not in self._metadata:
            return True
        
        stored_metadata = self._metadata[metadata.filename]
        
        # Compare file size and hash
        if (stored_metadata.get('file_size') != metadata.file_size or
            stored_metadata.get('md5_hash') != metadata.md5_hash):
            return True
        
        return False
    
    def get_file_metadata(self, filename: str) -> Optional[Dict]:
        """Get stored metadata for a file"""
        return self._metadata.get(filename)
    
    def get_all_files(self) -> List[Dict]:
        """Get metadata for all tracked files"""
        return list(self._metadata.values())
    
    def cleanup_missing_files(self):
        """Remove metadata for files that no longer exist locally"""
        to_remove = []
        for filename, metadata in self._metadata.items():
            local_path = Path(metadata.get('local_path', ''))
            if not local_path.exists():
                to_remove.append(filename)
                logger.info(f"Marking {filename} for removal - file no longer exists")
        
        for filename in to_remove:
            del self._metadata[filename]
        
        if to_remove:
            self.save_metadata()
            logger.info(f"Cleaned up metadata for {len(to_remove)} missing files")
    
    def get_download_history(self, filename: str) -> List[Dict]:
        """Get download history for a specific file (if versioning is implemented)"""
        # For now, just return the current metadata
        # This could be extended to track full version history
        metadata = self.get_file_metadata(filename)
        return [metadata] if metadata else []
    
    def get_stats(self) -> Dict:
        """Get statistics about tracked downloads"""
        if not self._metadata:
            return {
                'total_files': 0,
                'total_size': 0,
                'latest_download': None
            }
        
        total_size = sum(meta.get('file_size', 0) for meta in self._metadata.values())
        
        latest_download = None
        for meta in self._metadata.values():
            download_time = meta.get('download_time')
            if isinstance(download_time, str):
                download_time = datetime.fromisoformat(download_time)
            if download_time and (not latest_download or download_time > latest_download):
                latest_download = download_time
        
        return {
            'total_files': len(self._metadata),
            'total_size': total_size,
            'latest_download': latest_download
        }