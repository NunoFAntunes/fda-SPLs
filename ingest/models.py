from datetime import datetime
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field


class DownloadMetadata(BaseModel):
    """Metadata for a downloaded file"""
    filename: str
    url: str
    download_time: datetime
    file_size: int
    md5_hash: str
    local_path: Path


class SPLBulkFile(BaseModel):
    """Represents an SPL bulk file from DailyMed"""
    filename: str
    url: str
    last_modified: Optional[datetime] = None
    size: Optional[int] = None


class IngestionConfig(BaseModel):
    """Configuration for the ingestion process"""
    base_url: str = "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"
    download_dir: Path = Field(default_factory=lambda: Path("data/raw"))
    max_concurrent_downloads: int = 3
    chunk_size: int = 8192
    user_agent: str = "SPL-Ingestion-Pipeline/1.0"
    timeout: int = 300
    retry_attempts: int = 3
    retry_delay: float = 1.0