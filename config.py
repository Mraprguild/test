import os
from typing import List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    """High-performance configuration"""
    
    # Telegram API
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # Wasabi Configuration
    WASABI_ACCESS_KEY: str = os.getenv("WASABI_ACCESS_KEY", "")
    WASABI_SECRET_KEY: str = os.getenv("WASABI_SECRET_KEY", "")
    WASABI_BUCKET: str = os.getenv("WASABI_BUCKET", "")
    WASABI_REGION: str = os.getenv("WASABI_REGION", "us-east-1")
    
    # Admin
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", 0))
    
    # Performance Tuning - CRITICAL FOR SPEED
    CHUNK_SIZE: int = 64 * 1024 * 1024  # 64MB for maximum throughput
    MAX_WORKERS: int = min(64, (os.cpu_count() or 1) * 8)  # Aggressive threading
    BUFFER_SIZE: int = 1024 * 1024  # 1MB buffer
    CONNECTION_POOL_SIZE: int = 100
    MAX_CONCURRENT_UPLOADS: int = 10
    
    # S3 Optimizations
    MULTIPART_THRESHOLD: int = 100 * 1024 * 1024  # 100MB - use multipart
    MULTIPART_CHUNKSIZE: int = 64 * 1024 * 1024  # 64MB per part
    MAX_PARTS: int = 10000
    
    # Timeouts
    S3_TIMEOUT: int = 3600  # 1 hour for large files
    S3_CONNECT_TIMEOUT: int = 30
    
    # Maximum file size (20GB)
    MAX_FILE_SIZE: int = 20 * 1024 * 1024 * 1024
    
    # Web Server
    WEB_SERVER_URL: str = os.getenv("RENDER_URL", "http://localhost:8000")
    WEB_PORT: int = int(os.getenv("PORT", 8000))
    
    # URL options
    PRESIGNED_URL_EXPIRY: int = 604800  # 7 days
    DIRECT_DOWNLOAD: bool = True  # Direct Wasabi URLs
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        required = ['API_ID', 'API_HASH', 'BOT_TOKEN', 
                   'WASABI_ACCESS_KEY', 'WASABI_SECRET_KEY', 
                   'WASABI_BUCKET', 'ADMIN_ID']
        
        for req in required:
            if not getattr(cls, req):
                raise ValueError(f"{req} is required")

config = Config()
config.validate()
