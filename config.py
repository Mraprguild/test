import os
from typing import Optional

class Config:
    """Configuration class for environment variables"""
    
    def __init__(self):
        # Telegram API
        self.API_ID = self._get_required_int("API_ID")
        self.API_HASH = self._get_required("API_HASH")
        self.BOT_TOKEN = self._get_required("BOT_TOKEN")
        
        # Wasabi Configuration
        self.WASABI_ACCESS_KEY = self._get_required("WASABI_ACCESS_KEY")
        self.WASABI_SECRET_KEY = self._get_required("WASABI_SECRET_KEY")
        self.WASABI_BUCKET = self._get_required("WASABI_BUCKET")
        self.WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1")
        
        # Admin Configuration
        self.ADMIN_ID = self._get_required_int("ADMIN_ID")
        
        # Web Server Configuration
        self.WEB_SERVER_URL = os.environ.get("WEB_SERVER_URL", "http://localhost:8000")
        
        # GPLinks Configuration
        self.GPLINKS_API_KEY = os.environ.get("GPLINKS_API_KEY", "c1332c0b286628ba047359efde6a5bdac1509655")
        self.AUTO_SHORTEN = os.environ.get("AUTO_SHORTEN", "True").lower() == "true"
        
        # New: Link expiration in days (changed from 7 to 28)
        self.LINK_EXPIRY_DAYS = int(os.environ.get("LINK_EXPIRY_DAYS", "28"))
        
        # New: Performance tuning
        self.CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "32 * 1024 * 1024"))  # 32MB chunks
        self.MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "64"))
        self.BUFFER_SIZE = int(os.environ.get("BUFFER_SIZE", "512 * 1024"))  # 512KB buffer

    def _get_required(self, key: str) -> str:
        """Get required environment variable"""
        value = os.environ.get(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value

    def _get_required_int(self, key: str) -> int:
        """Get required environment variable as integer"""
        value = self._get_required(key)
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Environment variable {key} must be an integer")

# Create config instance
config = Config()
