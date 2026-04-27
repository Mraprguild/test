import os
import time
import math
import asyncio
import logging
import base64
import aiofiles
import requests
from functools import wraps
from urllib.parse import quote
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import hashlib
from dataclasses import dataclass
from typing import Optional, Tuple
from collections import deque

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config as BotoConfig
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from flask import Flask, render_template, request, jsonify, send_file

# Import configuration
from config import config

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Use configuration from config module
API_ID = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = config.BOT_TOKEN
WASABI_ACCESS_KEY = config.WASABI_ACCESS_KEY
WASABI_SECRET_KEY = config.WASABI_SECRET_KEY
WASABI_BUCKET = config.WASABI_BUCKET
WASABI_REGION = config.WASABI_REGION
ADMIN_ID = config.ADMIN_ID

# Link expiration (28 days)
LINK_EXPIRY_SECONDS = config.LINK_EXPIRY_DAYS * 24 * 60 * 60

# GPLinks.in Configuration
GPLINKS_API_KEY = getattr(config, 'GPLINKS_API_KEY', '')
GPLINKS_API_URL = "https://gplinks.in/api"
AUTO_SHORTEN = getattr(config, 'AUTO_SHORTEN', True)

# Player URL configuration
RENDER_URL = os.getenv("RENDER_URL", "http://localhost:8000")
SUPPORTED_VIDEO_FORMATS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.mpeg', '.mpg'}
SUPPORTED_AUDIO_FORMATS = {'.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac'}
SUPPORTED_IMAGE_FORMATS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}

# Performance optimization settings
CHUNK_SIZE = 32 * 1024 * 1024  # 32MB chunks for parallel upload (increased from 16MB)
MAX_WORKERS = min(64, (os.cpu_count() or 1) * 4)  # Increased thread count
BUFFER_SIZE = 512 * 1024  # 512KB buffer for file operations

# Thread pools for parallel operations
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
process_pool = ProcessPoolExecutor(max_workers=max(1, (os.cpu_count() or 1) // 2))

# In-memory storage for authorized user IDs
ALLOWED_USERS = {ADMIN_ID}

# --- Cache System for Performance ---
class LRUCache:
    """Simple LRU Cache implementation"""
    def __init__(self, maxsize=1000):
        self.maxsize = maxsize
        self.cache = {}
        self.order = deque()
    
    def get(self, key):
        if key in self.cache:
            self.order.remove(key)
            self.order.append(key)
            return self.cache[key]
        return None
    
    def set(self, key, value):
        if key in self.cache:
            self.order.remove(key)
        elif len(self.cache) >= self.maxsize:
            oldest = self.order.popleft()
            del self.cache[oldest]
        self.cache[key] = value
        self.order.append(key)
    
    def clear(self):
        self.cache.clear()
        self.order.clear()

# Initialize caches
url_cache = LRUCache(maxsize=500)
presigned_url_cache = LRUCache(maxsize=200)

# --- Callback Data Management ---
class CallbackData:
    """Manage callback data to avoid exceeding 64-byte limit"""
    def __init__(self):
        self.file_map = {}
        self.next_id = 1
        self.ttl = 3600  # 1 hour TTL
        self.timestamps = {}
    
    def store_file(self, filename):
        short_id = str(self.next_id)
        self.file_map[short_id] = filename
        self.timestamps[short_id] = time.time()
        self.next_id += 1
        
        if len(self.file_map) > 1000:
            self._cleanup()
        return short_id
    
    def _cleanup(self):
        current_time = time.time()
        to_delete = [k for k, v in self.timestamps.items() if current_time - v > self.ttl]
        for k in to_delete:
            if k in self.file_map:
                del self.file_map[k]
            if k in self.timestamps:
                del self.timestamps[k]
    
    def get_file(self, short_id):
        if short_id in self.timestamps:
            if time.time() - self.timestamps[short_id] <= self.ttl:
                return self.file_map.get(short_id)
            else:
                self.clear_file(short_id)
        return None
    
    def clear_file(self, short_id):
        if short_id in self.file_map:
            del self.file_map[short_id]
        if short_id in self.timestamps:
            del self.timestamps[short_id]

callback_data = CallbackData()

# --- Bot & Wasabi Client Initialization ---
# Optimized Boto3 config for maximum performance
boto_config = BotoConfig(
    max_pool_connections=MAX_WORKERS,
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    read_timeout=600,
    connect_timeout=60,
    tcp_keepalive=True,
)

try:
    session = boto3.Session(
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=WASABI_REGION
    )
    
    s3_client = session.client(
        's3',
        endpoint_url=f'https://s3.{WASABI_REGION}.wasabisys.com',
        config=boto_config
    )
    
    # Test connection
    s3_client.head_bucket(Bucket=WASABI_BUCKET)
    logger.info(f"✅ Successfully connected to Wasabi with {MAX_WORKERS} workers")
    logger.info(f"📦 Using {CHUNK_SIZE // 1024 // 1024}MB chunks for multipart upload")
except Exception as e:
    logger.error(f"❌ Failed to connect to Wasabi: {e}")
    s3_client = None

# Initialize Pyrogram Client
app = Client("wasabi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- Performance Tracking ---
@dataclass
class TransferStats:
    start_time: float = 0
    bytes_transferred: int = 0
    last_update: float = 0
    speed_samples: deque = None
    
    def __post_init__(self):
        self.speed_samples = deque(maxlen=10)
    
    def start(self):
        self.start_time = time.time()
        self.bytes_transferred = 0
        self.last_update = self.start_time
        self.speed_samples.clear()
        
    def update(self, bytes_count):
        self.bytes_transferred += bytes_count
        now = time.time()
        if now - self.last_update >= 0.5:  # Update speed every 0.5 seconds
            instant_speed = bytes_count / (now - self.last_update) if (now - self.last_update) > 0 else 0
            self.speed_samples.append(instant_speed)
            self.last_update = now
        
    def get_speed(self):
        if not self.speed_samples:
            return "0 B/s"
        avg_speed = sum(self.speed_samples) / len(self.speed_samples)
        return self.human_speed(avg_speed)
    
    def get_eta(self, total_size):
        if self.bytes_transferred <= 0:
            return "Calculating..."
        elapsed = time.time() - self.start_time
        if elapsed <= 0:
            return "∞"
        speed = self.bytes_transferred / elapsed
        if speed <= 0:
            return "∞"
        remaining = total_size - self.bytes_transferred
        eta_seconds = remaining / speed
        return self.format_time(eta_seconds)
    
    @staticmethod
    def format_time(seconds):
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.0f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    @staticmethod
    def human_speed(speed):
        for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
            if speed < 1024.0:
                return f"{speed:.2f} {unit}"
            speed /= 1024.0
        return f"{speed:.2f} TB/s"

transfer_stats = TransferStats()

# --- Progress Cache ---
last_update_time = {}
progress_cache = {}

async def progress_callback(current, total, message, status):
    """High-performance progress updates with speed tracking."""
    chat_id = message.chat.id
    message_id = message.id if message else id(message)
    
    if message_id not in progress_cache:
        progress_cache[message_id] = 0
    
    transfer_stats.update(current - progress_cache[message_id])
    progress_cache[message_id] = current
    
    # Throttle UI updates (every 0.5 seconds or when complete)
    now = time.time()
    if (now - last_update_time.get(message_id, 0)) < 0.5 and current != total:
        return
    
    last_update_time[message_id] = now

    percentage = (current * 100) / total if total > 0 else 0
    filled_length = int(15 * percentage / 100)
    bar = '█' * filled_length + '░' * (15 - filled_length)
    
    speed = transfer_stats.get_speed()
    eta = transfer_stats.get_eta(total)
    
    details = (
        f"**{status}**\n"
        f"`{bar}` `{percentage:.1f}%`\n"
        f"**Speed:** `{speed}` | **ETA:** `{eta}`\n"
        f"**{humanbytes(current)}** / **{humanbytes(total)}**"
    )
    
    try:
        await app.edit_message_text(chat_id, message_id, text=details)
    except Exception as e:
        logger.debug(f"Progress update skipped: {e}")

# --- Helper Functions ---
def humanbytes(size):
    """Convert bytes to human readable format"""
    if not size:
        return "0 B"
    size = int(size)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def get_file_extension(filename):
    return os.path.splitext(filename)[1].lower()

def get_media_type(filename):
    ext = get_file_extension(filename)
    if ext in SUPPORTED_VIDEO_FORMATS:
        return 'video'
    elif ext in SUPPORTED_AUDIO_FORMATS:
        return 'audio'
    elif ext in SUPPORTED_IMAGE_FORMATS:
        return 'image'
    return 'file'

def is_video_file(filename):
    return get_media_type(filename) == 'video'

async def shorten_url_gplinks(long_url):
    """Shorten URL using GPLinks.in API with caching"""
    if not GPLINKS_API_KEY or not AUTO_SHORTEN:
        return long_url
    
    # Check cache
    cache_key = hashlib.md5(long_url.encode()).hexdigest()
    cached = url_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        api_url = f"{GPLINKS_API_URL}?api={GPLINKS_API_KEY}&url={quote(long_url)}"
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                shortened_url = data.get('shortenedUrl')
                if shortened_url:
                    url_cache.set(cache_key, shortened_url)
                    return shortened_url
    except Exception as e:
        logger.error(f"GPLinks shortening failed: {e}")
    
    return long_url

async def generate_presigned_url(file_name):
    """Generate presigned URL with 28-day expiry and caching"""
    # Check cache
    cached = presigned_url_cache.get(file_name)
    if cached:
        return cached
    
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': file_name},
            ExpiresIn=LINK_EXPIRY_SECONDS
        )
        presigned_url_cache.set(file_name, url)
        return url
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        return None

def generate_player_url(filename, presigned_url):
    """Generate player URL for supported media types"""
    if not RENDER_URL or not presigned_url:
        return None
    
    media_type = get_media_type(filename)
    if media_type in ['video', 'audio', 'image']:
        encoded_url = base64.urlsafe_b64encode(presigned_url.encode()).decode().rstrip('=')
        return f"{RENDER_URL}/player/{media_type}/{encoded_url}"
    return None

def get_expiry_date():
    """Get formatted expiry date"""
    expiry_timestamp = time.time() + LINK_EXPIRY_SECONDS
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.localtime(expiry_timestamp))

# --- Modern UI Buttons ---
async def create_modern_buttons(direct_url, player_url, filename, is_admin=False):
    """Create modern styled inline buttons"""
    file_id = callback_data.store_file(filename)
    media_type = get_media_type(filename)
    
    # Shorten URLs
    shortened_direct = await shorten_url_gplinks(direct_url) if direct_url else None
    shortened_player = await shorten_url_gplinks(player_url) if player_url else None
    
    buttons = []
    
    # Main action buttons
    action_buttons = []
    if shortened_direct:
        action_buttons.append(InlineKeyboardButton("📥 Download", url=shortened_direct))
    if shortened_player and media_type in ['video', 'audio']:
        action_buttons.append(InlineKeyboardButton("🎬 Stream", url=shortened_player))
    if media_type == 'image' and shortened_direct:
        action_buttons.append(InlineKeyboardButton("🖼️ View", url=shortened_direct))
    
    if action_buttons:
        buttons.append(action_buttons)
    
    # Utility buttons
    utility_buttons = []
    if direct_url:
        utility_buttons.append(InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{file_id}"))
    utility_buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"ref_{file_id}"))
    buttons.append(utility_buttons)
    
    # Admin buttons
    if is_admin:
        buttons.append([
            InlineKeyboardButton("🗑️ Delete", callback_data=f"del_{file_id}"),
            InlineKeyboardButton("📊 Info", callback_data=f"info_{file_id}")
        ])
    
    # Expiry info
    expiry_date = get_expiry_date()
    buttons.append([
        InlineKeyboardButton(f"⏰ Expires: {expiry_date}", callback_data="expiry_info")
    ])
    
    return InlineKeyboardMarkup(buttons)

async def create_simple_buttons(direct_url, player_url, filename):
    """Create simple buttons for non-admin users"""
    file_id = callback_data.store_file(filename)
    media_type = get_media_type(filename)
    
    shortened_direct = await shorten_url_gplinks(direct_url) if direct_url else None
    shortened_player = await shorten_url_gplinks(player_url) if player_url else None
    
    buttons = []
    
    if shortened_direct:
        buttons.append([InlineKeyboardButton("📥 Download", url=shortened_direct)])
    
    if shortened_player and media_type in ['video', 'audio']:
        buttons.append([InlineKeyboardButton("🎬 Stream Online", url=shortened_player)])
    
    if direct_url:
        buttons.append([InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{file_id}")])
    
    expiry_date = get_expiry_date()
    buttons.append([
        InlineKeyboardButton(f"⏰ Valid until: {expiry_date}", callback_data="expiry_info")
    ])
    
    return InlineKeyboardMarkup(buttons)

# --- Enhanced Upload Functions ---
async def upload_to_wasabi_optimized(file_path, file_name, status_message):
    """Optimized upload with adaptive chunk sizing"""
    try:
        file_size = os.path.getsize(file_path)
        
        # Use multipart for files > 50MB
        if file_size > 50 * 1024 * 1024:
            return await upload_multipart_optimized(file_path, file_name, file_size, status_message)
        else:
            return await upload_single_optimized(file_path, file_name, file_size, status_message)
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise

async def upload_multipart_optimized(file_path, file_name, file_size, status_message):
    """Optimized multipart upload with parallel parts"""
    try:
        # Calculate optimal part size (between 5MB and 5GB)
        optimal_part_size = min(max(CHUNK_SIZE, file_size // 100), 100 * 1024 * 1024)
        part_count = math.ceil(file_size / optimal_part_size)
        
        # Create multipart upload
        mpu = s3_client.create_multipart_upload(
            Bucket=WASABI_BUCKET,
            Key=file_name,
            ContentType='application/octet-stream'
        )
        mpu_id = mpu['UploadId']
        
        logger.info(f"Starting multipart upload: {part_count} parts, {optimal_part_size//1024//1024}MB each")
        
        # Prepare all parts for parallel upload
        async def upload_part_async(part_num):
            start = (part_num - 1) * optimal_part_size
            end = min(start + optimal_part_size, file_size)
            
            loop = asyncio.get_event_loop()
            
            def _upload():
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    data = f.read(end - start)
                    response = s3_client.upload_part(
                        Bucket=WASABI_BUCKET,
                        Key=file_name,
                        PartNumber=part_num,
                        UploadId=mpu_id,
                        Body=data
                    )
                    return {'ETag': response['ETag'], 'PartNumber': part_num}
            
            return await loop.run_in_executor(thread_pool, _upload)
        
        # Upload parts in parallel with concurrency limit
        semaphore = asyncio.Semaphore(MAX_WORKERS // 2)
        
        async def upload_with_limit(part_num):
            async with semaphore:
                return await upload_part_async(part_num)
        
        tasks = [upload_with_limit(i) for i in range(1, part_count + 1)]
        parts = await asyncio.gather(*tasks)
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=WASABI_BUCKET,
            Key=file_name,
            UploadId=mpu_id,
            MultipartUpload={'Parts': parts}
        )
        
        logger.info("Multipart upload completed successfully")
        return True
        
    except Exception as e:
        try:
            s3_client.abort_multipart_upload(Bucket=WASABI_BUCKET, Key=file_name, UploadId=mpu_id)
        except:
            pass
        raise e

async def upload_single_optimized(file_path, file_name, file_size, status_message):
    """Optimized single upload with buffer optimization"""
    loop = asyncio.get_event_loop()
    
    class ProgressTracker:
        def __init__(self):
            self.uploaded = 0
            self.file_size = file_size
        
        def __call__(self, bytes_amount):
            self.uploaded += bytes_amount
            asyncio.run_coroutine_threadsafe(
                progress_callback(self.uploaded, self.file_size, status_message, "⬆️ Uploading..."),
                loop
            )
    
    progress_tracker = ProgressTracker()
    
    # Configure upload with extra args for better performance
    extra_args = {
        'ServerSideEncryption': 'AES256',
        'StorageClass': 'STANDARD'
    }
    
    await loop.run_in_executor(
        thread_pool,
        lambda: s3_client.upload_file(
            file_path,
            WASABI_BUCKET,
            file_name,
            Callback=progress_tracker,
            ExtraArgs=extra_args
        )
    )
    return True

async def download_file_optimized(client, message, file_path, status_message):
    """Optimized file download with resume support"""
    try:
        transfer_stats.start()
        progress_cache[status_message.id] = 0
        
        await client.download_media(
            message=message,
            file_name=file_path,
            progress=progress_callback,
            progress_args=(status_message, "⬇️ Downloading...")
        )
        
        if status_message.id in progress_cache:
            del progress_cache[status_message.id]
            
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

# --- Bot Command Handlers ---
@app.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    user_name = message.from_user.first_name or "User"
    
    start_text = f"""
✨ **Welcome {user_name}!** ✨

**🚀 Ultra-Fast Cloud Storage Bot**
━━━━━━━━━━━━━━━━━━━━━

📦 **Features:**
• ⚡ Lightning-fast uploads/downloads
• 🎬 Built-in media player
• 🔗 28-day valid links
• 🔐 Secure Wasabi storage
• 📱 Mobile optimized

💡 **How to use:**
Simply send me any file and I'll upload it to the cloud instantly!

📊 **Your Stats:**
• User ID: `{message.from_user.id}`
• Status: {'✅ Authorized' if message.from_user.id in ALLOWED_USERS else '⏳ Pending'}
• URL Shortening: {'✅ Active' if AUTO_SHORTEN and GPLINKS_API_KEY else '❌ Inactive'}

━━━━━━━━━━━━━━━━━━━━━
"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Upload Guide", callback_data="guide_upload"),
         InlineKeyboardButton("🎬 Player Guide", callback_data="guide_player")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("ℹ️ About", callback_data="about")]
    ])
    
    await message.reply_text(start_text, reply_markup=keyboard)

@app.on_message(filters.command("help"))
async def help_handler(client: Client, message: Message):
    help_text = """
📚 **Help Center**

**Basic Commands:**
• `/start` - Start the bot
• `/help` - Show this help
• `/speedtest` - Test upload speed
• `/myid` - Get your user ID

**Admin Commands:**
• `/adduser <id>` - Add user
• `/removeuser <id>` - Remove user
• `/listusers` - List all users
• `/stats` - Bot statistics
• `/toggleshorten` - Toggle URL shortening

**File Support:**
• **Video:** MP4, MKV, AVI, MOV, WEBM
• **Audio:** MP3, WAV, OGG, M4A, FLAC
• **Images:** JPG, PNG, GIF, WEBP
• **Documents:** Any file up to 2GB

**Link Validity:** 28 days from upload

━━━━━━━━━━━━━━━━━━━━━
📌 **Need help?** Contact @Sathishkumar33
"""
    await message.reply_text(help_text)

@app.on_message(filters.command("myid"))
async def myid_handler(client: Client, message: Message):
    await message.reply_text(f"🆔 **Your User ID:** `{message.from_user.id}`")

@app.on_message(filters.command("speedtest"))
async def speed_test_handler(client: Client, message: Message):
    test_msg = await message.reply_text("🚀 **Running speed test...**")
    
    test_size = 10 * 1024 * 1024  # 10MB
    test_filename = f"speedtest_{int(time.time())}.bin"
    test_filepath = f"./downloads/{test_filename}"
    
    try:
        # Create test file
        with open(test_filepath, 'wb') as f:
            f.write(os.urandom(test_size))
        
        # Test upload
        start_time = time.time()
        await upload_to_wasabi_optimized(test_filepath, test_filename, test_msg)
        upload_time = time.time() - start_time
        
        upload_speed = test_size / upload_time
        
        # Cleanup
        os.remove(test_filepath)
        s3_client.delete_object(Bucket=WASABI_BUCKET, Key=test_filename)
        
        result_text = f"""
📊 **Speed Test Results**
━━━━━━━━━━━━━━━━━━━━━

📁 **Test File:** 10 MB
⬆️ **Upload Speed:** `{humanbytes(upload_speed)}/s`
⏱️ **Upload Time:** `{upload_time:.2f}` seconds
🔗 **Link Expiry:** `28 days`

**Status:** ✅ Connection is optimal!
━━━━━━━━━━━━━━━━━━━━━
"""
        await test_msg.edit_text(result_text)
        
    except Exception as e:
        await test_msg.edit_text(f"❌ **Speed test failed:** {str(e)}")

@app.on_message(filters.command("toggleshorten"))
async def toggle_shorten_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("⛔ Admin only command!")
        return
    
    global AUTO_SHORTEN
    AUTO_SHORTEN = not AUTO_SHORTEN
    status = "✅ **Enabled**" if AUTO_SHORTEN else "❌ **Disabled**"
    
    await message.reply_text(
        f"🔗 **URL Shortening {status}**\n\n"
        f"Service: GPLinks.in\n"
        f"API Key: {'✅ Configured' if GPLINKS_API_KEY else '❌ Missing'}"
    )

@app.on_message(filters.command("adduser"))
async def add_user_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("⛔ Admin only command!")
        return
    
    try:
        user_id = int(message.text.split()[1])
        ALLOWED_USERS.add(user_id)
        await message.reply_text(f"✅ **User Added!**\n\nUser ID: `{user_id}`\nNow has access to the bot.")
    except (IndexError, ValueError):
        await message.reply_text("⚠️ **Usage:** `/adduser <user_id>`")

@app.on_message(filters.command("removeuser"))
async def remove_user_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("⛔ Admin only command!")
        return
    
    try:
        user_id = int(message.text.split()[1])
        if user_id == ADMIN_ID:
            await message.reply_text("❌ Cannot remove the admin!")
            return
        if user_id in ALLOWED_USERS:
            ALLOWED_USERS.remove(user_id)
            await message.reply_text(f"🗑️ **User Removed!**\n\nUser ID: `{user_id}`\nAccess revoked.")
        else:
            await message.reply_text(f"⚠️ User `{user_id}` not found in authorized list.")
    except (IndexError, ValueError):
        await message.reply_text("⚠️ **Usage:** `/removeuser <user_id>`")

@app.on_message(filters.command("listusers"))
async def list_users_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("⛔ Admin only command!")
        return
    
    user_list = "\n".join([f"• `{uid}`" for uid in ALLOWED_USERS])
    await message.reply_text(
        f"👥 **Authorized Users**\n━━━━━━━━━━━━━━━━━━━━━\n{user_list}\n━━━━━━━━━━━━━━━━━━━━━\n**Total:** {len(ALLOWED_USERS)} users"
    )

@app.on_message(filters.command("stats"))
async def stats_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("⛔ Admin only command!")
        return
    
    stats_text = f"""
📊 **Bot Statistics**
━━━━━━━━━━━━━━━━━━━━━

**📈 Usage:**
• Authorized Users: `{len(ALLOWED_USERS)}`
• URL Shortening: `{'✅ On' if AUTO_SHORTEN else '❌ Off'}`

**⚙️ Performance:**
• Thread Workers: `{MAX_WORKERS}`
• Chunk Size: `{humanbytes(CHUNK_SIZE)}`
• Link Expiry: `28 days`

**🔗 Storage:**
• Bucket: `{WASABI_BUCKET}`
• Region: `{WASABI_REGION}`
• Status: `{'✅ Connected' if s3_client else '❌ Disconnected'}`

**🎮 Web Player:**
• URL: `{RENDER_URL}`
━━━━━━━━━━━━━━━━━━━━━
"""
    await message.reply_text(stats_text)

# --- File Upload Handler ---
@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def file_handler(client: Client, message: Message):
    # Check authorization
    if message.from_user.id not in ALLOWED_USERS:
        await message.reply_text("⛔ **Access Denied!**\n\nYou are not authorized to use this bot. Please contact the administrator.")
        return
    
    if not s3_client:
        await message.reply_text("❌ **Storage Error!**\n\nWasabi connection is not available. Please try again later.")
        return
    
    # Get file info
    if message.video:
        media = message.video
        file_name = media.file_name or f"video_{int(time.time())}.mp4"
    elif message.audio:
        media = message.audio
        file_name = media.file_name or f"audio_{int(time.time())}.mp3"
    elif message.document:
        media = message.document
        file_name = media.file_name
    elif message.photo:
        media = message.photo
        file_name = f"image_{int(time.time())}.jpg"
    else:
        await message.reply_text("❌ Unsupported file type!")
        return
    
    file_size = media.file_size
    
    if file_size > 2 * 1024 * 1024 * 1024:  # 2GB limit
        await message.reply_text("❌ **File too large!**\n\nMaximum file size is **2GB**.")
        return
    
    # Start upload process
    status_msg = await message.reply_text(
        f"🚀 **Starting upload...**\n\n"
        f"📁 **File:** `{file_name}`\n"
        f"📦 **Size:** `{humanbytes(file_size)}`\n"
        f"⏰ **Expires:** `28 days`"
    )
    
    # Create unique filename
    timestamp = int(time.time())
    safe_filename = f"{timestamp}_{file_name.replace('/', '_')}"
    file_path = f"./downloads/{safe_filename}"
    os.makedirs("./downloads", exist_ok=True)
    
    try:
        # Download file
        await status_msg.edit_text("⬇️ **Downloading from Telegram...**")
        await download_file_optimized(client, message, file_path, status_msg)
        
        # Upload to Wasabi
        await status_msg.edit_text("⬆️ **Uploading to Cloud Storage...**")
        await upload_to_wasabi_optimized(file_path, safe_filename, status_msg)
        
        # Generate URLs
        await status_msg.edit_text("🔗 **Generating secure links...**")
        presigned_url = await generate_presigned_url(safe_filename)
        player_url = generate_player_url(safe_filename, presigned_url)
        
        # Create buttons
        is_admin_user = message.from_user.id == ADMIN_ID
        if is_admin_user:
            keyboard = await create_modern_buttons(presigned_url, player_url, safe_filename, True)
        else:
            keyboard = await create_simple_buttons(presigned_url, player_url, safe_filename)
        
        # Get media type icon
        media_icons = {
            'video': '🎬',
            'audio': '🎵',
            'image': '🖼️',
            'file': '📄'
        }
        media_icon = media_icons.get(get_media_type(file_name), '📁')
        
        # Final response
        final_text = f"""
✅ **Upload Complete!** {media_icon}

━━━━━━━━━━━━━━━━━━━━━

📁 **File:** `{file_name}`
📦 **Size:** `{humanbytes(file_size)}`
🔗 **Expires:** `28 days` from now
⚡ **Storage:** Wasabi Cloud

━━━━━━━━━━━━━━━━━━━━━
🎬 **Media Player Available** • 🔗 **Links are valid for 28 days**
"""
        await status_msg.edit_text(final_text, reply_markup=keyboard, disable_web_page_preview=True)
        
    except Exception as e:
        logger.error(f"File handling error: {e}")
        await status_msg.edit_text(f"❌ **Upload Failed!**\n\nError: `{str(e)}`")
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)

# --- Callback Query Handler ---
@app.on_callback_query()
async def handle_callback_query(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    message = callback_query.message
    
    # Handle simple info callbacks
    if data == "guide_upload":
        await callback_query.answer()
        await message.reply_text(
            "📤 **Upload Guide**\n\n"
            "1️⃣ Send any file to the bot\n"
            "2️⃣ Wait for upload to complete\n"
            "3️⃣ Click Download or Stream buttons\n"
            "4️⃣ Links are valid for 28 days\n\n"
            "Supported: Video, Audio, Images, Documents"
        )
        await callback_query.answer()
        return
    
    elif data == "guide_player":
        await callback_query.answer()
        await message.reply_text(
            "🎬 **Player Guide**\n\n"
            "**Controls:**\n"
            "• Space - Play/Pause\n"
            "• ◀/▶ - Seek 10 seconds\n"
            "• ▲/▼ - Volume control\n"
            "• F - Fullscreen\n"
            "• M - Mute\n\n"
            "**Features:**\n"
            "• HTML5 player\n"
            "• Mobile responsive\n"
            "• Download option"
        )
        await callback_query.answer()
        return
    
    elif data == "settings":
        await callback_query.answer()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 URL Shortening", callback_data="toggle_shorten")],
            [InlineKeyboardButton("ℹ️ About Bot", callback_data="about")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_home")]
        ])
        await message.edit_text("⚙️ **Settings**\n\nConfigure your bot preferences:", reply_markup=keyboard)
        return
    
    elif data == "about":
        await callback_query.answer()
        about_text = """
🤖 **About This Bot**

**Version:** 2.0 Ultra-Fast
**Developer:** @Sathishkumar33
**Storage:** Wasabi Cloud
**Link Validity:** 28 days

**Features:**
• ⚡ Parallel uploading
• 🎬 Built-in media player
• 🔗 Auto URL shortening
• 📱 Mobile optimized

━━━━━━━━━━━━━━━━━━━━━
💡 Made with ❤️ for fast cloud storage
"""
        await message.reply_text(about_text)
        await callback_query.answer()
        return
    
    elif data == "back_home":
        await callback_query.answer()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload Guide", callback_data="guide_upload"),
             InlineKeyboardButton("🎬 Player Guide", callback_data="guide_player")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
             InlineKeyboardButton("ℹ️ About", callback_data="about")]
        ])
        await message.edit_text(
            "✨ **Welcome Back!** ✨\n\nSend me any file to upload to cloud storage!",
            reply_markup=keyboard
        )
        return
    
    elif data == "expiry_info":
        await callback_query.answer(f"Links expire after {config.LINK_EXPIRY_DAYS} days from upload", show_alert=True)
        return
    
    # Handle file-specific callbacks
    if '_' in data:
        action, file_id = data.split('_', 1)
        filename = callback_data.get_file(file_id)
        
        if not filename:
            await callback_query.answer("❌ Link expired! Please upload the file again.", show_alert=True)
            return
        
        if action == "copy":
            presigned_url = await generate_presigned_url(filename)
            if presigned_url:
                shortened = await shorten_url_gplinks(presigned_url)
                await callback_query.answer("✅ Link copied!", show_alert=False)
                await message.reply_text(f"**Direct Download Link:**\n`{shortened}`")
            else:
                await callback_query.answer("❌ Failed to generate link", show_alert=True)
        
        elif action == "del" and user_id == ADMIN_ID:
            try:
                s3_client.delete_object(Bucket=WASABI_BUCKET, Key=filename)
                callback_data.clear_file(file_id)
                await callback_query.answer("✅ File deleted!", show_alert=True)
                await message.edit_text(f"🗑️ **File Deleted**\n\n`{filename}` has been removed from storage.")
            except Exception as e:
                await callback_query.answer("❌ Delete failed", show_alert=True)
        
        elif action == "ref":
            presigned_url = await generate_presigned_url(filename)
            player_url = generate_player_url(filename, presigned_url)
            
            is_admin_user = user_id == ADMIN_ID
            if is_admin_user:
                keyboard = await create_modern_buttons(presigned_url, player_url, filename, True)
            else:
                keyboard = await create_simple_buttons(presigned_url, player_url, filename)
            
            await message.edit_reply_markup(reply_markup=keyboard)
            await callback_query.answer("✅ Links refreshed!")
        
        elif action == "info" and user_id == ADMIN_ID:
            presigned_url = await generate_presigned_url(filename)
            await message.reply_text(
                f"📊 **File Information**\n\n"
                f"**Name:** `{filename}`\n"
                f"**Type:** {get_media_type(filename)}\n"
                f"**Expires:** {get_expiry_date()}\n"
                f"**URL:** `{presigned_url[:100]}...`"
            )
            await callback_query.answer()
    
    else:
        await callback_query.answer("Invalid option", show_alert=True)

# --- Flask Web Server ---
web_app = Flask(__name__)

@web_app.route('/')
def index():
    return render_template('index.html', render_url=RENDER_URL)

@web_app.route('/player/<media_type>/<encoded_url>')
def player(media_type, encoded_url):
    try:
        padding = 4 - (len(encoded_url) % 4)
        encoded_url += '=' * padding
        media_url = base64.urlsafe_b64decode(encoded_url).decode()
        
        return render_template('player.html', 
                             media_type=media_type,
                             media_url=media_url,
                             render_url=RENDER_URL)
    except Exception as e:
        return f"Error: {str(e)}", 400

@web_app.route('/about')
def about():
    return render_template('about.html')

@web_app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "service": "wasabi_bot",
        "version": "2.0",
        "expiry_days": config.LINK_EXPIRY_DAYS
    })

def run_flask():
    web_app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)

# --- Main Function ---
def main():
    # Create directories
    os.makedirs("./downloads", exist_ok=True)
    
    # Start Flask server
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("🌐 Flask server started on port 8000")
    
    # Print startup info
    print("\n" + "="*50)
    print("🚀 ULTRA-FAST WASABI BOT v2.0")
    print("="*50)
    print(f"📦 Link Expiry: {config.LINK_EXPIRY_DAYS} days")
    print(f"⚡ Thread Workers: {MAX_WORKERS}")
    print(f"📁 Chunk Size: {humanbytes(CHUNK_SIZE)}")
    print(f"🔗 URL Shortening: {'ON' if AUTO_SHORTEN else 'OFF'}")
    print("="*50 + "\n")
    
    # Start bot
    logger.info("🤖 Starting bot...")
    app.run()

if __name__ == "__main__":
    main()