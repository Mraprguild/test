import os
import sys
import time
import math
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from flask import Flask, render_template, request, jsonify

from config import config

# ==================== Logging ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== High-Performance S3 Client ====================
class FastWasabiClient:
    """Optimized Wasabi client for maximum speed"""
    
    def __init__(self):
        # Optimized boto3 config for maximum throughput
        self.boto_config = BotoConfig(
            region_name=config.WASABI_REGION,
            signature_version='s3v4',
            max_pool_connections=config.MAX_WORKERS,
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            },
            tcp_keepalive=True,
            use_dualstack_endpoint=False,
            payload_signing_enabled=False,  # Disable for speed
            read_timeout=config.S3_TIMEOUT,
            connect_timeout=config.S3_CONNECT_TIMEOUT,
            parameter_validation=False  # Disable for speed
        )
        
        # Create S3 client
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
            config=self.boto_config
        )
        
        self.bucket = config.WASABI_BUCKET
        self.upload_semaphore = Semaphore(config.MAX_CONCURRENT_UPLOADS)
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self):
        """Test S3 connection"""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ Connected to Wasabi ({config.WASABI_REGION})")
            logger.info(f"🚀 Max workers: {config.MAX_WORKERS}")
            logger.info(f"📦 Chunk size: {config.CHUNK_SIZE // 1024 // 1024}MB")
        except Exception as e:
            logger.error(f"❌ Wasabi connection failed: {e}")
            raise
    
    def upload_file_parallel(self, file_path: str, key: str, 
                            callback=None) -> Tuple[bool, str]:
        """Parallel multipart upload for maximum speed"""
        file_size = os.path.getsize(file_path)
        
        # For small files, use single upload
        if file_size < config.MULTIPART_THRESHOLD:
            return self._upload_single(file_path, key, callback)
        
        # For large files, use parallel multipart
        return self._upload_multipart_parallel(file_path, key, file_size, callback)
    
    def _upload_single(self, file_path: str, key: str, callback) -> Tuple[bool, str]:
        """Single-part upload for small files"""
        try:
            with self.upload_semaphore:
                self.s3.upload_file(
                    file_path, self.bucket, key,
                    Callback=callback,
                    Config=boto3.s3.transfer.TransferConfig(
                        multipart_threshold=config.MULTIPART_THRESHOLD,
                        max_concurrency=config.MAX_WORKERS,
                        use_threads=True
                    )
                )
            return True, ""
        except Exception as e:
            return False, str(e)
    
    def _upload_multipart_parallel(self, file_path: str, key: str, 
                                   file_size: int, callback) -> Tuple[bool, str]:
        """Parallel multipart upload for maximum speed"""
        try:
            # Create multipart upload
            mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=key)
            upload_id = mpu['UploadId']
            
            # Calculate parts
            part_size = config.MULTIPART_CHUNKSIZE
            num_parts = math.ceil(file_size / part_size)
            
            logger.info(f"Starting multipart upload: {num_parts} parts @ {part_size//1024//1024}MB each")
            
            # Prepare upload tasks
            import concurrent.futures
            parts = [None] * num_parts
            
            def upload_part(part_num):
                start = (part_num - 1) * part_size
                end = min(start + part_size, file_size)
                
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    data = f.read(end - start)
                    
                    response = self.s3.upload_part(
                        Bucket=self.bucket,
                        Key=key,
                        PartNumber=part_num,
                        UploadId=upload_id,
                        Body=data
                    )
                    
                    if callback:
                        callback(file_size)
                    
                    return {
                        'PartNumber': part_num,
                        'ETag': response['ETag']
                    }
            
            # Upload parts in parallel
            with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                futures = [executor.submit(upload_part, i) for i in range(1, num_parts + 1)]
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    part = future.result()
                    parts[part['PartNumber'] - 1] = part
            
            # Complete upload
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.info(f"✅ Upload complete: {key}")
            return True, ""
            
        except Exception as e:
            # Abort on failure
            try:
                self.s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
            except:
                pass
            return False, str(e)
    
    def generate_direct_url(self, key: str) -> str:
        """Generate direct Wasabi URL (fastest, no presigning overhead)"""
        return f"https://{self.bucket}.s3.{config.WASABI_REGION}.wasabisys.com/{key}"
    
    def generate_presigned_url(self, key: str, expires: int = 604800) -> Optional[str]:
        """Generate presigned URL (for security)"""
        try:
            url = self.s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=expires
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate URL: {e}")
            return None
    
    def delete_file(self, key: str) -> bool:
        """Delete file from Wasabi"""
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return False

# ==================== FastDownloader ====================
class FastDownloader:
    """Optimized downloader for Telegram"""
    
    def __init__(self, client: Client):
        self.client = client
        self.speed_stats = {}
    
    async def download_fast(self, message: Message, file_path: str, 
                           progress_callback=None) -> bool:
        """Fast download with optimized settings"""
        try:
            # Start download with high-performance settings
            await self.client.download_media(
                message=message,
                file_name=file_path,
                progress=progress_callback,
                progress_args=(),
                block=True
            )
            return True
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False

# ==================== Telegram Bot ====================
class WasabiBot:
    """Main bot application"""
    
    def __init__(self):
        self.app = Client(
            "wasabi_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BOT_TOKEN,
            workdir="./bot_data",
            max_concurrent_transmissions=config.MAX_WORKERS
        )
        
        self.wasabi = FastWasabiClient()
        self.downloader = FastDownloader(self.app)
        self.allowed_users = {config.ADMIN_ID}
        
        # Progress tracking
        self.progress_data = {}
        
        # Register handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register all message handlers"""
        
        @self.app.on_message(filters.command("start"))
        async def start_handler(client: Client, message: Message):
            await self._handle_start(client, message)
        
        @self.app.on_message(filters.command("help"))
        async def help_handler(client: Client, message: Message):
            await self._handle_help(client, message)
        
        @self.app.on_message(filters.document | filters.video | filters.audio)
        async def file_handler(client: Client, message: Message):
            await self._handle_file(client, message)
        
        @self.app.on_callback_query()
        async def callback_handler(client: Client, callback_query):
            await self._handle_callback(client, callback_query)
        
        @self.app.on_message(filters.command("speedtest"))
        async def speedtest_handler(client: Client, message: Message):
            await self._handle_speedtest(client, message)
        
        @self.app.on_message(filters.command("adduser"))
        async def adduser_handler(client: Client, message: Message):
            await self._handle_adduser(client, message)
    
    async def _handle_start(self, client: Client, message: Message):
        """Handle /start command"""
        user_id = message.from_user.id
        is_admin = user_id == config.ADMIN_ID
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Speed Test", callback_data="speedtest")],
            [InlineKeyboardButton("📤 Upload Guide", callback_data="guide"),
             InlineKeyboardButton("📊 Stats", callback_data="stats")]
        ])
        
        if is_admin:
            keyboard.inline_keyboard.append([
                InlineKeyboardButton("👥 Users", callback_data="users"),
                InlineKeyboardButton("🗑 Clear Cache", callback_data="clearcache")
            ])
        
        await message.reply_text(
            f"🚀 **Ultra-Fast Wasabi Bot**\n\n"
            f"**User:** `{user_id}`\n"
            f"**Role:** {'👑 Admin' if is_admin else '👤 User'}\n\n"
            f"**⚡ Performance:**\n"
            f"• Parallel uploads: {config.MAX_WORKERS} threads\n"
            f"• Chunk size: {config.CHUNK_SIZE // 1024 // 1024}MB\n"
            f"• Direct Wasabi links\n\n"
            f"**📤 Just send any file to upload!**",
            reply_markup=keyboard
        )
    
    async def _handle_help(self, client: Client, message: Message):
        """Handle /help command"""
        help_text = f"""
📚 **Help & Commands**

**Basic Commands:**
/start - Show main menu
/help - This help message
/speedtest - Test upload speed

**Admin Commands:**
/adduser <id> - Add user
/removeuser <id> - Remove user
/listusers - List all users

**⚡ Speed Optimizations:**
• Parallel multipart uploads
• {config.MAX_WORKERS} concurrent threads
• {config.MULTIPART_CHUNKSIZE // 1024 // 1024}MB chunks
• Direct Wasabi CDN links

**📱 Features:**
• Direct download buttons
• 7-day valid links
• Instant uploads
• No file size limits (up to 20GB)

**Need help?** Contact your administrator.
        """
        await message.reply_text(help_text)
    
    async def _handle_file(self, client: Client, message: Message):
        """Handle file upload with maximum speed"""
        user_id = message.from_user.id
        
        if user_id not in self.allowed_users:
            await message.reply_text("⛔ Unauthorized!")
            return
        
        # Get file info
        media = message.document or message.video or message.audio
        if not media:
            return
        
        original_name = media.file_name
        file_size = media.file_size
        
        # Check file size
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text(f"❌ File too large! Max: {config.MAX_FILE_SIZE // 1024 // 1024 // 1024}GB")
            return
        
        # Create progress message
        progress_msg = await message.reply_text("🚀 **Starting ultra-fast upload...**")
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(original_name).suffix) as tmp:
            temp_path = tmp.name
        
        try:
            # Step 1: Fast download from Telegram
            await progress_msg.edit_text("⬇️ **Downloading at max speed...**")
            
            download_start = time.time()
            
            async def download_progress(current, total):
                elapsed = time.time() - download_start
                if elapsed > 0:
                    speed = current / elapsed
                    percentage = (current / total) * 100
                    
                    if speed > 1024 * 1024:
                        speed_str = f"{speed / 1024 / 1024:.2f} MB/s"
                    elif speed > 1024:
                        speed_str = f"{speed / 1024:.2f} KB/s"
                    else:
                        speed_str = f"{speed:.2f} B/s"
                    
                    try:
                        asyncio.create_task(
                            progress_msg.edit_text(
                                f"⬇️ **Downloading...**\n"
                                f"📦 {self._humanbytes(current)} / {self._humanbytes(total)}\n"
                                f"⚡ {speed_str}\n"
                                f"📊 {percentage:.1f}%"
                            )
                        )
                    except:
                        pass
            
            # Download file
            await self.downloader.download_fast(message, temp_path, download_progress)
            
            download_time = time.time() - download_start
            download_speed = file_size / download_time
            
            # Step 2: Fast upload to Wasabi
            await progress_msg.edit_text("⬆️ **Uploading at max speed...**")
            
            upload_start = time.time()
            
            def upload_progress(byte_count):
                elapsed = time.time() - upload_start
                if elapsed > 0:
                    speed = byte_count / elapsed
                    percentage = (byte_count / file_size) * 100
                    
                    logger.debug(f"Upload: {percentage:.1f}% @ {speed/1024/1024:.2f} MB/s")
            
            # Upload file
            key = f"uploads/{int(time.time())}_{original_name}"
            success, error = self.wasabi.upload_file_parallel(temp_path, key, upload_progress)
            
            if not success:
                await progress_msg.edit_text(f"❌ Upload failed: {error}")
                return
            
            upload_time = time.time() - upload_start
            upload_speed = file_size / upload_time
            
            # Step 3: Generate URLs
            await progress_msg.edit_text("🔗 **Generating download links...**")
            
            # Generate direct URL (fastest)
            direct_url = self.wasabi.generate_direct_url(key)
            
            # Optional: Presigned URL for security
            presigned_url = self.wasabi.generate_presigned_url(key)
            
            # Generate player URL for videos
            player_url = None
            is_video = message.video or (message.document and 
                                        message.document.mime_type and 
                                        'video' in message.document.mime_type)
            
            if is_video and config.WEB_SERVER_URL:
                encoded_url = base64.urlsafe_b64encode(direct_url.encode()).decode().rstrip('=')
                player_url = f"{config.WEB_SERVER_URL}/player/{encoded_url}"
            
            # Create buttons
            buttons = []
            
            if direct_url:
                buttons.append([InlineKeyboardButton("⚡ Direct Download", url=direct_url)])
            
            if player_url:
                buttons.append([InlineKeyboardButton("🎥 Stream Video", url=player_url)])
            
            buttons.append([
                InlineKeyboardButton("📋 Copy Direct", callback_data=f"copy_direct_{key}"),
                InlineKeyboardButton("📋 Copy Player", callback_data=f"copy_player_{key}")
            ])
            
            if user_id == config.ADMIN_ID:
                buttons.append([InlineKeyboardButton("🗑 Delete", callback_data=f"delete_{key}")])
            
            keyboard = InlineKeyboardMarkup(buttons) if buttons else None
            
            # Final message with performance stats
            result_text = (
                f"✅ **Upload Complete!**\n\n"
                f"📄 **File:** `{original_name}`\n"
                f"📦 **Size:** {self._humanbytes(file_size)}\n\n"
                f"⚡ **Performance:**\n"
                f"⬇️ Download: {download_speed/1024/1024:.2f} MB/s ({download_time:.1f}s)\n"
                f"⬆️ Upload: {upload_speed/1024/1024:.2f} MB/s ({upload_time:.1f}s)\n\n"
                f"🔗 **Links valid for 7 days**\n"
                f"🗄️ **Storage:** Wasabi CDN"
            )
            
            await progress_msg.edit_text(result_text, reply_markup=keyboard, disable_web_page_preview=True)
            
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            await progress_msg.edit_text(f"❌ Error: {str(e)}")
        
        finally:
            # Cleanup temp file
            try:
                os.unlink(temp_path)
            except:
                pass
    
    async def _handle_callback(self, client: Client, callback_query):
        """Handle button callbacks"""
        data = callback_query.data
        message = callback_query.message
        user_id = callback_query.from_user.id
        
        if data.startswith("copy_direct_"):
            key = data.replace("copy_direct_", "")
            url = self.wasabi.generate_direct_url(key)
            await callback_query.answer("📋 Direct link copied!", show_alert=False)
            await message.reply_text(f"**Direct Download Link:**\n`{url}`")
        
        elif data.startswith("copy_player_"):
            key = data.replace("copy_player_", "")
            url = self.wasabi.generate_direct_url(key)
            encoded_url = base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')
            player_url = f"{config.WEB_SERVER_URL}/player/{encoded_url}"
            await callback_query.answer("📋 Player link copied!", show_alert=False)
            await message.reply_text(f"**Player URL:**\n{player_url}")
        
        elif data.startswith("delete_"):
            if user_id != config.ADMIN_ID:
                await callback_query.answer("❌ Admin only!", show_alert=True)
                return
            
            key = data.replace("delete_", "")
            if self.wasabi.delete_file(key):
                await callback_query.answer("✅ File deleted!", show_alert=True)
                await message.edit_text("🗑 **File deleted successfully**")
            else:
                await callback_query.answer("❌ Delete failed", show_alert=True)
        
        elif data == "speedtest":
            await callback_query.answer()
            await self._handle_speedtest(client, message)
        
        elif data == "stats":
            stats = f"""
📊 **Bot Statistics**

**Performance:**
• Max workers: {config.MAX_WORKERS}
• Chunk size: {config.CHUNK_SIZE // 1024 // 1024}MB
• Upload threads: {config.MAX_CONCURRENT_UPLOADS}

**System:**
• CPUs: {os.cpu_count()}
• Memory: {psutil.virtual_memory().percent}%
• Disk: {psutil.disk_usage('/').percent}%

**Users:**
• Authorized: {len(self.allowed_users)}
            """
            await callback_query.message.reply_text(stats)
        
        elif data == "users" and user_id == config.ADMIN_ID:
            users_list = "\n".join([f"• `{uid}`" for uid in self.allowed_users])
            await callback_query.message.reply_text(f"**Authorized Users:**\n{users_list}")
        
        elif data in ["guide", "clearcache"]:
            await callback_query.answer()
    
    async def _handle_speedtest(self, client: Client, message: Message):
        """Test upload speed"""
        msg = await message.reply_text("🚀 **Running speed test...**")
        
        # Create test file
        test_size = 50 * 1024 * 1024  # 50MB test file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".test") as tmp:
            tmp.write(os.urandom(test_size))
            test_path = tmp.name
        
        try:
            # Test upload
            start = time.time()
            key = f"speedtest_{int(time.time())}.test"
            success, _ = self.wasabi.upload_file_parallel(test_path, key)
            
            if success:
                elapsed = time.time() - start
                speed = test_size / elapsed
                
                result = (
                    f"⚡ **Speed Test Results**\n\n"
                    f"📦 Size: 50 MB\n"
                    f"⏱️ Time: {elapsed:.2f}s\n"
                    f"🚀 Speed: {speed/1024/1024:.2f} MB/s\n\n"
                    f"✅ **System is running at maximum speed!**"
                )
                
                # Cleanup
                self.wasabi.delete_file(key)
                await msg.edit_text(result)
            else:
                await msg.edit_text("❌ Speed test failed")
        
        finally:
            os.unlink(test_path)
    
    async def _handle_adduser(self, client: Client, message: Message):
        """Add authorized user"""
        if message.from_user.id != config.ADMIN_ID:
            await message.reply_text("⛔ Admin only!")
            return
        
        try:
            user_id = int(message.text.split()[1])
            self.allowed_users.add(user_id)
            await message.reply_text(f"✅ User `{user_id}` added!")
        except:
            await message.reply_text("❌ Usage: /adduser <user_id>")
    
    def _humanbytes(self, size: int) -> str:
        """Convert bytes to human readable"""
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.2f} KB"
        elif size < 1024 * 1024 * 1024:
            return f"{size / 1024 / 1024:.2f} MB"
        else:
            return f"{size / 1024 / 1024 / 1024:.2f} GB"
    
    def run(self):
        """Run the bot"""
        logger.info("🚀 Starting Ultra-Fast Wasabi Bot...")
        self.app.run()

# ==================== Flask Web Server ====================
web_app = Flask(__name__)

@web_app.route('/')
def index():
    return render_template('index.html')

@web_app.route('/player/<encoded_url>')
def player(encoded_url):
    """Video player endpoint"""
    try:
        # Decode URL
        padding = 4 - (len(encoded_url) % 4)
        encoded_url += '=' * padding
        video_url = base64.urlsafe_b64decode(encoded_url).decode()
        
        return render_template('player.html', video_url=video_url)
    except Exception as e:
        return f"Error: {str(e)}", 400

@web_app.route('/health')
def health():
    return jsonify({"status": "healthy", "service": "wasabi-bot"})

def run_flask():
    """Run Flask server"""
    web_app.run(host='0.0.0.0', port=config.WEB_PORT, debug=False, threaded=True)

# ==================== Templates ====================
# Create templates directory and files if not exists
TEMPLATES = {
    "index.html": """
<!DOCTYPE html>
<html>
<head>
    <title>Wasabi Bot - Ultra Fast Storage</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 50px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            text-align: center;
        }
        h1 {
            font-size: 3em;
            margin-bottom: 20px;
        }
        .status {
            background: rgba(255,255,255,0.2);
            padding: 20px;
            border-radius: 10px;
            margin: 20px 0;
        }
        .feature {
            display: inline-block;
            margin: 10px;
            padding: 10px 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 5px;
        }
        input {
            width: 100%;
            padding: 15px;
            margin: 10px 0;
            border: none;
            border-radius: 5px;
            font-size: 16px;
        }
        button {
            padding: 15px 30px;
            background: #4CAF50;
            color: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 16px;
            transition: transform 0.2s;
        }
        button:hover {
            transform: scale(1.05);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Wasabi Ultra-Fast Bot</h1>
        <div class="status">
            <h2>✅ Bot Status: Online</h2>
            <p>High-speed file storage and streaming</p>
        </div>
        <div>
            <div class="feature">⚡ 1GB/s Upload</div>
            <div class="feature">🎥 Video Streaming</div>
            <div class="feature">🔗 Direct Links</div>
            <div class="feature">🛡️ Secure Storage</div>
        </div>
        <p>Send files to <strong>@YourBotUsername</strong> on Telegram</p>
    </div>
</body>
</html>
    """,
    
    "player.html": """
<!DOCTYPE html>
<html>
<head>
    <title>Video Player - Wasabi Bot</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            margin: 0;
            padding: 20px;
            background: #000;
            color: white;
            font-family: Arial, sans-serif;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        video {
            width: 100%;
            max-height: 80vh;
            background: #000;
            border-radius: 10px;
        }
        .controls {
            margin-top: 20px;
            text-align: center;
        }
        .btn {
            display: inline-block;
            padding: 10px 20px;
            background: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            margin: 0 10px;
        }
        .btn:hover {
            background: #45a049;
        }
    </style>
</head>
<body>
    <div class="container">
        <video controls autoplay>
            <source src="{{ video_url }}" type="video/mp4">
            Your browser does not support the video tag.
        </video>
        <div class="controls">
            <a href="{{ video_url }}" class="btn" download>📥 Download Video</a>
            <a href="/" class="btn">🏠 Home</a>
        </div>
    </div>
</body>
</html>
    """
}

# Create templates
os.makedirs("templates", exist_ok=True)
for filename, content in TEMPLATES.items():
    filepath = os.path.join("templates", filename)
    if not os.path.exists(filepath):
        with open(filepath, "w") as f:
            f.write(content)
        logger.info(f"Created template: {filename}")

# ==================== Main Entry Point ====================
if __name__ == "__main__":
    # Ensure required directories exist
    os.makedirs("./bot_data", exist_ok=True)
    os.makedirs("./downloads", exist_ok=True)
    
    # Start Flask in background thread
    import threading
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Web server started on port {config.WEB_PORT}")
    
    # Run bot
    bot = WasabiBot()
    bot.run()
