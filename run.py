#!/usr/bin/env python3
import threading
import asyncio
import logging
from web_server import run_web_server
from bot import main as bot_main
from config import config

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)

def run_bot():
    """Run the Telegram bot"""
    try:
        asyncio.run(bot_main())
    except Exception as e:
        logger.error(f"Bot error: {e}")

if __name__ == "__main__":
    logger.info("🚀 Starting Wasabi Storage Bot with Web Player...")
    
    # Start web server in a separate thread
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()
    
    logger.info(f"🌐 Web server started on port {config.WEB_PORT}")
    logger.info("🤖 Starting Telegram bot...")
    
    # Run bot in main thread
    run_bot()
