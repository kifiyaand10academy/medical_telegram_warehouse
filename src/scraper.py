# src/scraper.py
import os
import json
import logging
from datetime import datetime, timezone
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Telegram API credentials
api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')
client = TelegramClient('session_name', api_id, api_hash)

# Channels to scrape (use usernames without @)
CHANNELS = [
    'lobelia4cosmetics',
    'tikvahpharma',
    'CheMed123'
    # Add more from et.tgstat.com/medicine as needed
]

async def scrape_channel(channel_name):
    try:
        logging.info(f"Starting scrape for channel: {channel_name}")
        channel = await client.get_entity(channel_name)
        messages = []

        # Fetch messages (limit=100 for testing; increase later)
        async for message in client.iter_messages(channel, limit=100):
            msg_data = {
                'message_id': message.id,
                'channel_name': channel_name,
                'message_date': message.date.isoformat() if message.date else None,
                'message_text': message.message or '',
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0),
                'has_media': bool(message.media and hasattr(message.media, 'photo')),
                'image_path': None
            }

            # Download image if exists
            if msg_data['has_media']:
                img_dir = f"data/raw/images/{channel_name}"
                os.makedirs(img_dir, exist_ok=True)
                img_path = f"{img_dir}/{message.id}.jpg"
                await client.download_media(message, img_path)
                msg_data['image_path'] = img_path

            messages.append(msg_data)

        # Save to partitioned JSON
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        out_dir = f"data/raw/telegram_messages/{date_str}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/{channel_name}.json"

        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)

        logging.info(f"Saved {len(messages)} messages for {channel_name} to {out_file}")

    except Exception as e:
        logging.error(f"Error scraping {channel_name}: {e}")

async def main():
    await client.start()
    for channel in CHANNELS:
        await scrape_channel(channel)
    await client.disconnect()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())