# src/load_raw_to_postgres.py
import os
import json
import glob
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from collections import defaultdict

# Load .env from project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../.env'))

def connect_db():
    return psycopg2.connect(
        host=os.getenv('postgres_host'),
        port=os.getenv('postgres_port'),
        dbname=os.getenv('postgres_db'),
        user=os.getenv('postgres_user'),
        password=os.getenv('postgres_password'),
        sslmode=os.getenv("DB_SSLMODE")  # important for Neon
    )

def load_raw_data():
    conn = connect_db()
    cur = conn.cursor()

    # Create raw schema and table with composite PK
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            message_id BIGINT,
            channel_name TEXT,
            message_date TIMESTAMP,
            message_text TEXT,
            views INTEGER,
            forwards INTEGER,
            has_media BOOLEAN,
            image_path TEXT,
            PRIMARY KEY (message_id, channel_name)
        );
    """)
    conn.commit()

    # Find all JSON files under all date folders
    json_files = glob.glob("data/raw/telegram_messages/*/*.json")
    print(f"Found {len(json_files)} JSON files")

    records = []
    channel_counts = defaultdict(int)

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Skipped invalid JSON file: {file}")
                continue

            for msg in data:
                # Handle missing keys safely
                message_id = msg.get('message_id')
                channel_name = msg.get('channel_name')
                message_date = msg.get('message_date')
                message_text = msg.get('message_text', '')
                views = msg.get('views') or 0
                forwards = msg.get('forwards') or 0
                has_media = msg.get('has_media', False)
                image_path = msg.get('image_path', None)

                if message_id and channel_name:
                    records.append((
                        message_id,
                        channel_name,
                        datetime.fromisoformat(message_date.replace('Z', '+00:00')) if message_date else None,
                        message_text,
                        views,
                        forwards,
                        has_media,
                        image_path
                    ))
                    channel_counts[channel_name] += 1

    if records:
        execute_values(
            cur,
            """
            INSERT INTO raw.telegram_messages
            (message_id, channel_name, message_date, message_text, views, forwards, has_media, image_path)
            VALUES %s
            ON CONFLICT (message_id, channel_name) DO NOTHING;
            """,
            records
        )
        conn.commit()
        print(f"Attempted to load {len(records)} total records")

        # Print per-channel summary
        print("Records per channel:")
        for channel, count in channel_counts.items():
            print(f"  {channel}: {count}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    load_raw_data()
