# src/load_yolo_to_postgres.py
import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    return psycopg2.connect(
        host=os.getenv("postgres_host"),
        port=os.getenv("postgres_port"),
        dbname=os.getenv("postgres_db"),   
        user=os.getenv("postgres_user"),
        password=os.getenv("postgres_password"),
        sslmode=os.getenv("DB_SSLMODE", "require")
    )

def load_yolo_results():
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.yolo_detections (
            message_id TEXT,
            channel_name TEXT,
            detected_objects TEXT,
            confidence_score FLOAT,
            image_category TEXT
        );
    """)
    conn.commit()

    csv_path = "data/processed/yolo_detections.csv"
    if not os.path.exists(csv_path):
        print("❌ No YOLO CSV found. Run `python src/yolo_detect.py` first.")
        return

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = [
            (
                row["message_id"],
                row["channel_name"].strip().lower(),  # ← NORMALIZE HERE
                row["detected_objects"],
                float(row["confidence_score"]) if row["confidence_score"] else 0.0,
                row["image_category"]
            )
            for row in reader
        ]

    if records:
        cur.executemany("""
            INSERT INTO raw.yolo_detections 
            (message_id, channel_name, detected_objects, confidence_score, image_category)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, records)
        conn.commit()
        print(f"✅ Loaded {len(records)} YOLO records into raw.yolo_detections")
    else:
        print("📭 No records to load.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    load_yolo_results()
    