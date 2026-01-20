# debug_yolo.py
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn=psycopg2.connect(
        host=os.getenv('postgres_host'),
        port=os.getenv('postgres_port'),
        dbname=os.getenv('postgres_db'),
        user=os.getenv('postgres_user'),
        password=os.getenv('postgres_password'),
)

cur = conn.cursor()

# Check raw YOLO data
cur.execute("SELECT COUNT(*) FROM raw.yolo_detections;")
print("Raw YOLO records:", cur.fetchone()[0])

# Check fact table
cur.execute("SELECT COUNT(*) FROM fct_image_detections;")
print("Fact table records:", cur.fetchone()[0])

# Test full query
try:
    cur.execute("""
        SELECT 
            c.channel_name,
            COUNT(*) as image_posts,
            ROUND(AVG(d.confidence_score)::NUMERIC, 3) as avg_confidence
        FROM fct_image_detections d
        JOIN dim_channels c ON d.channel_key = c.channel_key
        GROUP BY c.channel_name
        LIMIT 1;
    """)
    print("Query result:", cur.fetchall())
except Exception as e:
    print("Query failed:", e)

cur.close()
conn.close()