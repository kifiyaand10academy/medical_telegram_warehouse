# notebooks/analyze_yolo.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('postgres_host'),
    port=os.getenv('postgres_port'),
    dbname=os.getenv('postgres_db'),
    user=os.getenv('postgres_user'),
    password=os.getenv('postgres_password'),
    sslmode=os.getenv('DB_SSLMODE', 'prefer')
)

cur = conn.cursor()
cur.execute("""
    SELECT 
        d.image_category,
        ROUND(AVG(m.views), 2) AS avg_views,   -- ← CHANGED: views instead of view_count
        COUNT(*) AS post_count
    FROM fct_image_detections d
    JOIN fct_messages m ON d.message_id = m.message_id
    WHERE d.image_category IN ('promotional', 'product_display')
    GROUP BY d.image_category
    ORDER BY avg_views DESC;
""")

results = cur.fetchall()
print("Image Category Analysis:")
print("Category          | Avg Views | Post Count")
print("------------------+-----------+-----------")
for row in results:
    print(f"{row[0]:<18}| {row[1]:>9} | {row[2]:>10}")

cur.close()
conn.close()