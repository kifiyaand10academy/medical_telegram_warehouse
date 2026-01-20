# api/main.py
"""
Analytical API for Ethiopian Medical Telegram Data Warehouse

Exposes 4 business-focused endpoints:
1. Top mentioned medical products (with noise filtering)
2. Channel activity trends
3. Keyword-based message search
4. Visual content analysis (YOLO-enriched)

All queries run on your dbt-transformed PostgreSQL warehouse.
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
import re
from api.database import get_db
from api.schemas import TopProduct, ChannelActivity, MessageSearchResult, VisualContentStat

# -----------------------------
# STOP WORDS FOR PRODUCT ANALYSIS
# -----------------------------
# Comprehensive list to filter out prices, dosages, days, generic terms
STOP_WORDS = {
    # Common English stop words
    "and", "the", "for", "from", "with", "you", "your", "our", "new", "now",
    "of", "to", "in", "is", "it", "on", "at", "by", "as", "be", "are", "this",
    "please", "thanks", "thank", "very", "good", "best", "quality", "original",
    
    # Ethiopian context: prices, currency, locations
    "birr", "etb", "price", "only", "just", "get", "buy", "call", "contact",
    "available", "delivery", "free", "today", "tomorrow", "monday", "tuesday",
    "wednesday", "thursday", "friday", "saturday", "sunday", "week", "month",
    "addis", "ababa", "ethiopia", "pharmacy", "shop", "store", "order", "send",
    
    # Dosage forms & abbreviations
    "tab", "tabs", "caps", "cap", "mg", "ml", "grm", "gram", "pcs", "piece",
    "tablet", "capsule", "inj", "injection", "syrup", "ointment", "cream",
    
    # Descriptive adjectives (not products)
    "round", "high", "low", "fees", "cost", "pay", "cash", "mobile", "telebirr",
    "cbe", "dashen", "old", "latest", "generic", "brand", "stock", "limited",
    "offer", "discount", "special", "pack", "box", "strip", "white", "yellow",
    "blue", "red", "green", "small", "large", "strong", "mild", "soft", "hard",
    "fast", "quick", "home", "quality", "original", "available"
}

# -----------------------------
# FASTAPI APP SETUP
# -----------------------------

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="Analytical API for Ethiopian medical Telegram channels built with dbt + YOLO",
    version="1.0.0"
)

@app.get("/")
def read_root():
    """Health check endpoint."""
    return {"message": "Welcome to the Medical Telegram Analytics API!"}

# ───────────────────────────────────────
# ENDPOINT 1: Top Products (Improved)
# Business Question: What are the top mentioned MEDICAL PRODUCTS?
# ───────────────────────────────────────
@app.get("/api/reports/top-products", response_model=List[TopProduct])
def get_top_products(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Returns most frequently mentioned MEDICAL TERMS across all channels.
    
    Improvements:
    - Removes pure numbers (e.g., '500', '200')
    - Filters out newlines and special characters
    - Excludes dosage forms, prices, and generic descriptors
    - Only returns terms that start with a letter
    """
    query = text("""
        WITH cleaned_messages AS (
            -- Step 1: Clean text (lowercase, keep only letters/numbers/spaces)
            SELECT 
                regexp_replace(lower(message_text), '[^a-z0-9\s]', ' ', 'g') AS clean_text
            FROM fct_messages
            WHERE message_text IS NOT NULL 
              AND length(message_text) > 0
        ),
        words AS (
            -- Step 2: Split into individual words
            SELECT 
                unnest(string_to_array(clean_text, ' ')) AS term
            FROM cleaned_messages
        )
        -- Step 3: Filter and count meaningful terms
        SELECT 
            term,
            COUNT(*) AS frequency
        FROM words
        WHERE 
            term <> ''                          -- no empty strings
            AND length(term) BETWEEN 3 AND 25   -- reasonable word length
            AND term ~ '^[a-z]'                 -- must start with a letter (exclude '500', '200')
            AND term NOT IN :stop_words         -- exclude noise
        GROUP BY term
        ORDER BY frequency DESC
        LIMIT :limit
    """)
    results = db.execute(
        query, 
        {"limit": limit, "stop_words": tuple(STOP_WORDS)}
    ).fetchall()
    
    return [TopProduct(term=row.term, frequency=row.frequency) for row in results]

# ───────────────────────────────────────
# ENDPOINT 2: Channel Activity
# Business Question: How active is a specific channel?
# ───────────────────────────────────────
@app.get("/api/channels/{channel_name}/activity", response_model=List[ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """
    Returns daily posting activity for a given channel (case-insensitive).
    Includes message count and average views per day (last 30 days).
    """
    query = text("""
        SELECT 
            d.full_date::text as date,
            COUNT(*) as message_count,
            AVG(m.views) as avg_views
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        JOIN dim_dates d ON m.date_key = d.date_key
        WHERE lower(c.channel_name) = lower(:channel_name)
        GROUP BY d.full_date
        ORDER BY d.full_date DESC
        LIMIT 30
    """)
    results = db.execute(query, {"channel_name": channel_name}).fetchall()
    
    return [
        ChannelActivity(
            date=str(row.date),
            message_count=row.message_count,
            avg_views=float(row.avg_views) if row.avg_views else 0.0
        )
        for row in results
    ]

# ───────────────────────────────────────
# ENDPOINT 3: Message Search
# Business Question: Find messages about a keyword
# ───────────────────────────────────────
@app.get("/api/search/messages", response_model=List[MessageSearchResult])
def search_messages(
    query: str = Query(..., min_length=2, description="Keyword to search for"),
    limit: int = Query(20, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Searches message_text for a keyword (case-insensitive).
    Sanitizes input to prevent injection-like issues.
    Returns most viewed matches first.
    """
    # Sanitize: keep only alphanumeric + space
    safe_query = re.sub(r'[^a-zA-Z0-9\s]', '', query)
    like_pattern = f"%{safe_query}%"
    
    sql = text("""
        SELECT 
            m.message_id,
            c.channel_name,
            m.message_text,
            m.views,
            d.full_date::text as date
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        JOIN dim_dates d ON m.date_key = d.date_key
        WHERE lower(m.message_text) LIKE lower(:pattern)
        ORDER BY m.views DESC
        LIMIT :limit
    """)
    results = db.execute(sql, {"pattern": like_pattern, "limit": limit}).fetchall()
    
    return [
        MessageSearchResult(
            message_id=row.message_id,
            channel_name=row.channel_name,
            message_text=row.message_text,
            views=row.views,
            date=str(row.date)
        )
        for row in results
    ]

# ───────────────────────────────────────
# ENDPOINT 4: Visual Content Stats
# Business Question: Which channels use more images?
# ───────────────────────────────────────
@app.get("/api/reports/visual-content", response_model=List[VisualContentStat])
def get_visual_content_stats(db: Session = Depends(get_db)):
    """
    Summarizes YOLO-based image analysis per channel.
    Shows total image posts, average detection confidence, and dominant category.
    """
    query = text("""
        WITH category_counts AS (
            SELECT 
                c.channel_name,
                d.image_category,
                COUNT(*) as category_count,
                AVG(d.confidence_score) as avg_conf
            FROM fct_image_detections d
            JOIN dim_channels c ON d.channel_key = c.channel_key
            GROUP BY c.channel_name, d.image_category
        ),
        top_categories AS (
            SELECT 
                channel_name,
                image_category,
                category_count,
                avg_conf,
                ROW_NUMBER() OVER (PARTITION BY channel_name ORDER BY category_count DESC) as rn
            FROM category_counts
        )
        SELECT 
            tc.channel_name,
            SUM(cc.category_count) as image_posts,
            ROUND(AVG(cc.avg_conf)::NUMERIC, 3) as avg_confidence,
            tc.image_category as top_category
        FROM top_categories tc
        JOIN category_counts cc ON tc.channel_name = cc.channel_name
        WHERE tc.rn = 1
        GROUP BY tc.channel_name, tc.image_category
        ORDER BY image_posts DESC
    """)
    results = db.execute(query).fetchall()
    
    return [
        VisualContentStat(
            channel_name=row.channel_name,
            image_posts=row.image_posts,
            avg_confidence=float(row.avg_confidence) if row.avg_confidence else 0.0,
            top_category=row.top_category or "unknown"
        )
        for row in results
    ]