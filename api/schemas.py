# api/schemas.py
"""
Pydantic models: define the structure of API requests and responses.

Why Pydantic?
- Automatic data validation
- Clear contract between server and client
- Auto-generated OpenAPI docs (Swagger)

Each class = one type of response.
"""

from pydantic import BaseModel
from typing import List, Optional

class TopProduct(BaseModel):
    """
    Response for /api/reports/top-products
    
    Represents a frequently mentioned term in messages.
    """
    term: str          # e.g., "paracetamol"
    frequency: int     # how many times it appeared

class ChannelActivity(BaseModel):
    """
    Response for /api/channels/{channel}/activity
    
    Daily posting stats for a channel.
    """
    date: str          # ISO date: "2026-01-17"
    message_count: int # number of posts that day
    avg_views: float   # average views per post

class MessageSearchResult(BaseModel):
    """
    Response for /api/search/messages
    
    A single matching message.
    """
    message_id: int
    channel_name: str
    message_text: str
    views: int
    date: str          # ISO date

class VisualContentStat(BaseModel):
    """
    Response for /api/reports/visual-content
    
    Summary of image usage per channel.
    """
    channel_name: str
    image_posts: int           # total images posted
    avg_confidence: float      # average YOLO confidence
    top_category: str          # most common image type