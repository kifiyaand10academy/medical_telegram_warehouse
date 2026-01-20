# src/config.py
"""
Central configuration for the project.

Why this exists:
- Avoid hardcoding paths and model names
- Make changes in ONE place
- Cleaner, more testable code
"""

from pathlib import Path

# -----------------------------
# PROJECT ROOT
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# -----------------------------
# DATA DIRECTORIES
# -----------------------------
RAW_IMAGE_DIR = PROJECT_ROOT / "data" / "raw" / "images"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure processed directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# YOLO CONFIGURATION
# -----------------------------
YOLO_MODEL_NAME = "yolov8n.pt"  # nano model (CPU friendly)

# Output file
YOLO_OUTPUT_CSV = PROCESSED_DIR / "yolo_detections.csv"

# -----------------------------
# BUSINESS LOGIC CONFIG
# -----------------------------
# Objects we treat as "products"
PRODUCT_KEYWORDS = {
    "bottle",
    "box",
    "cup",
    "vase",
    "medicine",
    "pill",
    "car",
    "bus",
    "truck",
}
