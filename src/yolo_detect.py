# src/yolo_detect.py
"""
TASK 3: Data Enrichment with YOLOv8

PURPOSE:
- Run object detection on images scraped from Telegram
- Convert raw detections into business-friendly categories
- Save the enriched results to a CSV file

This script USES:
- YOLOv8 for object detection
- config.py for all paths and configuration
"""

# -----------------------------
# STANDARD LIBRARY IMPORTS
# -----------------------------

import csv  # Used to write results into a CSV file

# -----------------------------
# THIRD-PARTY IMPORTS
# -----------------------------

from ultralytics import YOLO  # YOLOv8 object detection model

# -----------------------------
# PROJECT CONFIGURATION IMPORTS
# -----------------------------
# All paths, model names, and business rules live here
# This keeps the logic clean and flexible

from config import (
    RAW_IMAGE_DIR,     # data/raw/images/
    YOLO_MODEL_NAME,   # yolov8n.pt
    YOLO_OUTPUT_CSV,   # data/processed/yolo_detections.csv
    PRODUCT_KEYWORDS   # set of product-like object names
)

# -----------------------------
# LOAD YOLO MODEL
# -----------------------------
# This loads a pre-trained YOLOv8 model.
# If the model is not present locally, YOLO will download it automatically.
# 'yolov8n.pt' is lightweight and runs well on CPU.

model = YOLO(YOLO_MODEL_NAME)

# ============================================================
# IMAGE CLASSIFICATION LOGIC
# ============================================================

def classify_image(detections):
    """
    Converts YOLO detection results into a business category.

    INPUT:
    - detections: YOLO result object containing:
        - detected object classes
        - confidence scores
        - bounding boxes

    OUTPUT:
    - category (str): promotional | product_display | lifestyle | other
    - max_confidence (float): highest detection confidence
    """

    # Mapping from class ID → class name
    # Example: {0: 'person', 39: 'bottle', ...}
    class_names = detections.names

    # Bounding box information (classes + confidence)
    boxes = detections.boxes

    # ------------------------------------------------
    # CASE 1: No objects detected at all
    # ------------------------------------------------
    if not boxes or len(boxes.cls) == 0:
        # YOLO saw nothing meaningful in the image
        return "other", 0.0

    # ------------------------------------------------
    # Extract detected class names
    # ------------------------------------------------
    # Convert class IDs (integers) into readable names
    detected_classes = [
        class_names[int(cls)]
        for cls in boxes.cls
    ]

    # ------------------------------------------------
    # Extract confidence scores
    # ------------------------------------------------
    # Each detected object has a confidence score
    confidences = boxes.conf.tolist()

    # Use the highest confidence as a summary score
    max_conf = max(confidences)

    # ------------------------------------------------
    # Business logic checks
    # ------------------------------------------------

    # Does the image contain a person?
    has_person = "person" in detected_classes

    # Does the image contain a product-like object?
    # PRODUCT_KEYWORDS comes from config.py
    has_product = any(
        obj in PRODUCT_KEYWORDS
        for obj in detected_classes
    )

    # ------------------------------------------------
    # Final classification rules
    # ------------------------------------------------

    if has_person and has_product:
        # Example: person holding medicine or cosmetics
        return "promotional", max_conf

    elif has_product and not has_person:
        # Example: product on table or shelf
        return "product_display", max_conf

    elif has_person and not has_product:
        # Example: selfie, lifestyle photo
        return "lifestyle", max_conf

    else:
        # Example: car, building, animal, etc.
        return "other", max_conf

# ============================================================
# MAIN YOLO PIPELINE
# ============================================================

def run_yolo_on_images():
    """
    Main pipeline function:
    - Loops through all downloaded Telegram images
    - Runs YOLO object detection
    - Classifies each image
    - Saves results into a CSV file
    """

    # List that will store results for ALL images
    results = []

    # ------------------------------------------------
    # Safety check: ensure image directory exists
    # ------------------------------------------------
    if not RAW_IMAGE_DIR.exists():
        print("⚠️ No images found in data/raw/images/")
        print("⚠️ Skipping YOLO detection step.")
        return

    # ------------------------------------------------
    # Loop through channel directories
    # Example: data/raw/images/lobelia4cosmetics/
    # ------------------------------------------------
    for channel_dir in RAW_IMAGE_DIR.iterdir():

        # Skip files, only process folders
        if not channel_dir.is_dir():
            continue

        channel_name = channel_dir.name
        print(f"🔍 Processing channel: {channel_name}")

        # ------------------------------------------------
        # Loop through images inside each channel
        # ------------------------------------------------
        for img_path in channel_dir.glob("*.jpg"):

            # Message ID comes from the filename
            # Example: 123456789.jpg → message_id = 123456789
            message_id = img_path.stem

            try:
                # ----------------------------------------
                # Run YOLO detection on the image
                # ----------------------------------------
                # model(img_path) returns a list of results
                # [0] because we pass only ONE image
                detections = model(
                    img_path,
                    verbose=False  # suppress YOLO logs
                )[0]

                # ----------------------------------------
                # Classify image using business rules
                # ----------------------------------------
                category, confidence = classify_image(detections)

                # ----------------------------------------
                # Collect detected object names (debugging)
                # ----------------------------------------
                detected_objects = ""
                if detections.boxes:
                    detected_objects = ", ".join(
                        sorted(
                            {
                                detections.names[int(cls)]
                                for cls in detections.boxes.cls
                            }
                        )
                    )

                # ----------------------------------------
                # Append row to results list
                # ----------------------------------------
                results.append({
                    "message_id": message_id,
                    "channel_name": channel_name,
                    "detected_objects": detected_objects,
                    "confidence_score": round(confidence, 4),
                    "image_category": category
                })

                # Print progress for visibility
                print(
                    f"  ✅ {message_id} → {category} "
                    f"(conf: {confidence:.2f})"
                )

            except Exception as e:
                # Catch errors like corrupted images
                print(f"  ❌ Error processing {img_path}: {e}")

    # ------------------------------------------------
    # SAVE RESULTS TO CSV
    # ------------------------------------------------
    if results:
        with open(
            YOLO_OUTPUT_CSV,
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=results[0].keys()
            )

            # Write column headers
            writer.writeheader()

            # Write all rows
            writer.writerows(results)

        print(
            f"\n🎉 Saved {len(results)} rows to "
            f"{YOLO_OUTPUT_CSV}"
        )
    else:
        print("📭 No images were processed.")

# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

# This allows you to run the file directly:
# python src/yolo_detect.py
if __name__ == "__main__":
    run_yolo_on_images()
