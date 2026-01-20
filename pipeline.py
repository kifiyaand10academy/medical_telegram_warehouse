# pipeline.py
"""
Dagster pipeline for end-to-end Telegram medical data processing.
Orchestrates: scrape → load → transform → enrich
"""

import os
import subprocess
from dagster import job, op, Definitions, get_dagster_logger

@op
def scrape_telegram_data():
    """Run Telegram scraper."""
    logger = get_dagster_logger()
    logger.info("Starting Telegram scraping...")
    result = subprocess.run(
        ["python", "src/scraper.py"],
        cwd=os.getcwd(),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Scraping failed: {result.stderr}")
    logger.info("Scraping completed.")

@op
def load_raw_to_postgres():
    """Load raw JSON to PostgreSQL."""
    logger = get_dagster_logger()
    logger.info("Loading raw data to PostgreSQL...")
    result = subprocess.run(
        ["python", "src/load_raw_to_postgres.py"],
        cwd=os.getcwd(),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Loading failed: {result.stderr}")
    logger.info("Raw data loaded.")

@op
def run_dbt_transformations():
    """Run dbt models."""
    logger = get_dagster_logger()
    logger.info("Running dbt transformations...")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=os.path.join(os.getcwd(), "medical_warehouse"),
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    logger.info("dbt transformations completed.")

@op
def run_yolo_enrichment():
    """Run YOLO detection and load results."""
    logger = get_dagster_logger()
    logger.info("Running YOLO enrichment...")
    # Run detection
    result1 = subprocess.run(
        ["python", "src/yolo_detect.py"],
        cwd=os.getcwd(),
        capture_output=True,
        text=True
    )
    if result1.returncode != 0:
        raise Exception(f"YOLO detection failed: {result1.stderr}")
    
    # Load to DB
    result2 = subprocess.run(
        ["python", "src/load_yolo_to_postgres.py"],
        cwd=os.getcwd(),
        capture_output=True,
        text=True
    )
    if result2.returncode != 0:
        raise Exception(f"YOLO loading failed: {result2.stderr}")
    
    logger.info("YOLO enrichment completed.")

@job
def telegram_medical_pipeline():
    """
    Full end-to-end pipeline.
    Ops execute in the order they are called.
    """
    # Step 1: Scrape
    scrape_telegram_data()
    
    # Step 2: Load raw data
    load_raw_to_postgres()
    
    # Step 3: Transform with dbt
    run_dbt_transformations()
    
    # Step 4: Enrich with YOLO
    run_yolo_enrichment()

# ✅ Required for Dagster v1.4+
defs = Definitions(
    jobs=[telegram_medical_pipeline]
)