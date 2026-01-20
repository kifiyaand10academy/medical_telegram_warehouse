# pipeline.py
import os
from dagster import Definitions, job, op

PROJECT_ROOT = "/home/haben-etsay/Pictures/week 8/medical-telegram-warehouse"

@op
def scrape_telegram_data():
    os.chdir(PROJECT_ROOT)
    exit_code = os.system("python src/scraper.py")
    if exit_code != 0:
        raise Exception("Scraping failed")

@op
def load_raw_to_postgres():
    os.chdir(PROJECT_ROOT)
    exit_code = os.system("python src/load_raw_to_postgres.py")
    if exit_code != 0:
        raise Exception("Loading failed")

@op
def run_dbt_transformations():
    os.chdir(os.path.join(PROJECT_ROOT, "medical_warehouse"))
    exit_code = os.system("dbt run")
    if exit_code != 0:
        raise Exception("dbt run failed")

@op
def run_yolo_enrichment():
    os.chdir(PROJECT_ROOT)
    exit_code1 = os.system("python src/yolo_detect.py")
    exit_code2 = os.system("python src/load_yolo_to_postgres.py")
    if exit_code1 != 0 or exit_code2 != 0:
        raise Exception("YOLO enrichment failed")

@job
def telegram_medical_pipeline():
    scraped = scrape_telegram_data()
    loaded = load_raw_to_postgres()
    transformed = run_dbt_transformations()
    enriched = run_yolo_enrichment()
    
    loaded.after(scraped)
    transformed.after(loaded)
    enriched.after(transformed)

defs = Definitions(jobs=[telegram_medical_pipeline])