from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

SILVER_DIR = PROJECT_ROOT / "data" / "silver"

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
