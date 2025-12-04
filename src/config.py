import os
from datetime import datetime

# Ruta raíz del proyecto dentro del contenedor
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

DATA_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "bronze")
DATA_SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
DATA_GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")

def now_timestamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
