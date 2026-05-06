"""
AIAP Backend — Application Configuration
Supports environment-variable-driven config with dotenv.
DB_AVAILABLE flag controls whether the system uses PostgreSQL or the Smart Stub.
"""
import os
from dotenv import load_dotenv

load_dotenv()

def check_db_availability(url):
    if not url:
        return False
    try:
        from sqlalchemy import create_engine
        # Set a short timeout so fallback happens quickly
        engine = create_engine(url, connect_args={'connect_timeout': 3})
        with engine.connect():
            return True
    except Exception:
        return False


class Config:
    # Flask core
    SECRET_KEY = os.environ.get('SECRET_KEY', 'aiap-dev-secret-key-change-in-production-2026')
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', SECRET_KEY)

    # ── Database ────────────────────────────────────────────────────────────
    DATABASE_URL = os.environ.get('DATABASE_URL', '')
    DB_AVAILABLE = check_db_availability(DATABASE_URL)
    DATA_SOURCE = 'db' if DB_AVAILABLE else 'file'


    # ── Paths ───────────────────────────────────────────────────────────────
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    DATASETS_DIR = os.path.join(DATA_DIR, 'datasets')
    RAW_DATA_DIR = DATASETS_DIR  # Backward-compatible alias (legacy code references this)
    RAW_BACKUP_DIR = os.path.join(DATA_DIR, 'raw_backup')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
    MODEL_DIR = os.path.join(BASE_DIR, 'ml_engine', 'saved_models')

    # ── Master Metadata ─────────────────────────────────────────────────────
    ATM_METADATA_CSV = os.path.join(DATASETS_DIR, 'atm_metadata.csv')

    # ── CORS ────────────────────────────────────────────────────────────────
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*')

    # ── Synthetic Supervisor noise ──────────────────────────────────────────
    NOISE_SIGMA_HEALTH = float(os.environ.get('NOISE_SIGMA_HEALTH', '2.5'))
    NOISE_SIGMA_CASH = float(os.environ.get('NOISE_SIGMA_CASH', '0.5'))
    LABEL_FLIP_RATE = float(os.environ.get('LABEL_FLIP_RATE', '0.02'))
