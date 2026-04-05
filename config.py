"""
AIAP Backend — Application Configuration
Supports environment-variable-driven config with dotenv.
DB_AVAILABLE flag controls whether the system uses PostgreSQL or the Smart Stub.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Flask core
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', SECRET_KEY)

    # ── Database ────────────────────────────────────────────────────────────
    DATABASE_URL = os.environ.get('DATABASE_URL', '')
    DB_AVAILABLE = os.environ.get('DB_AVAILABLE', 'false').lower() == 'true'

    # ── Paths ───────────────────────────────────────────────────────────────
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
    MODEL_DIR = os.path.join(BASE_DIR, 'ml_engine', 'saved_models')

    # ── Fixture CSVs (Smart Stub) ───────────────────────────────────────────
    FIXTURE_MASTER_CSV = os.path.join(RAW_DATA_DIR, 'atm_fixture_master.csv')
    FIXTURE_METRICS_CSV = os.path.join(RAW_DATA_DIR, 'atm_fixture_daily_metrics.csv')
    FIXTURE_MAINTENANCE_CSV = os.path.join(RAW_DATA_DIR, 'atm_fixture_maintenance_logs.csv')

    # ── CORS ────────────────────────────────────────────────────────────────
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*')

    # ── Synthetic Supervisor noise ──────────────────────────────────────────
    NOISE_SIGMA_HEALTH = float(os.environ.get('NOISE_SIGMA_HEALTH', '2.5'))
    NOISE_SIGMA_CASH = float(os.environ.get('NOISE_SIGMA_CASH', '0.5'))
    LABEL_FLIP_RATE = float(os.environ.get('LABEL_FLIP_RATE', '0.02'))
