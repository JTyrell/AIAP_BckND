"""
AIAP — Data Repository (Smart Stub Pattern)

When DB_AVAILABLE=true  → queries PostgreSQL via psycopg2.
When DB_AVAILABLE=false → loads fixture CSVs from data/raw/ with
  dynamic Gaussian perturbation so the ML pipeline always has
  realistic, slightly-varying data for offline development.

Switch between modes with a single env-var change.
"""
import os
import logging
import numpy as np
import pandas as pd
from config import Config

logger = logging.getLogger(__name__)

# ── In-memory fixture cache ─────────────────────────────────────────────────
_cache = {}

def clear_cache():
    """Clear all in-memory caches to force a reload from disk."""
    global _cache
    _cache = {}
    from ml_engine.data_provider import clear_provider_cache
    clear_provider_cache()

def _load_fixture(path, parse_dates=None):
    """Load a CSV fixture into memory, caching for performance."""
    if path in _cache:
        return _cache[path].copy()
    if not os.path.exists(path):
        logger.warning(f"Fixture file not found: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, parse_dates=parse_dates or [])
        _cache[path] = df
        logger.info(f"Cached fixture: {path}  ({len(df)} rows)")
        return df.copy()
    except Exception as e:
        logger.error(f"Failed to load fixture {path}: {e}")
        return pd.DataFrame()


def _perturb(df, columns, sigma_pct=0.005):
    """
    Add small Gaussian noise to numeric columns.
    sigma_pct: noise as fraction of column mean (default ±0.5%).
    """
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors='coerce')
        sigma = max(series.std() * sigma_pct, 0.01)
        noise = np.random.normal(0, sigma, len(series))
        df[col] = (series + noise).round(2)
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  Abstract-ish interface (duck-typed; a Protocol for type checkers later)
# ─────────────────────────────────────────────────────────────────────────────


class StubRepository:
    """
    Smart Stub: serves fixture CSVs with dynamic noise injection.
    Logs a prominent warning on every access.
    """

    def __init__(self):
        logger.warning(
            "\u26a0\ufe0f DB_UNAVAILABLE: Running in Demo Mode with Fixture Data. "
            "Set DB_AVAILABLE=true and provide DATABASE_URL to use PostgreSQL."
        )

    # ── ATM master ───────────────────────────────────────────────────────
    def get_atm_master(self, atm_id=None):
        df = _load_fixture(Config.ATM_METADATA_CSV)
        if df.empty:
            return df
        if atm_id:
            df = df[df['atm_id'] == atm_id]
        return df

    # ── Daily metrics ────────────────────────────────────────────────────
    def get_daily_metrics(self, atm_id=None, days=30):
        from ml_engine.data_provider import get_data_provider
        df = get_data_provider().get_data(days=days)

        if df.empty:
            return df

        if atm_id:
            df = df[df['atm_id'] == atm_id]

        return df.reset_index(drop=True)

    # ── Maintenance logs ─────────────────────────────────────────────────
    def get_maintenance_logs(self, atm_id=None):
        maint_path = os.path.join(Config.RAW_DATA_DIR, 'maintenance_records.csv')
        df = _load_fixture(
            maint_path,
            parse_dates=['maintenance_date'],
        )
        if not df.empty:
            df = df.rename(columns={'maintenance_date': 'service_date'})
            
        if atm_id and not df.empty:
            df = df[df['atm_id'] == atm_id]
        return df

    # ── Combined metrics + maintenance (for feature engineering) ─────────
    def get_metrics_with_maintenance(self, atm_id=None, days=30):
        metrics = self.get_daily_metrics(atm_id=atm_id, days=days)
        maint = self.get_maintenance_logs(atm_id=atm_id)
        if metrics.empty:
            return metrics
        if maint.empty:
            metrics['service_date'] = pd.NaT
            return metrics
        # Left-join: each metric row gets its ATM's most recent service date
        maint_latest = (
            maint.sort_values('service_date')
            .drop_duplicates(subset='atm_id', keep='last')
            [['atm_id', 'service_date']]
        )
        return metrics.merge(maint_latest, on='atm_id', how='left')

    # ── Fleet snapshot (for KPIs) ────────────────────────────────────────
    def get_fleet_snapshot(self):
        metrics = self.get_daily_metrics()
        if metrics.empty:
            return metrics
        latest = (
            metrics.sort_values('record_date')
            .drop_duplicates(subset='atm_id', keep='last')
        )
        master = self.get_atm_master()
        if not master.empty:
            latest = latest.merge(master, on='atm_id', how='left')
        return latest

    # ── Save metrics (no-op in stub) ─────────────────────────────────────
    def save_metrics(self, df):
        logger.warning(
            "[STUB MODE] save_metrics called but DB is unavailable. "
            "Data NOT persisted. %d rows discarded.", len(df),
        )
        return len(df)


class PostgresRepository:
    """
    Live PostgreSQL data access. Activated when DB_AVAILABLE=true.
    Delegates queries to psycopg2 via utils.db.get_db_connection.
    """

    def _conn(self):
        from utils.db import get_db_connection
        return get_db_connection()

    def get_atm_master(self, atm_id=None):
        conn = self._conn()
        q = "SELECT * FROM atm_master"
        params = []
        if atm_id:
            q += " WHERE atm_id = %s"
            params = [atm_id]
        df = pd.read_sql(q, conn, params=params)
        conn.close()
        return df

    def get_daily_metrics(self, atm_id=None, days=30):
        conn = self._conn()
        q = """
            SELECT * FROM atm_daily_metrics
            WHERE record_date >= CURRENT_DATE - INTERVAL '%s days'
        """
        params = [days]
        if atm_id:
            q += " AND atm_id = %s"
            params.append(atm_id)
        q += " ORDER BY atm_id, record_date"
        df = pd.read_sql(q, conn, params=params)
        conn.close()
        return df

    def get_maintenance_logs(self, atm_id=None):
        conn = self._conn()
        q = "SELECT * FROM maintenance_logs"
        params = []
        if atm_id:
            q += " WHERE atm_id = %s"
            params = [atm_id]
        q += " ORDER BY service_date DESC"
        df = pd.read_sql(q, conn, params=params)
        conn.close()
        return df

    def get_metrics_with_maintenance(self, atm_id=None, days=30):
        conn = self._conn()
        q = """
            SELECT m.*, ml.service_date
            FROM atm_daily_metrics m
            LEFT JOIN (
                SELECT atm_id, MAX(service_date) AS service_date
                FROM maintenance_logs GROUP BY atm_id
            ) ml ON m.atm_id = ml.atm_id
            WHERE m.record_date >= CURRENT_DATE - INTERVAL '%s days'
        """
        params = [days]
        if atm_id:
            q += " AND m.atm_id = %s"
            params.append(atm_id)
        q += " ORDER BY m.atm_id, m.record_date"
        df = pd.read_sql(q, conn, params=params)
        conn.close()
        return df

    def get_fleet_snapshot(self):
        conn = self._conn()
        q = """
            SELECT am.*, adm.*
            FROM atm_master am
            JOIN atm_daily_metrics adm ON am.atm_id = adm.atm_id
            WHERE adm.record_date = (
                SELECT MAX(record_date)
                FROM atm_daily_metrics WHERE atm_id = am.atm_id
            )
        """
        df = pd.read_sql(q, conn)
        conn.close()
        return df

    def save_metrics(self, df):
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO atm_daily_metrics (
                    atm_id, record_date, uptime_percentage, error_count,
                    transaction_count, starting_cash_balance,
                    ending_cash_balance
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (atm_id, record_date) DO UPDATE SET
                    uptime_percentage = EXCLUDED.uptime_percentage,
                    error_count = EXCLUDED.error_count,
                    transaction_count = EXCLUDED.transaction_count,
                    starting_cash_balance = EXCLUDED.starting_cash_balance,
                    ending_cash_balance = EXCLUDED.ending_cash_balance
            """, (
                row['atm_id'], row['record_date'],
                row['uptime_percentage'], row['error_count'],
                row['transaction_count'], row['starting_cash_balance'],
                row['ending_cash_balance'],
            ))
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count


# ── Factory ──────────────────────────────────────────────────────────────────
_repo_instance = None


def get_repository():
    """Return the singleton repository (Stub or Postgres)."""
    global _repo_instance
    if _repo_instance is None:
        if Config.DB_AVAILABLE:
            _repo_instance = PostgresRepository()
            logger.info("Using PostgreSQL repository.")
        else:
            _repo_instance = StubRepository()
    return _repo_instance
