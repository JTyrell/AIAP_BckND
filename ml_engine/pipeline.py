"""
AIAP ML Engine — End-to-End Training + Inference Pipeline

Implements:
  - train_all_models()  : Synthetic Supervisor pattern with optional real labels
  - predict_for_atm()   : per-ATM inference returning prediction dict
  - generate_alerts()   : threshold-based alert code generation

Training Strategy (per user spec):
  1. If data/processed/training_labels.csv exists → supervised training
  2. Otherwise → proxy labels from deterministic PDF formulas + noise injection
     (Gaussian sigma=2.5 for health, sigma=0.5 for cash, 2% label-flip for failure)
"""
import os
import logging
import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import r2_score

from config import Config
from ml_engine.feature_engineering import engineer_features
from ml_engine.models import (
    build_health_model, build_cash_model,
    build_failure_model, build_activity_model,
    HEALTH_FEATURES, CASH_FEATURES,
    FAILURE_FEATURES, ACTIVITY_FEATURES,
)

logger = logging.getLogger(__name__)

LABELS_FILE = os.path.join(
    Config.PROCESSED_DATA_DIR, 'training_labels.csv',
)


# ─────────────────────────────────────────────────────────────────────────────
#  Training
# ─────────────────────────────────────────────────────────────────────────────

def train_all_models(df=None, repository=None):
    """
    Train all 4 ML models and save to ml_engine/saved_models/.

    Args:
        df: pre-loaded DataFrame (optional; bypasses repository)
        repository: DataRepository instance (optional; auto-resolved)

    Returns:
        dict with training metrics.
    """
    os.makedirs(Config.MODEL_DIR, exist_ok=True)

    # ── Load data ────────────────────────────────────────────────────────
    if df is None:
        from ml_engine.data_provider import get_data_provider
        provider = get_data_provider()
        df = provider.get_data(days=None)

    if df.empty:
        logger.warning("[TRAINING] No training data available. Skipping.")
        return {'status': 'no_data'}

    # ── Feature engineering ──────────────────────────────────────────────
    df_feat = engineer_features(df)

    # Ensure all feature columns exist (fill missing with 0)
    all_feats = (
        HEALTH_FEATURES + CASH_FEATURES
        + FAILURE_FEATURES + ACTIVITY_FEATURES
    )
    for f in all_feats:
        if f not in df_feat.columns:
            df_feat[f] = 0
            logger.warning(f"[TRAINING] Feature '{f}' missing, filled with 0")

    # ── Determine label source ───────────────────────────────────────────
    use_real = os.path.exists(LABELS_FILE)

    if use_real:
        logger.info(
            "[TRAINING] Found real labels at %s. Using supervised training.",
            LABELS_FILE,
        )
        labels = pd.read_csv(LABELS_FILE)
        labels['record_date'] = pd.to_datetime(labels['record_date'])
        df_feat = df_feat.merge(
            labels, on=['atm_id', 'record_date'], how='inner',
        )
        y_health = df_feat['actual_health_score']
        y_failure = df_feat['actual_failed_next_7_days'].astype(int)
        y_cash = df_feat['actual_days_to_depletion']
    else:
        sigma_h = Config.NOISE_SIGMA_HEALTH
        sigma_c = Config.NOISE_SIGMA_CASH
        flip_rate = Config.LABEL_FLIP_RATE

        logger.warning(
            "[TRAINING] Labels file not found. Using synthetic supervision "
            "with noise injection. sigma_health=%.1f, sigma_cash=%.1f, "
            "flip_rate=%.2f",
            sigma_h, sigma_c, flip_rate,
        )

        # Pseudo-labels from deterministic formulas
        y_health_clean = df_feat['composite_health_score'].clip(0, 100)
        y_failure_clean = (
            df_feat['failure_risk_score'] > 0.5
        ).astype(int)

        consumption_daily = (
            df_feat['cash_consumption_rate'].clip(lower=1) * 24
        )
        y_cash_clean = (
            df_feat['ending_cash_balance'] / consumption_daily
        ).clip(0, 60)

        # Inject noise
        n = len(df_feat)
        y_health = (
            y_health_clean
            + np.random.normal(0, sigma_h, n)
        ).clip(0, 100)

        y_cash = (
            y_cash_clean
            + np.random.normal(0, sigma_c, n)
        ).clip(0, 60)

        flip_mask = np.random.random(n) < flip_rate
        y_failure = y_failure_clean.copy()
        y_failure.loc[flip_mask] = 1 - y_failure.loc[flip_mask]

    # ── Time-Series Split ────────────────────────────────────────────────
    n_splits = min(5, max(2, len(df_feat) // 10))
    tscv = TimeSeriesSplit(n_splits=n_splits)
    results = {
        'rows_trained': len(df_feat),
        'used_real_labels': use_real,
    }

    # ── 1. Health Model (RF Regressor) ───────────────────────────────────
    X_h = df_feat[HEALTH_FEATURES].fillna(0)
    health_model = build_health_model()

    scores = []
    for tr, va in tscv.split(X_h):
        health_model.fit(X_h.iloc[tr], y_health.iloc[tr])
        pred = health_model.predict(X_h.iloc[va])
        scores.append(r2_score(y_health.iloc[va], pred))
    avg_r2 = float(np.mean(scores))
    results['health_r2'] = round(avg_r2, 4)
    logger.info("[TRAINING] Health Model CV R²: %.4f", avg_r2)

    health_model.fit(X_h, y_health)
    joblib.dump(
        health_model,
        os.path.join(Config.MODEL_DIR, 'health_model.joblib'),
    )

    # Feature importances
    rf = health_model.named_steps['model']
    if hasattr(rf, 'feature_importances_'):
        imp = dict(zip(HEALTH_FEATURES, rf.feature_importances_))
        logger.info("[TRAINING] Health importances: %s", imp)

    # ── 2. Cash Depletion Model (LassoCV) ────────────────────────────────
    X_c = df_feat[CASH_FEATURES].fillna(0)
    cash_model = build_cash_model()
    cash_model.fit(X_c, y_cash)
    joblib.dump(
        cash_model,
        os.path.join(Config.MODEL_DIR, 'cash_depletion_model.joblib'),
    )
    logger.info("[TRAINING] Cash depletion model saved.")

    # ── 3. Failure Model (XGBoost) ───────────────────────────────────────
    X_f = df_feat[FAILURE_FEATURES].fillna(0)
    failure_model = build_failure_model()

    if y_failure.nunique() >= 2:
        failure_model.fit(X_f, y_failure)
        joblib.dump(
            failure_model,
            os.path.join(Config.MODEL_DIR, 'failure_model.joblib'),
        )
        logger.info("[TRAINING] Failure model saved.")
    else:
        logger.warning(
            "[TRAINING] Failure model skipped — only one class in labels."
        )

    # ── 4. Activity Model (KMeans — unsupervised) ────────────────────────
    X_a = df_feat[ACTIVITY_FEATURES].fillna(0)
    activity_model = build_activity_model()
    activity_model.fit(X_a)
    joblib.dump(
        activity_model,
        os.path.join(Config.MODEL_DIR, 'activity_model.joblib'),
    )
    logger.info("[TRAINING] Activity model (KMeans) saved.")

    logger.info("[TRAINING] All models trained successfully.")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Inference
# ─────────────────────────────────────────────────────────────────────────────

def predict_for_atm(atm_id, data_provider=None):
    """
    Run inference for one ATM. Returns a prediction dict matching
    the API contract expected by Prototype_V2.jsx.
    """
    if data_provider is None:
        from ml_engine.data_provider import get_data_provider
        data_provider = get_data_provider()

    df = data_provider.get_data(days=90)
    if not df.empty:
        df = df[df['atm_id'] == atm_id]
    if df.empty:
        raise ValueError(f"No data found for ATM {atm_id}")

    df_feat = engineer_features(df)
    latest = df_feat.iloc[[-1]].copy()

    # Fill any missing feature columns
    for f in (HEALTH_FEATURES + CASH_FEATURES
              + FAILURE_FEATURES + ACTIVITY_FEATURES):
        if f not in latest.columns:
            latest[f] = 0

    # Base result from deterministic formulas
    result = {
        'atm_id': atm_id,
        'health_score': round(
            float(latest['composite_health_score'].iloc[0]), 1,
        ),
        'failure_probability': 0.05,
        'activity_level': 'Moderate',
        'days_to_depletion': 7.0,
        'cash_stress_indicator': round(
            float(latest['cash_stress_indicator'].iloc[0]), 3,
        ),
        'error_acceleration': round(
            float(latest['error_acceleration'].iloc[0]), 3,
        ),
        'uptime_percentage': round(
            float(latest['uptime_percentage'].iloc[0]), 1,
        ),
        'days_since_maintenance': int(
            latest['days_since_maintenance'].fillna(999).iloc[0]
        ),
        'operational_status': (
            latest['operational_status'].iloc[0]
            if 'operational_status' in latest.columns
            else 'in_service'
        ),
    }

    # Override with trained model predictions when available
    _predict_health(latest, result)
    _predict_cash(latest, result)
    _predict_failure(latest, result)
    _predict_activity(latest, result)

    return result


def _predict_health(latest, result):
    try:
        m = joblib.load(
            os.path.join(Config.MODEL_DIR, 'health_model.joblib'),
        )
        X = latest[HEALTH_FEATURES].fillna(0)
        result['health_score'] = round(
            float(m.predict(X)[0]), 1,
        )
    except Exception as e:
        logger.debug("Health model unavailable: %s", e)


def _predict_cash(latest, result):
    try:
        m = joblib.load(
            os.path.join(Config.MODEL_DIR, 'cash_depletion_model.joblib'),
        )
        X = latest[CASH_FEATURES].fillna(0)
        result['days_to_depletion'] = round(
            max(0, float(m.predict(X)[0])), 1,
        )
    except Exception as e:
        logger.debug("Cash model unavailable: %s", e)
        # Fallback formula
        rate = latest['cash_consumption_rate'].iloc[0]
        bal = latest['ending_cash_balance'].iloc[0]
        if rate > 0:
            result['days_to_depletion'] = round(
                min(60, max(0, bal / (rate * 24))), 1,
            )


def _predict_failure(latest, result):
    try:
        m = joblib.load(
            os.path.join(Config.MODEL_DIR, 'failure_model.joblib'),
        )
        X = latest[FAILURE_FEATURES].fillna(0)
        proba = m.predict_proba(X)[0]
        result['failure_probability'] = round(
            float(proba[1]) if len(proba) > 1 else float(proba[0]),
            3,
        )
    except Exception as e:
        logger.debug("Failure model unavailable: %s", e)
        score = latest['failure_risk_score'].iloc[0]
        result['failure_probability'] = round(
            float(np.clip(score, 0, 1)), 3,
        )


def _predict_activity(latest, result):
    try:
        m = joblib.load(
            os.path.join(Config.MODEL_DIR, 'activity_model.joblib'),
        )
        X = latest[ACTIVITY_FEATURES].fillna(0)
        cluster = int(m.predict(X)[0])
        centers = m.named_steps['model'].cluster_centers_
        order = np.argsort(centers[:, 0])
        labels = {order[0]: 'Low', order[1]: 'Moderate', order[2]: 'High'}
        result['activity_level'] = labels.get(cluster, 'Moderate')
    except Exception as e:
        logger.debug("Activity model unavailable: %s", e)
        txn = latest['transaction_count'].iloc[0]
        if txn > 220:
            result['activity_level'] = 'High'
        elif txn > 100:
            result['activity_level'] = 'Moderate'
        else:
            result['activity_level'] = 'Low'


# ─────────────────────────────────────────────────────────────────────────────
#  Alerts
# ─────────────────────────────────────────────────────────────────────────────

ALERT_THRESHOLDS = {
    'CASH_CRITICAL': lambda d: d.get('cash_stress_indicator', 0) > 0.70,
    'CASH_LOW': lambda d: 0.50 < d.get('cash_stress_indicator', 0) <= 0.70,
    'ERROR_SPIKE': lambda d: d.get('error_acceleration', 0) > 0.50,
    'MAINTENANCE_DUE': (
        lambda d: 30 < d.get('days_since_maintenance', 0) <= 60
    ),
    'MAINTENANCE_OVERDUE': (
        lambda d: d.get('days_since_maintenance', 0) > 60
    ),
    'OUT_OF_SERVICE': (
        lambda d: d.get('operational_status') == 'out_of_service'
    ),
    'CRITICAL_ERROR': (
        lambda d: d.get('failure_probability', 0) > 0.70
    ),
}


def generate_alerts(prediction, atm_data=None):
    """
    Generate alert codes matching frontend ALERTS format.
    Returns list of alert code strings.
    """
    combined = {**prediction}
    if atm_data:
        combined.update(atm_data)

    alerts = []
    for code, check in ALERT_THRESHOLDS.items():
        try:
            if check(combined):
                alerts.append(code)
        except Exception:
            continue
    return alerts
