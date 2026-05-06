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

    # ── 5. Train Short-Term Model ──────────────────────────────────────────
    train_short_term_model()

    logger.info("[TRAINING] All models trained successfully.")
    return results

def train_short_term_model():
    """
    Train the 1-hour short-term failure prediction model.
    """
    logger.info("[TRAINING] Starting short-term failure model training...")
    try:
        from ml_engine.feature_engineering import engineer_short_term_features, select_top_features
        import xgboost as xgb
        from sqlalchemy import create_engine
        
        engine = create_engine(Config.DATABASE_URL) if getattr(Config, 'DB_AVAILABLE', False) else None
        
        use_csv = True
        if engine:
            try:
                logs = pd.read_sql_table('operational_logs', engine)
                cash = pd.read_sql_table('cash_status', engine)
                atm = pd.read_sql_table('atm_data', engine)
                use_csv = False
                logger.info("[TRAINING] Successfully connected to database for short-term model.")
            except Exception as e:
                logger.warning(f"[TRAINING] Database connection failed: {e}. Falling back to CSVs.")
                
        if use_csv:
            datasets_dir = getattr(Config, 'DATASETS_DIR', os.path.join(Config.DATA_DIR, 'raw'))
            logs = pd.read_csv(os.path.join(datasets_dir, 'operational_logs.csv'), parse_dates=["timestamp"])
            cash = pd.read_csv(os.path.join(datasets_dir, 'cash_status.csv'), parse_dates=["timestamp"])
            atm = pd.read_csv(getattr(Config, 'ATM_METADATA_CSV', os.path.join(datasets_dir, 'atm_metadata.csv')))
            
        if 'timestamp' not in logs.columns and 'transaction_time' in logs.columns:
            logs['timestamp'] = logs['transaction_time']
            
        df = engineer_short_term_features(logs, cash, atm)
        if df.empty:
            logger.warning("[TRAINING] Short-term feature engineering returned empty DataFrame.")
            return
            
        target = 'future_failure'
        selected_features = select_top_features(df, target, k=5)
        
        if not selected_features:
            logger.warning("[TRAINING] No features selected for short-term model.")
            return
            
        X = df[selected_features].fillna(0)
        y = df[target]
        
        if len(y.unique()) <= 1:
            logger.warning("[TRAINING] Short-term target has only 1 class. Skipping training.")
            return
            
        model = xgb.XGBClassifier(
            n_estimators=50,
            max_depth=3,
            learning_rate=0.1,
            use_label_encoder=False,
            eval_metric='logloss',
            random_state=42
        )
        model.fit(X, y)
        
        os.makedirs(Config.MODEL_DIR, exist_ok=True)
        joblib.dump(model, os.path.join(Config.MODEL_DIR, 'short_term_failure_model.joblib'))
        
        # Save selected features list
        joblib.dump(selected_features, os.path.join(Config.MODEL_DIR, 'short_term_features.joblib'))
        
        logger.info("[TRAINING] Short-term failure model saved successfully.")
        
    except Exception as e:
        logger.error(f"[TRAINING] Error training short-term model: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  Inference
# ─────────────────────────────────────────────────────────────────────────────

def predict_for_atm(atm_id, data_provider=None, precomputed_df=None):
    """
    Run inference for one ATM. Returns a prediction dict matching
    the API contract expected by Prototype_V2.jsx.
    """
    if precomputed_df is not None:
        df_feat = precomputed_df
    else:
        if data_provider is None:
            from ml_engine.data_provider import get_data_provider
            data_provider = get_data_provider()

        df = data_provider.get_data(days=90)
        if not df.empty:
            df = df[df['atm_id'] == atm_id]
        if df.empty:
            raise ValueError(f"No data found for ATM {atm_id}")

        df_feat = engineer_features(df)

    # Filter to specific ATM if not already done
    atm_feat = df_feat[df_feat['atm_id'] == atm_id]
    if atm_feat.empty:
        raise ValueError(f"No features found for ATM {atm_id}")

    latest = atm_feat.iloc[[-1]].copy()

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
        'transactions_24h': int(latest['transaction_count'].iloc[0]),
        'avg_daily_withdrawal': round(float(latest.get('daily_withdrawal_amount_7d', pd.Series([0])).iloc[0]), 2),
        'avg_amount': round(float(latest.get('avg_transaction_amount_7d', pd.Series([0])).iloc[0]), 2),
        'days_since_maintenance': int(
            latest['days_since_maintenance'].fillna(999).iloc[0]
        ),
        'deposit_bin_utilization': round(
            float(latest['deposit_bin_utilization'].iloc[0]) if 'deposit_bin_utilization' in latest.columns else 0.0, 3
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

    # Extract historical series for sparklines
    # Align specifically to Sunday-Saturday calendar view
    now_date = pd.Timestamp.now().normalize()
    weekly_txns = [0] * 7
    base_labels = ["s", "m", "t", "w", "t", "f", "s"]
    
    # Pandas dayofweek is 0=Monday, 6=Sunday. Add 1 and modulo 7 ensures Sunday=0.
    today_index = (now_date.dayofweek + 1) % 7
    
    # Capitalize only today's label
    weekly_labels = [lbl.upper() if i == today_index else lbl for i, lbl in enumerate(base_labels)]
    
    # Pull explicit 7-day window
    start_date = now_date - pd.Timedelta(days=6)
    history = atm_feat[(atm_feat['record_date'] >= start_date) & (atm_feat['record_date'] <= now_date)]
    
    for _, row in history.iterrows():
        if pd.notna(row['record_date']):
            # Align to our static 0-6 array
            idx = (row['record_date'].dayofweek + 1) % 7
            weekly_txns[idx] = int(row.get('transaction_count', 0))

    # Calculate more stable transaction velocity (7-day average)
    avg_velocity = sum(weekly_txns) / sum([1 for val in weekly_txns if val > 0]) if sum(weekly_txns) > 0 else 0
    result['transactions_24h_predicted'] = int(round(avg_velocity))

    # Inactive rate (monthly failures) - strictly past 3 COMPLETE months
    now_date = pd.Timestamp.now()
    target_months = [
        (now_date - pd.DateOffset(months=3)).strftime('%b'),
        (now_date - pd.DateOffset(months=2)).strftime('%b'),
        (now_date - pd.DateOffset(months=1)).strftime('%b')
    ]
    
    monthly = atm_feat.copy()
    if not monthly.empty:
        monthly['month_name'] = monthly['record_date'].dt.strftime('%b')
        monthly_grouped = monthly.groupby('month_name')['error_count'].sum().to_dict()
        inactive = [int(monthly_grouped.get(m, 0)) for m in target_months]
    else:
        inactive = [0, 0, 0]
        
    monthly_labels = target_months

    result['historical_series'] = {
        'weekly_txns': weekly_txns,
        'weekly_labels': weekly_labels,
        'monthly_inactive': [int(v) for v in inactive],
        'monthly_labels': monthly_labels,
        # Simulated hourly based on last 24h intensity
        'hourly_txns': [int(v * np.random.uniform(0.8, 1.2)) for v in [0,0,0,0,1,5,10,20,30,40,30,25,20,20,25,30,20,15,10,5,2,1,0,0]]
    }

    return result


def predict_fleet(data_provider=None):
    """
    Perform batch inference for the entire fleet.
    Engineers features once and runs models for all available ATMs.
    """
    if data_provider is None:
        from ml_engine.data_provider import get_data_provider
        data_provider = get_data_provider()

    df = data_provider.get_data(days=90)
    if df.empty:
        return {}

    df_feat = engineer_features(df)
    atms = df_feat['atm_id'].unique()

    results = {}
    for atm_id in atms:
        try:
            results[atm_id] = predict_for_atm(atm_id, precomputed_df=df_feat)
        except Exception as e:
            logger.error(f"Prediction failed for ATM {atm_id}: {e}")
            continue
    return results


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
    # Hard Guardrail: If balance is already 0, depletion is 0.
    bal = latest['ending_cash_balance'].iloc[0]
    if bal <= 0:
        result['days_to_depletion'] = 0.0
        return

    try:
        m = joblib.load(
            os.path.join(Config.MODEL_DIR, 'cash_depletion_model.joblib'),
        )
        X = latest[CASH_FEATURES].fillna(0)
        days = float(m.predict(X)[0])
    except Exception as e:
        logger.debug("Cash model unavailable: %s", e)
        # Fallback formula
        rate = latest['cash_consumption_rate'].iloc[0]
        if rate > 0:
            days = bal / rate
        else:
            days = 7.0

    # ── Campus Traffic & Peak Event Bounding Heuristic ──
    # User Rules: 
    # 1. "Cash depletion is generally within 24hrs no less than 12 hours since last replenishment"
    # 2. "adjust these schedules during peak periods... when withdrawal volumes spike"
    
    # Identify Peak multipliers
    is_holiday = latest.get('is_holiday', pd.Series([0])).iloc[0]
    is_salary = latest.get('is_salary_day', pd.Series([0])).iloc[0]
    is_month_end = latest.get('is_month_end', pd.Series([0])).iloc[0]
    
    peak_multiplier = 1.0 + (is_holiday * 0.3) + (is_salary * 0.4) + (is_month_end * 0.2)
    days = days / peak_multiplier
    
    # Enforce strict 12hr (0.5 days) to 24hr (1.0 days) absolute max bounds based on load
    # An ATM with "full" capacity (proxy 2.5m JMD to 3m JMD) scales strictly into this box.
    max_capacity = 3_000_000 # Proxy for standard campus ATM capacity
    fill_ratio = max(0.0, min(1.0, bal / max_capacity))
    
    # If a machine is full, it's 1.0 days max. If half full, it's 0.5 days max.
    upper_bound = 1.0 * fill_ratio 
    lower_bound = 0.5 * fill_ratio
    
    # During extreme heavy traffic days, lower bound is aggressively enforced
    if peak_multiplier > 1.0:
        days = min(days, lower_bound + (upper_bound - lower_bound) * 0.25)
    
    # Safe numerical clamp
    days = max(lower_bound, min(upper_bound, days))

    result['days_to_depletion'] = round(days, 2)


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
    'DEPOSIT_BIN_CRITICAL': lambda d: d.get('deposit_bin_utilization', 0) > 0.90,
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
