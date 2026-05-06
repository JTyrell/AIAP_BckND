"""
AIAP ML Engine — Feature Engineering

Composite Health Score Weights:
  Uptime 40%  |  Error Inverse 30%  |  Maintenance Inverse 20%  |  Cash Stress Inverse 10%

Engineered Indicators:
  utilization_rate          = transaction_count / service_capacity
  health_trend_7d           = Slope of uptime % over 7 days
  cash_stress_indicator     = ending_balance / avg_daily_consumption
  error_acceleration        = error_count_7d / error_count_30d
  transaction_volatility    = Std dev of hourly counts (or daily proxy)
  weekend_vs_weekday_ratio  = Weekend avg / Weekday avg
  time_to_predicted_empty   = ending_balance / consumption_rate
  composite_health_score    = Weighted combination (40/30/20/10)

Required input columns:
  atm_id, record_date, uptime_percentage, error_count,
  transaction_count, starting_cash_balance, ending_cash_balance

Optional (used if present):
  uptime_minutes, service_date, service_capacity, operational_status,
  hourly_counts (list/array — enables true hourly volatility)
"""
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Configurable Jamaica public holidays (extend per deployment period)
HOLIDAYS = [
    # 2025
    '2025-01-01', '2025-02-19', '2025-03-12', '2025-04-18', '2025-04-21',
    '2025-05-23', '2025-06-09', '2025-08-01', '2025-08-06', '2025-10-20',
    '2025-12-25', '2025-12-26',
    # 2026
    '2026-01-01', '2026-02-18', '2026-03-11', '2026-04-03', '2026-04-06',
    '2026-05-25', '2026-06-08', '2026-08-01', '2026-08-06', '2026-10-19',
    '2026-12-25', '2026-12-26',
    # 2027
    '2027-01-01', '2027-02-17', '2027-03-10', '2027-03-26', '2027-03-29',
    '2027-05-24', '2027-06-07', '2027-08-02', '2027-08-06', '2027-10-18',
    '2027-12-25', '2027-12-27',
]


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all feature engineering rules from AIAP_Feature_Selection_Guide.pdf.
    Input:  df with ATM_DAILY_METRICS + MAINTENANCE_LOGS columns (joined on atm_id).
    Output: enriched DataFrame with engineered indicators.
    """
    if df.empty:
        logger.warning("engineer_features called with empty DataFrame")
        return df

    df = df.copy()

    # Ensure record_date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['record_date']):
        df['record_date'] = pd.to_datetime(df['record_date'])

    # ── Temporal features ────────────────────────────────────────────────
    df['day_of_week'] = df['record_date'].dt.dayofweek
    df['month'] = df['record_date'].dt.month

    holiday_dates = pd.to_datetime(HOLIDAYS)
    df['is_holiday'] = (
        df['record_date'].dt.normalize().isin(holiday_dates).astype(int)
    )
    df['is_month_end'] = df['record_date'].dt.is_month_end.astype(int)
    
    # Salary days (typically 25th to 27th in standard regional cycles)
    df['is_salary_day'] = df['record_date'].dt.day.isin([25, 26, 27]).astype(int)

    # Cyclical encoding
    df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

    # ── Sort for rolling windows ─────────────────────────────────────────
    df = df.sort_values(['atm_id', 'record_date']).reset_index(drop=True)

    # ── 7-day rolling stats ──────────────────────────────────────────────
    roll_cols = [
        'uptime_percentage', 'error_count',
        'transaction_count', 'ending_cash_balance',
        'daily_withdrawal_amount', 'avg_transaction_amount'
    ]
    for col in roll_cols:
        if col in df.columns:
            df[f'{col}_7d'] = df.groupby('atm_id')[col].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )

    # 30-day rolling error sum (for error_acceleration ratio)
    if 'error_count' in df.columns:
        df['error_count_30d'] = df.groupby('atm_id')['error_count'].transform(
            lambda x: x.rolling(window=30, min_periods=1).sum()
        )
    else:
        df['error_count_30d'] = 0

    # 7-day rolling error sum (for error_acceleration numerator)
    if 'error_count' in df.columns:
        df['error_count_7d_sum'] = df.groupby('atm_id')['error_count'].transform(
            lambda x: x.rolling(window=7, min_periods=1).sum()
        )
    else:
        df['error_count_7d_sum'] = 0

    # ── Derived metrics (PDF Section 2.4) ────────────────────────────────
    # utilization_rate = transaction_count / service_capacity
    svc_cap = df['service_capacity'] if 'service_capacity' in df.columns else 1000
    df['utilization_rate'] = df['transaction_count'] / svc_cap

    # Uptime hours for consumption calc
    if 'uptime_minutes' in df.columns:
        uptime_h = df['uptime_minutes'] / 60 + 1
    else:
        uptime_h = df['uptime_percentage'] / 100.0 * 24 + 1

    df['cash_consumption_rate'] = (
        (df['starting_cash_balance'] - df['ending_cash_balance']) / uptime_h
    )

    # Base avg_daily_consumption: 7-day rolling mean of daily cash burn
    df['_daily_burn'] = (
        df['starting_cash_balance'] - df['ending_cash_balance']
    ).clip(lower=0)
    df['base_daily_consumption'] = df.groupby('atm_id')['_daily_burn'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    ).clip(lower=1)
    df.drop(columns=['_daily_burn'], inplace=True)


    # Days since replenishment (proxy: days since starting_cash increased)
    df['_cash_up'] = df.groupby('atm_id')['starting_cash_balance'].transform(
        lambda x: x.diff().gt(0).cumsum()
    )
    df['days_since_replenishment'] = df.groupby(
        ['atm_id', '_cash_up']
    ).cumcount()
    df.drop(columns=['_cash_up'], inplace=True)

    # ── Days since maintenance (correct per-row calculation) ─────────────
    if 'service_date' in df.columns:
        df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

        dsm_parts = []
        for _, group in df.groupby('atm_id'):
            group = group.sort_values('record_date')
            last_svc = pd.NaT
            vals = []
            for _, row in group.iterrows():
                if pd.notna(row.get('service_date')):
                    last_svc = row['service_date']
                if pd.notna(last_svc):
                    vals.append((row['record_date'] - last_svc).days)
                else:
                    vals.append(999)
            dsm_parts.append(
                pd.Series(vals, index=group.index, dtype=float)
            )
        df['days_since_maintenance'] = pd.concat(dsm_parts)
    else:
        df['days_since_maintenance'] = 999

    # Maintenance count in last 30 days
    if 'service_date' in df.columns:
        df['_has_maint'] = df['service_date'].notna().astype(int)
        df['maintenance_count_30d'] = df.groupby('atm_id')['_has_maint'].transform(
            lambda x: x.rolling(window=30, min_periods=1).sum()
        )
        df.drop(columns=['_has_maint'], inplace=True)
    else:
        df['maintenance_count_30d'] = 0

    # Transaction velocity (txns per hour of uptime)
    df['transaction_velocity'] = (
        df['transaction_count'] / (df['uptime_percentage'] / 100.0 * 24 + 0.01)
    )

    # Consecutive downtime minutes (estimated from uptime)
    df['consecutive_downtime_mins'] = (
        (100 - df['uptime_percentage']).clip(lower=0) / 100.0 * 1440
    )

    # Health trend 7d (slope of uptime over last 7 days)
    def _slope(vals):
        if len(vals) < 2:
            return 0
        return np.polyfit(range(len(vals)), vals, 1)[0]

    df['health_trend_7d'] = df.groupby('atm_id')['uptime_percentage'].transform(
        lambda x: x.rolling(window=7, min_periods=2).apply(_slope, raw=True)
    ).fillna(0)

    # Time since last error (days since error_count > 0)
    df['_had_error'] = (df['error_count'] > 0).astype(int)
    df['_no_error_run'] = df.groupby('atm_id')['_had_error'].transform(
        lambda x: x.eq(0).cumsum()
    )
    df['time_since_last_error'] = df.groupby(
        ['atm_id', '_no_error_run']
    ).cumcount()
    df.drop(columns=['_had_error', '_no_error_run'], inplace=True)

    # Error type diversity (proxy: std of error counts over 7d window)
    df['error_type_diversity'] = df.groupby('atm_id')['error_count'].transform(
        lambda x: x.rolling(window=7, min_periods=1).std()
    ).fillna(0)

    # ── Weekend-vs-weekday ratio ─────────────────────────────────────────
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

    ww_ratios = {}
    for aid, grp in df.groupby('atm_id'):
        wkend = grp.loc[grp['day_of_week'] >= 5, 'transaction_count']
        wkday = grp.loc[grp['day_of_week'] < 5, 'transaction_count']
        we_avg = wkend.mean() if len(wkend) > 0 else 0
        wd_avg = wkday.mean() if len(wkday) > 0 else 1
        ww_ratios[aid] = we_avg / (wd_avg + 1)
    df['weekend_vs_weekday_ratio'] = df['atm_id'].map(ww_ratios).fillna(1.0)

    # Apply Trend-Aware Depletion Multiplier to avg_daily_consumption
    # If heading into weekend (Thu/Fri), apply the weekend ratio multiplier.
    is_heading_to_weekend = df['day_of_week'].isin([3, 4]).astype(float)
    multiplier = 1.0 + (is_heading_to_weekend * (df['weekend_vs_weekday_ratio'] - 1.0).clip(lower=0))
    df['avg_daily_consumption'] = df['base_daily_consumption'] * multiplier 

    # ── Deposit Bin Utilization ──────────────────────────────────────────
    # Estimate deposited notes: Limit 30 notes/txn, assume avg 2000 JMD/note
    if 'daily_deposit_amount' not in df.columns:
        df['daily_deposit_amount'] = 0.0
        df['daily_deposit_count'] = 0

    df['estimated_deposit_notes'] = (df['daily_deposit_amount'] / 2000).fillna(0)
    # Clip by theoretical maximum (30 notes per transaction)
    df['estimated_deposit_notes'] = np.minimum(
        df['estimated_deposit_notes'], 
        df['daily_deposit_count'].fillna(0) * 30
    )

    # Accumulate since last maintenance
    # days_since_maintenance >= 0 means this record is post-maintenance.
    df['is_post_maintenance'] = df['days_since_maintenance'] >= 0
    df['post_maint_deposits'] = np.where(df['is_post_maintenance'], df['estimated_deposit_notes'], 0)
    df['cumulative_deposit_notes'] = df.groupby('atm_id')['post_maint_deposits'].cumsum()

    # Hardware Capacity Default (3000 Notes Max per safe bin)
    capacity_notes = 3000
    df['deposit_bin_utilization'] = (df['cumulative_deposit_notes'] / capacity_notes).clip(0, 1.0)
    df.drop(columns=['is_post_maintenance', 'post_maint_deposits', 'base_daily_consumption'], inplace=True, errors='ignore')


    # Peak hour transactions (24% of daily — JSX HOURLY_WEIGHTS peak)
    df['peak_hour_transactions'] = (df['transaction_count'] * 0.24).round(0)

    # ── Critical derived features ────────────────────────────────────────

    # error_acceleration = error_count_7d / error_count_30d  (safe division)
    err_7d_sum = df['error_count_7d_sum']
    err_30d_sum = df['error_count_30d']
    df['error_acceleration'] = np.where(
        err_30d_sum > 0,
        err_7d_sum / err_30d_sum,
        0.0,
    )

    # cash_stress_indicator = ending_balance / avg_daily_consumption
    # Liquidity risk metric: LOWER value = MORE stressed (fewer days of cash)
    df['cash_stress_indicator'] = (
        df['ending_cash_balance'] / df['avg_daily_consumption']
    ).clip(0, 60)  # cap at 60 days
    # Normalise to 0-1 scale (0 = healthy, 1 = critical) for downstream use
    df['cash_stress_indicator_raw'] = df['cash_stress_indicator']
    df['cash_stress_indicator'] = (1 - df['cash_stress_indicator'] / 60).clip(0, 1)

    # transaction_volatility: dual-path
    #   Path A: If hourly_counts column is present (list of 24 values), use real std dev
    #   Path B: Otherwise, proxy from 7-day rolling std of daily transaction_count
    if 'hourly_counts' in df.columns:
        def _hourly_std(val):
            """Compute std dev from an hourly counts array."""
            if isinstance(val, (list, np.ndarray)) and len(val) > 0:
                return float(np.std(val))
            return 0.0
        df['transaction_volatility'] = df['hourly_counts'].apply(_hourly_std)
    else:
        df['transaction_volatility'] = df.groupby('atm_id')['transaction_count'].transform(
            lambda x: x.rolling(window=7, min_periods=1).std()
        ).fillna(0)

    # time_to_predicted_empty = ending_balance / consumption_rate
    consumption_rate = df['cash_consumption_rate'].clip(lower=0.01)
    df['time_to_predicted_empty'] = (
        df['ending_cash_balance'] / consumption_rate
    ).clip(0, 720)  # cap at 720 hours (30 days)

    # ── Composite health (PDF Section 2.4 weights) ───────────────────────
    # Uptime 40%  |  Error Inverse 30%  |  Maintenance Inverse 20%  |  Cash Stress Inverse 10%
    uptime_norm = df['uptime_percentage'] / 100.0                   # 0-1
    error_inv   = 1 / (df['error_count_7d_sum'] + 1)                # 0-1 (fewer errors → higher)
    maint_inv   = 1 / (df['days_since_maintenance'].clip(lower=0) + 1)  # 0-1 (recent maint → higher)
    cash_inv    = 1 - df['cash_stress_indicator']                   # 0-1 (lower stress → higher)

    df['uptime_weight']                = uptime_norm * 0.40
    df['error_inverse_weight']         = error_inv   * 0.30
    df['maintenance_inverse_weight']   = maint_inv   * 0.20
    df['cash_stress_inverse_weight']   = cash_inv    * 0.10

    df['composite_health_score'] = (
        df['uptime_weight']
        + df['error_inverse_weight']
        + df['maintenance_inverse_weight']
        + df['cash_stress_inverse_weight']
    ) * 100  # Scale to 0–100

    # Activity classification baseline (unsupervised clustering input)
    df['activity_level_raw'] = df['transaction_count']

    # ── Failure risk proxy (PDF Section 2.4) ─────────────────────────────
    df['failure_risk_score'] = (
        df['error_acceleration'] * 0.30
        + (1 / (df['consecutive_downtime_mins'] + 1)) * 0.25
        + (-df['health_trend_7d'].fillna(0)) * 0.20
        + (1 / (df['time_since_last_error'] + 1)) * 0.15
        + (1 / (df['error_type_diversity'] + 1)) * 0.10
    )

    n_rows = len(df)
    n_cols = len(df.columns)
    logger.info(f"Feature engineering complete: {n_rows} rows, {n_cols} cols")
    return df

def engineer_short_term_features(logs: pd.DataFrame, cash: pd.DataFrame, atm: pd.DataFrame) -> pd.DataFrame:
    """
    Generate features and 1-hour prediction target for short-term customer-facing predictions,
    incorporating logic from data_cleaning.py.
    """
    if logs.empty or cash.empty:
        return pd.DataFrame()
        
    logs = logs.drop_duplicates().sort_values(["atm_id", "timestamp"]).reset_index(drop=True)
    df = logs.merge(atm, on="atm_id", how="left")
    
    df = df.sort_values(["timestamp", "atm_id"]).reset_index(drop=True)
    cash = cash.sort_values(["timestamp", "atm_id"]).reset_index(drop=True)
    
    df = pd.merge_asof(
        df,
        cash,
        on="timestamp",
        by="atm_id",
        direction="backward"
    )
    
    df = df.sort_values(["atm_id", "timestamp"]).reset_index(drop=True)
    
    prediction_window = pd.Timedelta(hours=1)
    df["future_failure"] = 0
    
    # Very slow in python, but follows original data_cleaning.py logic exactly
    for atm_id, group in df.groupby("atm_id"):
        group = group.sort_values("timestamp")
        for i in group.index:
            current_time = df.loc[i, "timestamp"]
            future_rows = group[
                (group["timestamp"] > current_time)
                & (group["timestamp"] <= current_time + prediction_window)
            ]
            if (future_rows["uptime_status"] == 0).any():
                df.loc[i, "future_failure"] = 1
                
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    
    if "remaining_cash" not in df.columns:
        df["remaining_cash"] = 0
        
    df["low_cash_flag"] = (df["remaining_cash"] < 200000).astype(int)
    df["cash_change"] = df.groupby("atm_id")["remaining_cash"].diff().fillna(0)
    
    df["last_failure_time"] = df["timestamp"].where(df["uptime_status"] == 0)
    df["last_failure_time"] = df.groupby("atm_id")["last_failure_time"].ffill()
    
    df["time_since_last_failure"] = (
        df["timestamp"] - df["last_failure_time"]
    ).dt.total_seconds().fillna(999999999)
    
    df["past_failure"] = df.groupby("atm_id")["uptime_status"].shift(1).fillna(1)
    df["past_failure_flag"] = (df["past_failure"] == 0).astype(int)
    df["recent_failures"] = (
        df.groupby("atm_id")["past_failure_flag"]
        .rolling(window=5)
        .sum()
        .reset_index(level=0, drop=True)
        .fillna(0)
    )
    
    return df


def select_top_features(df: pd.DataFrame, target_col: str, k: int = 5) -> list:
    """
    Select top K features using Fisher Score (SelectKBest with f_classif)
    """
    from sklearn.feature_selection import SelectKBest, f_classif
    
    # Only use numeric columns that aren't ID or timestamp
    exclude_cols = ['atm_id', 'timestamp', 'record_date', 'atm_bank', 'location', 'atm_model', 'last_failure_time', target_col]
    features = [c for c in df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]
    
    if not features:
        return []
        
    X = df[features].fillna(0)
    y = df[target_col]
    
    # Ensure there are at least two classes to avoid errors
    if len(y.unique()) <= 1:
        logger.warning(f"select_top_features: Target {target_col} has only one class.")
        return features[:k]
        
    selector = SelectKBest(score_func=f_classif, k=min(k, len(features)))
    try:
        selector.fit(X, y)
        selected_indices = selector.get_support(indices=True)
        selected_features = [features[i] for i in selected_indices]
        return selected_features
    except Exception as e:
        logger.error(f"Error in Fisher Score selection: {e}")
        return features[:k]