"""
AIAP ML Engine — Feature Engineering
PDF Section 2.4 & 4.4: all feature creation rules.

Weights (confirmed per PDF):
  Uptime 40%  |  Error 30%  |  Maintenance 20%  |  Cash Stress 10%

Required input columns:
  atm_id, record_date, uptime_percentage, error_count,
  transaction_count, starting_cash_balance, ending_cash_balance

Optional (used if present):
  uptime_minutes, service_date, service_capacity, operational_status
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
    ]
    for col in roll_cols:
        if col in df.columns:
            df[f'{col}_7d'] = df.groupby('atm_id')[col].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )

    # 30-day rolling error mean
    if 'error_count' in df.columns:
        df['error_count_30d'] = df.groupby('atm_id')['error_count'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
    else:
        df['error_count_30d'] = 0

    # ── Derived metrics (PDF Section 2.4) ────────────────────────────────
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

    # Peak hour transactions (24% of daily — JSX HOURLY_WEIGHTS peak)
    df['peak_hour_transactions'] = (df['transaction_count'] * 0.24).round(0)

    # ── Critical derived features ────────────────────────────────────────
    err_7d = df.get('error_count_7d', df['error_count'])
    err_30d = df.get('error_count_30d', df['error_count'])

    df['error_acceleration'] = err_7d / (err_30d + 1)

    # Cash stress indicator: higher = worse (more stressed)
    df['cash_stress_indicator'] = 1 - (
        df['ending_cash_balance'] / df['starting_cash_balance'].clip(lower=1)
    )
    df['cash_stress_indicator'] = df['cash_stress_indicator'].clip(0, 1)

    df['transaction_volatility'] = df.groupby('atm_id')['transaction_count'].transform(
        lambda x: x.rolling(window=7, min_periods=1).std()
    ).fillna(0)

    # ── Composite health (PDF Section 2.4 weights) ───────────────────────
    # Uptime 40%  |  Error 30%  |  Maintenance 20%  |  Cash Stress 10%
    df['uptime_weight'] = df['uptime_percentage'] / 100.0 * 0.40
    df['error_inverse_weight'] = (1 / (err_7d + 1)) * 0.30
    df['maintenance_inverse_weight'] = (
        1 / (df['days_since_maintenance'] + 1)
    ) * 0.20
    df['cash_stress_inverse_weight'] = (
        1 / (df['cash_stress_indicator'] + 1)
    ) * 0.10

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