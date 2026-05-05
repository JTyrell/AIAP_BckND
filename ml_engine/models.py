"""
AIAP ML Engine — Model Definitions
Feature groups and model architectures.
"""
import joblib
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor, XGBClassifier


# ── Feature groups ──────────────────────────────────

HEALTH_FEATURES = [
    'uptime_percentage', 'error_count_7d', 'maintenance_count_30d',
    'transaction_velocity', 'days_since_maintenance',
]

CASH_FEATURES = [
    'cash_stress_indicator', 'cash_consumption_rate',
    'days_since_replenishment', 'transaction_count', 'is_month_end',
    'deposit_bin_utilization', 'is_salary_day', 'is_holiday'
]

FAILURE_FEATURES = [
    'error_acceleration', 'consecutive_downtime_mins',
    'health_trend_7d', 'time_since_last_error', 'error_type_diversity',
]

ACTIVITY_FEATURES = [
    'transaction_count', 'peak_hour_transactions',
    'transaction_volatility', 'weekend_vs_weekday_ratio',
]


# ── Model builders ──────────────────────────────────────────────────────────

def build_health_model():
    """Random Forest Regressor for Health Score (0–100)."""
    return Pipeline([
        ('scaler', StandardScaler()),
        ('model', RandomForestRegressor(n_estimators=200, random_state=42)),
    ])


def build_cash_model():
    """LassoCV for days_to_depletion (regression)."""
    return Pipeline([
        ('scaler', RobustScaler()),
        ('model', LassoCV(cv=5, max_iter=10000)),
    ])


def build_failure_model():
    """XGBoost Classifier for binary failure risk P(failure) > 0.5."""
    return Pipeline([
        ('scaler', StandardScaler()),
        ('model', XGBClassifier(
            n_estimators=150,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            objective='binary:logistic',
            eval_metric='logloss',
            random_state=42,
        )),
    ])


def build_activity_model():
    """KMeans for Low / Moderate / High (unsupervised)."""
    from sklearn.cluster import KMeans
    return Pipeline([
        ('scaler', MinMaxScaler()),
        ('model', KMeans(n_clusters=3, random_state=42, n_init=10)),
    ])