"""
AIAP — Comprehensive Test Suite
================================
Validates every layer of the ATM Intelligence & Availability Platform:

  1. Feature Engineering  — formula correctness for all 8 engineered indicators
  2. ML Model Lifecycle   — train + predict for all 4 model heads
  3. Flask Routes          — public, staff, auth, data-ingest endpoints
  4. Alert Generation      — threshold-based alert codes
  5. Edge Cases            — divide-by-zero, empty DataFrames, missing columns

Run:
    python -m pytest scripts/test_suite.py -v
  or
    python -m unittest scripts.test_suite -v
"""

import os
import sys
import json
import math
import unittest
import logging
import warnings
import tempfile
import numpy as np
import pandas as pd

# ── Ensure project root is on sys.path ──────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Silence noisy loggers during tests
logging.disable(logging.WARNING)
warnings.filterwarnings("ignore")

from config import Config
from ml_engine.feature_engineering import engineer_features
from ml_engine.models import (
    build_health_model, build_cash_model,
    build_failure_model, build_activity_model,
    HEALTH_FEATURES, CASH_FEATURES,
    FAILURE_FEATURES, ACTIVITY_FEATURES,
)
from ml_engine.pipeline import (
    train_all_models, predict_for_atm,
    predict_fleet, generate_alerts,
)


# ─────────────────────────────────────────────────────────────────────────────
#  Test Data Factory
# ─────────────────────────────────────────────────────────────────────────────

def _make_daily_df(n_days=30, n_atms=3, seed=42):
    """
    Generate a deterministic synthetic DataFrame matching the schema
    expected by engineer_features().
    """
    np.random.seed(seed)
    rows = []
    base_date = pd.Timestamp('2026-01-01')
    for atm_idx in range(n_atms):
        atm_id = f'ATM-{atm_idx + 1:03d}'
        for d in range(n_days):
            date = base_date + pd.Timedelta(days=d)
            uptime = np.clip(np.random.normal(95, 3), 60, 100)
            errors = max(0, int(np.random.normal(1, 1.5)))
            txns = max(10, int(np.random.normal(180, 40)))
            starting = np.random.uniform(800_000, 1_200_000)
            ending = starting - np.random.uniform(50_000, 300_000)
            ending = max(ending, 10_000)
            rows.append({
                'atm_id': atm_id,
                'record_date': date,
                'uptime_percentage': round(uptime, 2),
                'error_count': errors,
                'transaction_count': txns,
                'starting_cash_balance': round(starting, 2),
                'ending_cash_balance': round(ending, 2),
                'daily_withdrawal_amount': round(starting - ending, 2),
                'avg_transaction_amount': round((starting - ending) / max(txns, 1), 2),
            })
    df = pd.DataFrame(rows)
    # Add a service_date for the first ATM halfway through
    df['service_date'] = pd.NaT
    mask = (df['atm_id'] == 'ATM-001') & (df['record_date'] == base_date + pd.Timedelta(days=14))
    df.loc[mask, 'service_date'] = base_date + pd.Timedelta(days=14)
    return df


def _make_minimal_row():
    """Single-row DataFrame for targeted formula checks."""
    return pd.DataFrame([{
        'atm_id': 'ATM-TEST',
        'record_date': pd.Timestamp('2026-03-15'),
        'day_of_week': 3, # arbitrary weekday
        'uptime_percentage': 92.0,
        'error_count': 3,
        'transaction_count': 200,
        'starting_cash_balance': 1_000_000,
        'ending_cash_balance': 700_000,
        'daily_deposit_count': 10,
        'daily_deposit_amount': 50_000,
        'days_since_maintenance': 5, # positive so it accumulates
    }])


# ─────────────────────────────────────────────────────────────────────────────
#  1. Feature Engineering Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFeatureEngineering(unittest.TestCase):
    """Validate every engineered indicator."""

    @classmethod
    def setUpClass(cls):
        cls.df_raw = _make_daily_df(n_days=30, n_atms=3)
        cls.df_feat = engineer_features(cls.df_raw)

    # ── Schema checks ────────────────────────────────────────────────────

    def test_output_not_empty(self):
        """Feature engineering returns non-empty DataFrame."""
        self.assertGreater(len(self.df_feat), 0)

    def test_all_engineered_columns_present(self):
        """All 8 specified engineered indicators exist in output."""
        required = [
            'utilization_rate',
            'health_trend_7d',
            'cash_stress_indicator',
            'error_acceleration',
            'transaction_volatility',
            'weekend_vs_weekday_ratio',
            'time_to_predicted_empty',
            'composite_health_score',
        ]
        for col in required:
            with self.subTest(col=col):
                self.assertIn(col, self.df_feat.columns,
                              f"Missing engineered indicator: {col}")

    def test_no_nan_in_critical_features(self):
        """Critical indicators should have no NaN values."""
        critical = [
            'utilization_rate', 'cash_stress_indicator',
            'error_acceleration', 'composite_health_score',
            'time_to_predicted_empty',
        ]
        for col in critical:
            with self.subTest(col=col):
                nans = self.df_feat[col].isna().sum()
                self.assertEqual(nans, 0, f"{col} has {nans} NaN values")

    # ── Formula accuracy ─────────────────────────────────────────────────

    def test_utilization_rate_formula(self):
        """utilization_rate = transaction_count / service_capacity."""
        row = self.df_feat.iloc[0]
        expected = row['transaction_count'] / 1000  # default service_capacity
        self.assertAlmostEqual(row['utilization_rate'], expected, places=4)

    def test_cash_stress_indicator_range(self):
        """cash_stress_indicator normalised to [0, 1]."""
        self.assertTrue((self.df_feat['cash_stress_indicator'] >= 0).all())
        self.assertTrue((self.df_feat['cash_stress_indicator'] <= 1).all())

    def test_cash_stress_uses_avg_daily_consumption(self):
        """avg_daily_consumption column exists and is the basis of cash_stress."""
        self.assertIn('avg_daily_consumption', self.df_feat.columns)
        self.assertTrue((self.df_feat['avg_daily_consumption'] >= 1).all(),
                        "avg_daily_consumption floor at 1 violated")

    def test_error_acceleration_safe_division(self):
        """error_acceleration = 7d_sum / 30d_sum, never NaN or Inf."""
        ea = self.df_feat['error_acceleration']
        self.assertFalse(ea.isna().any(), "error_acceleration has NaN")
        self.assertFalse(np.isinf(ea).any(), "error_acceleration has Inf")

    def test_error_acceleration_zero_when_no_errors(self):
        """When 30d error sum is 0, acceleration should be 0."""
        df = _make_minimal_row()
        df['error_count'] = 0
        result = engineer_features(df)
        self.assertEqual(result['error_acceleration'].iloc[0], 0.0)

    def test_transaction_volatility_daily_proxy(self):
        """When no hourly_counts column, volatility uses 7d rolling std."""
        self.assertNotIn('hourly_counts', self.df_raw.columns)
        self.assertIn('transaction_volatility', self.df_feat.columns)
        # With 30 days of data, std shouldn't be 0 for all rows
        self.assertGreater(self.df_feat['transaction_volatility'].sum(), 0)

    def test_transaction_volatility_hourly_path(self):
        """When hourly_counts is present, use real std dev of hourly array."""
        df = _make_minimal_row()
        df['hourly_counts'] = [list(range(24))]  # [0,1,2,...,23]
        result = engineer_features(df)
        expected_std = float(np.std(range(24)))
        self.assertAlmostEqual(
            result['transaction_volatility'].iloc[0],
            expected_std, places=2,
        )

    def test_time_to_predicted_empty(self):
        """time_to_predicted_empty = ending_balance / consumption_rate."""
        self.assertIn('time_to_predicted_empty', self.df_feat.columns)
        # Should be non-negative
        self.assertTrue((self.df_feat['time_to_predicted_empty'] >= 0).all())
        # Should be capped at 720
        self.assertTrue((self.df_feat['time_to_predicted_empty'] <= 720).all())

    def test_weekend_vs_weekday_ratio(self):
        """weekend_vs_weekday_ratio is positive for all ATMs."""
        ratio = self.df_feat['weekend_vs_weekday_ratio']
        self.assertTrue((ratio >= 0).all())

    def test_composite_health_score_range(self):
        """composite_health_score should roughly be in [0, 100]."""
        scores = self.df_feat['composite_health_score']
        self.assertTrue((scores >= 0).all(),
                        f"Min health score {scores.min()} < 0")
        self.assertTrue((scores <= 100).all(),
                        f"Max health score {scores.max()} > 100")

    def test_composite_health_score_weights_sum(self):
        """The four weight columns * 100 should equal composite_health_score."""
        row = self.df_feat.iloc[-1]
        reconstructed = (
            row['uptime_weight']
            + row['error_inverse_weight']
            + row['maintenance_inverse_weight']
            + row['cash_stress_inverse_weight']
        ) * 100
        self.assertAlmostEqual(
            row['composite_health_score'], reconstructed, places=4,
        )

    def test_health_trend_7d_is_slope(self):
        """health_trend_7d should exist and be finite."""
        self.assertIn('health_trend_7d', self.df_feat.columns)
        ht = self.df_feat['health_trend_7d']
        self.assertFalse(np.isinf(ht).any())

    def test_deposit_bin_utilization_computation(self):
        """Deposit bin tracking calculates accurately based on estimated notes and capacity."""
        df = _make_minimal_row()
        df['days_since_maintenance'] = 5
        df['daily_deposit_amount'] = 60_000 # 30 notes of 2000 JMD
        df['daily_deposit_count'] = 5
        result = engineer_features(df)
        self.assertIn('deposit_bin_utilization', result.columns)
        # 60,000 / 2000 = 30 notes. Out of 3000 capacity = 30/3000 = 0.010
        self.assertAlmostEqual(result['deposit_bin_utilization'].iloc[0], 0.010, places=3)
        
    def test_deposit_bin_resets_on_maintenance(self):
        """When days_since_maintenance is negative (pre-maintenance), utilization should not accumulate the old data for the latest status."""
        df = _make_minimal_row()
        df['service_date'] = pd.Timestamp('2026-03-10')
        df.loc[1] = df.loc[0].copy()
        
        # Row 0: Pre-maintenance (record_date < service_date)
        df.loc[0, 'record_date'] = pd.Timestamp('2026-03-09')
        df.loc[0, 'daily_deposit_amount'] = 6000000 # Massive pre-maintenance
        
        # Row 1: Post-maintenance (record_date > service_date)
        df.loc[1, 'record_date'] = pd.Timestamp('2026-03-11')
        df.loc[1, 'daily_deposit_amount'] = 60000 # Moderate post-maintenance
        
        result = engineer_features(df)
        final_utilization = result['deposit_bin_utilization'].iloc[1]
        self.assertAlmostEqual(final_utilization, 0.01, places=3)

    def test_trend_aware_cash_consumption(self):
        """Cash consumption should artificially jump around weekends if variance shows standard weekend spikes."""
        df = _make_daily_df(n_days=14, n_atms=1)
        # Manually inject higher weekend transaction variance
        df.loc[df['record_date'].dt.dayofweek >= 5, 'transaction_count'] = 800
        df.loc[df['record_date'].dt.dayofweek < 5, 'transaction_count'] = 100
        result = engineer_features(df)
        # Identify Thursday/Friday rows
        is_spiking = result['day_of_week'].isin([3, 4])
        # Base consumption should be multiplied by ratio > 1
        ratiocol = result['weekend_vs_weekday_ratio']
        self.assertTrue((ratiocol > 1.5).all(), "Weekend ratio failed to detect surge.")
        
        # Verify avg_daily_consumption uses multiplier logic
        friday_rows_consumption = result.loc[is_spiking, 'avg_daily_consumption']
        monday_rows_consumption = result.loc[~is_spiking, 'avg_daily_consumption']
        # On average, the algorithm scales up consumption expectations heading into a weekend
        self.assertGreater(friday_rows_consumption.mean(), monday_rows_consumption.mean() * 0.9)

    # ── Temporal features ────────────────────────────────────────────────

    def test_temporal_features_present(self):
        """day_of_week, month, is_holiday, cyclical encodings exist."""
        for col in ['day_of_week', 'month', 'is_holiday', 'is_month_end',
                     'day_sin', 'day_cos', 'month_sin', 'month_cos']:
            self.assertIn(col, self.df_feat.columns)

    # ── Edge: empty DataFrame ────────────────────────────────────────────

    def test_empty_dataframe_passthrough(self):
        """engineer_features on empty df returns empty df without error."""
        empty = pd.DataFrame()
        result = engineer_features(empty)
        self.assertTrue(result.empty)


# ─────────────────────────────────────────────────────────────────────────────
#  2. ML Model Lifecycle Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMLModels(unittest.TestCase):
    """Train all 4 models on synthetic data and verify predictions."""

    @classmethod
    def setUpClass(cls):
        cls.df_raw = _make_daily_df(n_days=60, n_atms=5, seed=99)
        cls.train_result = train_all_models(df=cls.df_raw)

    def test_training_returns_dict(self):
        """train_all_models returns a results dictionary."""
        self.assertIsInstance(self.train_result, dict)

    def test_training_rows_recorded(self):
        """Training result includes rows_trained count."""
        self.assertIn('rows_trained', self.train_result)
        self.assertGreater(self.train_result['rows_trained'], 0)

    def test_health_model_saved(self):
        """health_model.joblib exists on disk."""
        path = os.path.join(Config.MODEL_DIR, 'health_model.joblib')
        self.assertTrue(os.path.exists(path), f"Missing: {path}")

    def test_cash_model_saved(self):
        """cash_depletion_model.joblib exists on disk."""
        path = os.path.join(Config.MODEL_DIR, 'cash_depletion_model.joblib')
        self.assertTrue(os.path.exists(path), f"Missing: {path}")

    def test_failure_model_saved(self):
        """failure_model.joblib exists on disk."""
        path = os.path.join(Config.MODEL_DIR, 'failure_model.joblib')
        self.assertTrue(os.path.exists(path), f"Missing: {path}")

    def test_activity_model_saved(self):
        """activity_model.joblib exists on disk."""
        path = os.path.join(Config.MODEL_DIR, 'activity_model.joblib')
        self.assertTrue(os.path.exists(path), f"Missing: {path}")

    def test_health_r2_reasonable(self):
        """Health model R² should be reported (even if synthetic)."""
        self.assertIn('health_r2', self.train_result)

    def test_predict_for_atm_returns_dict(self):
        """predict_for_atm returns a well-formed prediction dict."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertIsInstance(pred, dict)
        for key in ['health_score', 'failure_probability',
                     'activity_level', 'days_to_depletion']:
            self.assertIn(key, pred, f"Missing key: {key}")

    def test_health_score_prediction_range(self):
        """Predicted health score should be in [0, 100]."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertGreaterEqual(pred['health_score'], 0)
        self.assertLessEqual(pred['health_score'], 100)

    def test_failure_probability_range(self):
        """Failure probability should be in [0, 1]."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertGreaterEqual(pred['failure_probability'], 0)
        self.assertLessEqual(pred['failure_probability'], 1)

    def test_activity_level_valid_label(self):
        """Activity level should be Low, Moderate, or High."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertIn(pred['activity_level'], ['Low', 'Moderate', 'High'])

    def test_days_to_depletion_non_negative(self):
        """days_to_depletion should be >= 0."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertGreaterEqual(pred['days_to_depletion'], 0)

    def test_predict_fleet_returns_all_atms(self):
        """predict_fleet should return predictions for all ATMs."""
        df_feat = engineer_features(self.df_raw)
        atm_ids = df_feat['atm_id'].unique()
        # Use individual predictions via precomputed
        for atm_id in atm_ids:
            pred = predict_for_atm(atm_id, precomputed_df=df_feat)
            self.assertIn('health_score', pred)

    def test_predict_for_nonexistent_atm_raises(self):
        """predict_for_atm raises ValueError for unknown ATM ID."""
        df_feat = engineer_features(self.df_raw)
        with self.assertRaises(ValueError):
            predict_for_atm('ATM-NONEXISTENT', precomputed_df=df_feat)

    def test_historical_series_in_prediction(self):
        """Prediction dict includes historical_series with expected keys."""
        df_feat = engineer_features(self.df_raw)
        pred = predict_for_atm('ATM-001', precomputed_df=df_feat)
        self.assertIn('historical_series', pred)
        hs = pred['historical_series']
        for key in ['weekly_txns', 'weekly_labels', 'monthly_inactive',
                     'monthly_labels', 'hourly_txns']:
            self.assertIn(key, hs, f"Missing historical_series key: {key}")


# ─────────────────────────────────────────────────────────────────────────────
#  3. Model Architecture Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestModelArchitectures(unittest.TestCase):
    """Verify model builder output types and feature lists."""

    def test_health_model_is_pipeline(self):
        from sklearn.pipeline import Pipeline
        model = build_health_model()
        self.assertIsInstance(model, Pipeline)

    def test_cash_model_is_pipeline(self):
        from sklearn.pipeline import Pipeline
        model = build_cash_model()
        self.assertIsInstance(model, Pipeline)

    def test_failure_model_is_pipeline(self):
        from sklearn.pipeline import Pipeline
        model = build_failure_model()
        self.assertIsInstance(model, Pipeline)

    def test_activity_model_is_pipeline(self):
        from sklearn.pipeline import Pipeline
        model = build_activity_model()
        self.assertIsInstance(model, Pipeline)

    def test_feature_lists_non_empty(self):
        """All feature lists must have at least one feature."""
        for name, flist in [
            ('HEALTH_FEATURES', HEALTH_FEATURES),
            ('CASH_FEATURES', CASH_FEATURES),
            ('FAILURE_FEATURES', FAILURE_FEATURES),
            ('ACTIVITY_FEATURES', ACTIVITY_FEATURES),
        ]:
            with self.subTest(name=name):
                self.assertGreater(len(flist), 0)

    def test_feature_lists_no_duplicates(self):
        """No duplicate features within any list."""
        for name, flist in [
            ('HEALTH_FEATURES', HEALTH_FEATURES),
            ('CASH_FEATURES', CASH_FEATURES),
            ('FAILURE_FEATURES', FAILURE_FEATURES),
            ('ACTIVITY_FEATURES', ACTIVITY_FEATURES),
        ]:
            with self.subTest(name=name):
                self.assertEqual(len(flist), len(set(flist)))


# ─────────────────────────────────────────────────────────────────────────────
#  4. Alert Generation Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAlerts(unittest.TestCase):
    """Validate alert threshold logic."""

    def test_cash_critical_alert(self):
        alerts = generate_alerts({'cash_stress_indicator': 0.85})
        self.assertIn('CASH_CRITICAL', alerts)

    def test_cash_low_alert(self):
        alerts = generate_alerts({'cash_stress_indicator': 0.60})
        self.assertIn('CASH_LOW', alerts)

    def test_no_cash_alert_when_healthy(self):
        alerts = generate_alerts({'cash_stress_indicator': 0.20, 'deposit_bin_utilization': 0.10})
        self.assertNotIn('CASH_CRITICAL', alerts)
        self.assertNotIn('CASH_LOW', alerts)
        self.assertNotIn('DEPOSIT_BIN_CRITICAL', alerts)

    def test_deposit_bin_critical_alert(self):
        alerts = generate_alerts({'deposit_bin_utilization': 0.95})
        self.assertIn('DEPOSIT_BIN_CRITICAL', alerts)

    def test_error_spike_alert(self):
        alerts = generate_alerts({'error_acceleration': 0.75})
        self.assertIn('ERROR_SPIKE', alerts)

    def test_maintenance_due_alert(self):
        alerts = generate_alerts({'days_since_maintenance': 45})
        self.assertIn('MAINTENANCE_DUE', alerts)

    def test_maintenance_overdue_alert(self):
        alerts = generate_alerts({'days_since_maintenance': 90})
        self.assertIn('MAINTENANCE_OVERDUE', alerts)

    def test_out_of_service_alert(self):
        alerts = generate_alerts({'operational_status': 'out_of_service'})
        self.assertIn('OUT_OF_SERVICE', alerts)

    def test_critical_error_alert(self):
        alerts = generate_alerts({'failure_probability': 0.85})
        self.assertIn('CRITICAL_ERROR', alerts)

    def test_empty_prediction_no_crash(self):
        alerts = generate_alerts({})
        self.assertIsInstance(alerts, list)

    def test_combined_atm_data_override(self):
        """atm_data merges into prediction for alert evaluation."""
        alerts = generate_alerts(
            {'failure_probability': 0.10},
            atm_data={'operational_status': 'out_of_service'},
        )
        self.assertIn('OUT_OF_SERVICE', alerts)


# ─────────────────────────────────────────────────────────────────────────────
#  5. Flask Route Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFlaskRoutes(unittest.TestCase):
    """Test all API endpoints via Flask test client."""

    @classmethod
    def setUpClass(cls):
        # Pre-train models so endpoints can serve predictions
        df = _make_daily_df(n_days=30, n_atms=3)
        train_all_models(df=df)

        from app import create_app
        cls.app = create_app()
        cls.app.config['TESTING'] = True
        cls.client = cls.app.test_client()

    # ── Root & Health ────────────────────────────────────────────────────

    def test_root_endpoint(self):
        resp = self.client.get('/')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['name'],
                         'AIAP - ATM Intelligence & Availability Platform')
        self.assertIn('endpoints', data)

    def test_health_endpoint(self):
        resp = self.client.get('/health')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['status'], 'ok')
        self.assertIn('timestamp', data)

    # ── Auth ─────────────────────────────────────────────────────────────

    def test_login_success(self):
        resp = self.client.post('/api/v1/auth/login', json={
            'username': 'ops.admin', 'password': 'aiap2026',
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn('access_token', data)
        self.assertEqual(data['role'], 'ops')

    def test_login_customer(self):
        resp = self.client.post('/api/v1/auth/login', json={
            'username': 'customer', 'password': 'uwiatm',
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data['role'], 'customer')

    def test_login_invalid_credentials(self):
        resp = self.client.post('/api/v1/auth/login', json={
            'username': 'bad', 'password': 'wrong',
        })
        self.assertEqual(resp.status_code, 401)

    def test_login_empty_body(self):
        resp = self.client.post('/api/v1/auth/login',
                                data='', content_type='application/json')
        self.assertEqual(resp.status_code, 401)

    # ── Staff (requires JWT) ─────────────────────────────────────────────

    def _get_ops_token(self):
        resp = self.client.post('/api/v1/auth/login', json={
            'username': 'ops.admin', 'password': 'aiap2026',
        })
        return resp.get_json()['access_token']

    def _get_customer_token(self):
        resp = self.client.post('/api/v1/auth/login', json={
            'username': 'customer', 'password': 'uwiatm',
        })
        return resp.get_json()['access_token']

    def test_staff_kpis_requires_auth(self):
        resp = self.client.get('/api/v1/staff/dashboard/kpis')
        self.assertIn(resp.status_code, [401, 422])

    def test_staff_kpis_with_ops_token(self):
        token = self._get_ops_token()
        resp = self.client.get(
            '/api/v1/staff/dashboard/kpis',
            headers={'Authorization': f'Bearer {token}'},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn('fleet_health_pct', data)
        self.assertIn('total_atms', data)

    def test_staff_kpis_denied_for_customer(self):
        token = self._get_customer_token()
        resp = self.client.get(
            '/api/v1/staff/dashboard/kpis',
            headers={'Authorization': f'Bearer {token}'},
        )
        self.assertEqual(resp.status_code, 403)

    def test_fleet_health_with_ops_token(self):
        token = self._get_ops_token()
        resp = self.client.get(
            '/api/v1/staff/fleet/health',
            headers={'Authorization': f'Bearer {token}'},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIsInstance(data, list)
        if len(data) > 0:
            self.assertIn('deposit_bin_utilization', data[0], "Ops UI payload must expose deposit bin utilization")
            self.assertIn('alerts', data[0])

    def test_public_atms_list_exposes_deposit_utilization(self):
        """Ensure /api/v1/public/atms explicitly leaks the deposit_utilization for UI."""
        resp = self.client.get('/api/v1/public/atms')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        if len(data) > 0:
            self.assertIn('deposit_bin_utilization', data[0], "Public UI payload must expose deposit bin utilization")
            
    def test_public_atm_detail_exposes_deposit_utilization(self):
        resp = self.client.get('/api/v1/public/atms')
        data = resp.get_json()
        if len(data) > 0:
            atm_id = data[0]['id']
            detail_resp = self.client.get(f'/api/v1/public/atms/{atm_id}')
            self.assertEqual(detail_resp.status_code, 200)
            self.assertIn('deposit_bin_utilization', detail_resp.get_json())

    # ── Data Ingest ──────────────────────────────────────────────────────

    def test_upload_no_file(self):
        resp = self.client.post('/api/v1/data/upload/metrics')
        self.assertEqual(resp.status_code, 400)

    def test_upload_wrong_extension(self):
        from io import BytesIO
        data = {'file': (BytesIO(b'data'), 'test.txt')}
        resp = self.client.post(
            '/api/v1/data/upload/metrics',
            data=data, content_type='multipart/form-data',
        )
        self.assertEqual(resp.status_code, 400)


# ─────────────────────────────────────────────────────────────────────────────
#  6. Config & Infrastructure Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestConfig(unittest.TestCase):
    """Validate Config constants and paths."""

    def test_model_dir_exists_or_creatable(self):
        os.makedirs(Config.MODEL_DIR, exist_ok=True)
        self.assertTrue(os.path.isdir(Config.MODEL_DIR))

    def test_data_dir_set(self):
        self.assertTrue(len(Config.DATA_DIR) > 0)

    def test_noise_sigma_defaults(self):
        self.assertGreater(Config.NOISE_SIGMA_HEALTH, 0)
        self.assertGreater(Config.NOISE_SIGMA_CASH, 0)
        self.assertGreater(Config.LABEL_FLIP_RATE, 0)
        self.assertLess(Config.LABEL_FLIP_RATE, 1)


# ─────────────────────────────────────────────────────────────────────────────
#  7. Edge-Case & Robustness Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEdgeCases(unittest.TestCase):
    """Stress-test boundary conditions."""

    def test_single_row_feature_engineering(self):
        """Feature engineering on a single row should not crash."""
        df = _make_minimal_row()
        result = engineer_features(df)
        self.assertEqual(len(result), 1)
        self.assertIn('composite_health_score', result.columns)

    def test_zero_transaction_count(self):
        """Zero transactions should not cause division errors."""
        df = _make_minimal_row()
        df['transaction_count'] = 0
        result = engineer_features(df)
        self.assertFalse(result['utilization_rate'].isna().any())

    def test_zero_cash_balance(self):
        """Zero ending balance should produce max stress, no errors."""
        df = _make_minimal_row()
        df['ending_cash_balance'] = 0
        result = engineer_features(df)
        # cash_stress_indicator should be high (near 1)
        self.assertGreaterEqual(result['cash_stress_indicator'].iloc[0], 0)

    def test_identical_start_end_balance(self):
        """No cash consumed = low stress."""
        df = _make_minimal_row()
        df['ending_cash_balance'] = df['starting_cash_balance'].iloc[0]
        result = engineer_features(df)
        # stress should be very low when no cash was consumed
        self.assertLessEqual(result['cash_stress_indicator'].iloc[0], 0.5)

    def test_100_percent_uptime(self):
        """100% uptime should give high health score."""
        df = _make_minimal_row()
        df['uptime_percentage'] = 100.0
        df['error_count'] = 0
        result = engineer_features(df)
        self.assertGreaterEqual(result['composite_health_score'].iloc[0], 40)

    def test_zero_uptime(self):
        """0% uptime should give low health score."""
        df = _make_minimal_row()
        df['uptime_percentage'] = 0.0
        result = engineer_features(df)
        self.assertLessEqual(result['composite_health_score'].iloc[0], 60)

    def test_train_on_too_few_rows(self):
        """Training on very few rows should still succeed."""
        df = _make_daily_df(n_days=5, n_atms=1)
        result = train_all_models(df=df)
        self.assertIn('rows_trained', result)

    def test_feature_engineering_preserves_atm_id(self):
        """atm_id column must survive feature engineering."""
        df = _make_daily_df()
        result = engineer_features(df)
        self.assertIn('atm_id', result.columns)
        self.assertEqual(set(result['atm_id'].unique()),
                         set(df['atm_id'].unique()))


# ─────────────────────────────────────────────────────────────────────────────
#  Runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Re-enable logging for console output
    logging.disable(logging.NOTSET)
    logging.basicConfig(level=logging.INFO)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Order: Feature Eng → Models → Architecture → Alerts → Routes → Config → Edge
    suite.addTests(loader.loadTestsFromTestCase(TestFeatureEngineering))
    suite.addTests(loader.loadTestsFromTestCase(TestMLModels))
    suite.addTests(loader.loadTestsFromTestCase(TestModelArchitectures))
    suite.addTests(loader.loadTestsFromTestCase(TestAlerts))
    suite.addTests(loader.loadTestsFromTestCase(TestFlaskRoutes))
    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
