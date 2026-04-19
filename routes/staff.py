"""
AIAP Routes — Staff / Ops Dashboard Endpoints
Serves fleet-wide KPIs and per-ATM health data.
"""
from flask import Blueprint, jsonify
from utils.auth import ops_required
from utils.repository import get_repository
from ml_engine.pipeline import predict_for_atm, generate_alerts
import pandas as pd
import logging

logger = logging.getLogger(__name__)

bp = Blueprint('staff', __name__, url_prefix='/api/v1/staff')


@bp.route('/dashboard/kpis', methods=['GET'])
@ops_required
def get_kpis():
    """Fleet-wide aggregate KPIs for the ops overview tab."""
    repo = get_repository()
    snapshot = repo.get_fleet_snapshot()

    if snapshot.empty:
        return jsonify({
            "fleet_health_pct": 0,
            "active_atms": 0,
            "total_atms": 0,
            "low_cash_alerts": 0,
            "high_risk_atms": 0,
            "offline_count": 0,
        })

    total = len(snapshot)
    active = len(
        snapshot[snapshot.get('operational_status', pd.Series()) == 'in_service']
    ) if 'operational_status' in snapshot.columns else total

    # Get predictions for each ATM
    healths = []
    low_cash = 0
    high_risk = 0

    for _, row in snapshot.iterrows():
        try:
            pred = predict_for_atm(row['atm_id'], repository=repo)
            healths.append(pred['health_score'])
            if pred.get('cash_stress_indicator', 0) > 0.70:
                low_cash += 1
            if pred.get('failure_probability', 0) > 0.50:
                high_risk += 1
        except Exception as e:
            logger.debug("KPI prediction failed for %s: %s", row['atm_id'], e)
            healths.append(75.0)

    avg_health = round(sum(healths) / len(healths), 1) if healths else 0

    return jsonify({
        "fleet_health_pct": avg_health,
        "active_atms": active,
        "total_atms": total,
        "low_cash_alerts": low_cash,
        "high_risk_atms": high_risk,
        "offline_count": total - active,
    })


@bp.route('/fleet/health', methods=['GET'])
@ops_required
def get_fleet_health():
    """Per-ATM health detail for the fleet table."""
    repo = get_repository()
    snapshot = repo.get_fleet_snapshot()

    if snapshot.empty:
        return jsonify([])

    results = []
    for _, row in snapshot.iterrows():
        atm_id = row['atm_id']
        try:
            pred = predict_for_atm(atm_id, repository=repo)
        except Exception:
            pred = {
                'health_score': 75.0,
                'failure_probability': 0.05,
                'days_to_depletion': 7.0,
                'activity_level': 'Moderate',
                'error_acceleration': 0.1,
                'cash_stress_indicator': 0.3,
            }

        cash_level = round(
            (1 - pred.get('cash_stress_indicator', 0.3)) * 100, 1,
        )

        results.append({
            "id": atm_id,
            "bank_name": row.get('atm_bank', ''),
            "location": row.get('location', ''),
            "manufacturer": row.get('atm_bank', ''),
            "model": row.get('atm_model', ''),
            "status": row.get('operational_status', 'in_service'),
            "health_score": pred['health_score'],
            "failure_probability": pred['failure_probability'],
            "days_to_depletion": pred['days_to_depletion'],
            "activity_level": pred['activity_level'],
            "uptime": float(row.get('uptime_percentage', 95.0)),
            "error_count": int(row.get('error_count', 0)),
            "transactions_24h": int(row.get('transaction_count', 0)),
            "cash_level": cash_level,
            "error_acceleration": pred.get('error_acceleration', 0.1),
            "alerts": generate_alerts(pred),
        })

    # Sort by health score ascending (worst first)
    results.sort(key=lambda x: x['health_score'])

    return jsonify(results)