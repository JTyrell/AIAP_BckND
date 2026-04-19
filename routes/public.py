"""
AIAP Routes — Public (Customer-Facing) Endpoints
Serves ATM finder & detail for the CustomerView component.
"""
from flask import Blueprint, request, jsonify
from utils.repository import get_repository
from ml_engine.pipeline import predict_for_atm, generate_alerts
import pandas as pd
from math import radians, cos, sin, sqrt, atan2

bp = Blueprint('public', __name__, url_prefix='/api/v1/public')

R_EARTH = 6371.0  # km


def _haversine(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R_EARTH * 2 * atan2(sqrt(a), sqrt(1 - a))


@bp.route('/atms', methods=['GET'])
def get_atms():
    lat = request.args.get('lat', type=float)
    lng = request.args.get('lng', type=float)
    radius_km = request.args.get('radius', default=2.0, type=float)
    service = request.args.get('service')

    repo = get_repository()
    snapshot = repo.get_fleet_snapshot()

    if snapshot.empty:
        return jsonify([])

    # Service filter
    if service and 'services' in snapshot.columns:
        snapshot = snapshot[
            snapshot['services'].str.contains(service, na=False)
        ]

    # Haversine filter
    if lat is not None and lng is not None and 'lat' in snapshot.columns:
        snapshot['distance_km'] = snapshot.apply(
            lambda r: _haversine(lat, lng, r['lat'], r['lng']), axis=1,
        )
        snapshot = snapshot[snapshot['distance_km'] <= radius_km]

    results = []
    for _, row in snapshot.iterrows():
        atm_id = row['atm_id']
        try:
            pred = predict_for_atm(atm_id, repository=repo)
        except Exception:
            pred = {
                "health_score": 75.0,
                "failure_probability": 0.05,
                "activity_level": "Moderate",
                "days_to_depletion": 7.0,
                "cash_stress_indicator": 0.3,
                "error_acceleration": 0.1,
                "uptime_percentage": float(
                    row.get('uptime_percentage', 95.0)
                ),
            }

        # cash_level = % remaining (higher = better, matching frontend)
        cash_level = round((1 - pred.get('cash_stress_indicator', 0.3)) * 100, 0)

        results.append({
            "id": str(row.get('atm_id', '')).strip(),
            "name": str(row.get('location', '')).strip(),
            "short": str(row.get('atm_bank', '')).strip(),
            "building": str(row.get('location', '')).strip(),
            "status": row.get('operational_status', 'in_service'),
            "health": pred["health_score"],
            "activity_level": pred["activity_level"],
            "cash_level": cash_level,
            "days_to_depletion": pred["days_to_depletion"],
            "transactions_24h": int(row.get('transaction_count', 0)),
            "services": row.get('services', ''),
            "card_types": row.get('card_types', ''),
            "lat": float(row['lat']) if 'lat' in row else None,
            "lng": float(row['lng']) if 'lng' in row else None,
            "distance_km": (
                round(row.get('distance_km', 0), 2)
                if lat and lng else None
            ),
        })

    return jsonify(results)


@bp.route('/atms/<atm_id>', methods=['GET'])
def get_atm_detail(atm_id):
    repo = get_repository()

    try:
        pred = predict_for_atm(atm_id, repository=repo)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    master_df = repo.get_atm_master(atm_id=atm_id)
    if master_df.empty:
        return jsonify({"error": "ATM not found"}), 404
    master = master_df.iloc[0]

    alerts = generate_alerts(pred)

    return jsonify({
        "id": atm_id,
        "name": master.get('location', ''),
        "building": master.get('location', ''),
        "street": master.get('location', ''),
        "services": master.get('services', ''),
        "card_types": master.get('card_types', ''),
        "status": pred.get('operational_status', 'in_service'),
        "health_score": pred["health_score"],
        "failure_probability": pred["failure_probability"],
        "activity_level": pred["activity_level"],
        "cash_level": round(
            (1 - pred.get('cash_stress_indicator', 0.3)) * 100, 0
        ),
        "days_to_depletion": pred["days_to_depletion"],
        "uptime_percentage": pred["uptime_percentage"],
        "error_acceleration": pred["error_acceleration"],
        "alerts": alerts,
    })