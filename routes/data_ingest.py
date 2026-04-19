"""
AIAP Routes — Data Ingestion (CSV upload + model retraining)
Uses DataRepository for DB abstraction.
"""
import os
import pandas as pd
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from utils.repository import get_repository
from ml_engine.pipeline import train_all_models

bp = Blueprint('data_ingest', __name__, url_prefix='/api/v1/data')

ALLOWED_EXTENSIONS = {'csv'}

# Map of recognisable dataset types by their required column signatures
DATASET_SIGNATURES = {
    'cash_status': {'atm_id', 'timestamp', 'remaining_cash'},
    'operational_logs': {'atm_id', 'timestamp', 'uptime_status'},
    'transactions': {'atm_id', 'transaction_time', 'withdrawal_amount'},
    'maintenance_records': {'atm_id', 'maintenance_date', 'maintenance_type'},
    'atm_metadata': {'atm_id', 'atm_bank', 'location', 'atm_model'},
}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def detect_dataset_type(columns):
    """Detect dataset type from column names."""
    col_set = set(columns)
    for dtype, sig in DATASET_SIGNATURES.items():
        if sig.issubset(col_set):
            return dtype
    return None


@bp.route('/upload/metrics', methods=['POST'])
def upload_metrics_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if not (file and allowed_file(file.filename)):
        return jsonify({"error": "Invalid file type. CSV only."}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join('data', 'raw', filename)
    os.makedirs('data/raw', exist_ok=True)
    file.save(filepath)

    try:
        df = pd.read_csv(filepath)

        # Auto-detect dataset type from columns
        dataset_type = detect_dataset_type(df.columns)

        if dataset_type is None:
            # Check for legacy aggregated metrics format
            legacy_required = [
                'atm_id', 'record_date', 'uptime_percentage',
                'error_count', 'transaction_count',
                'starting_cash_balance', 'ending_cash_balance',
            ]
            missing = [c for c in legacy_required if c not in df.columns]
            if missing:
                accepted = list(DATASET_SIGNATURES.keys())
                return jsonify({
                    "error": f"Unrecognised CSV schema. Accepted types: {accepted}. "
                             f"Or legacy metrics format with columns: {legacy_required}"
                }), 400
            dataset_type = "legacy_metrics"

        # Save to the canonical filename for the detected type
        if dataset_type != "legacy_metrics":
            canonical_path = os.path.join('data', 'raw', f'{dataset_type}.csv')
            if filepath != canonical_path:
                # Append to existing file if it exists, otherwise create new
                if os.path.exists(canonical_path):
                    existing = pd.read_csv(canonical_path)
                    combined = pd.concat([existing, df], ignore_index=True)
                    combined.drop_duplicates(inplace=True)
                    combined.to_csv(canonical_path, index=False)
                    rows = len(combined) - len(existing)
                else:
                    df.to_csv(canonical_path, index=False)
                    rows = len(df)
            else:
                rows = len(df)
        else:
            rows = len(df)

        # Trigger model retraining
        try:
            result = train_all_models()
            status = "success"
            msg = f"Ingested {dataset_type} data ({rows} new rows) and retrained models."
        except Exception as e:
            status = "partial"
            msg = f"Ingested {dataset_type} data ({rows} new rows) but model training failed: {str(e)}"

        return jsonify({
            "status": status,
            "message": msg,
            "dataset_type": dataset_type,
            "rows_processed": rows,
        }), 201

    except Exception as e:
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500