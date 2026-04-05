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


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


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

        # Validate required columns (per schema)
        required = [
            'atm_id', 'record_date', 'uptime_percentage',
            'error_count', 'transaction_count',
            'starting_cash_balance', 'ending_cash_balance',
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            return jsonify({"error": f"Missing columns: {missing}"}), 400

        # Clean & standardize
        df['record_date'] = pd.to_datetime(df['record_date'])
        df = df.dropna(subset=['atm_id'])

        # Persist via repository (stub logs warning; Postgres inserts)
        repo = get_repository()
        rows = repo.save_metrics(df)

        # Trigger model retraining on the uploaded data
        try:
            result = train_all_models(df=df)
            status = "success"
            msg = "Data ingested and models retrained."
        except Exception as e:
            status = "partial"
            msg = f"Data ingested but model training failed: {str(e)}"

        return jsonify({
            "status": status,
            "message": msg,
            "rows_processed": rows,
        }), 201

    except Exception as e:
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500