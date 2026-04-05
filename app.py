"""
AIAP Backend — Flask Application Entry Point
"""
import os
import logging
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from config import Config
from routes import public, staff, data_ingest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize extensions
    CORS(app, origins=Config.CORS_ORIGINS)
    jwt = JWTManager(app)

    # Register blueprints
    app.register_blueprint(public.bp)
    app.register_blueprint(staff.bp)
    app.register_blueprint(data_ingest.bp)

    # ── Auth route ───────────────────────────────────────────────────────
    @app.route('/api/v1/auth/login', methods=['POST'])
    def login():
        from utils.auth import authenticate, create_token
        data = request.get_json(silent=True) or {}
        username = data.get('username', '')
        password = data.get('password', '')

        user = authenticate(username, password)
        if not user:
            return jsonify({"error": "Invalid credentials"}), 401

        token = create_token(username)
        return jsonify({
            "access_token": token,
            "role": user["role"],
            "username": user["username"],
        }), 200

    # ── Root / API index ────────────────────────────────────────────────
    @app.route('/', methods=['GET'])
    def index():
        return jsonify({
            "name": "AIAP - ATM Intelligence & Availability Platform",
            "version": "1.0.0",
            "status": "running",
            "db_available": Config.DB_AVAILABLE,
            "endpoints": {
                "health": "/health",
                "login": "POST /api/v1/auth/login",
                "public_atms": "GET /api/v1/public/atms",
                "atm_detail": "GET /api/v1/public/atms/<atm_id>",
                "staff_kpis": "GET /api/v1/staff/dashboard/kpis",
                "fleet_health": "GET /api/v1/staff/fleet/health",
                "upload_csv": "POST /api/v1/data/upload/metrics",
            },
        }), 200

    # ── Health check ─────────────────────────────────────────────────────
    @app.route('/health', methods=['GET'])
    def health():
        return jsonify({
            "status": "ok",
            "timestamp": datetime.utcnow().isoformat(),
            "db_available": Config.DB_AVAILABLE,
        }), 200

    logger.info(
        "AIAP backend initialized. DB_AVAILABLE=%s", Config.DB_AVAILABLE,
    )
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)