"""
AIAP — Authentication & RBAC Helpers
Credentials sourced from environment variables with safe defaults.
"""
from functools import wraps
from flask import request, jsonify
from flask_jwt_extended import (
    create_access_token, get_jwt_identity, jwt_required
)
from datetime import timedelta
import os


def _load_credentials():
    """Build credential map from environment variables."""
    return {
        os.environ.get('OPS_USERNAME', 'ops.admin'): {
            "password": os.environ.get('OPS_PASSWORD', 'aiap2026'),
            "role": "ops",
        },
        os.environ.get('CUSTOMER_USERNAME', 'customer'): {
            "password": os.environ.get('CUSTOMER_PASSWORD', 'uwiatm'),
            "role": "customer",
        },
    }


def authenticate(username, password):
    """Validate credentials. Returns user dict or None."""
    creds = _load_credentials()
    user = creds.get(username)
    if user and user["password"] == password:
        return {"username": username, "role": user["role"]}
    return None


def create_token(username):
    """Issue a JWT access token valid for 8 hours."""
    creds = _load_credentials()
    user = creds.get(username)
    if not user:
        return None
    return create_access_token(
        identity={"username": username, "role": user["role"]},
        expires_delta=timedelta(hours=8),
    )


def ops_required(fn):
    """Decorator: requires a valid JWT with role == 'ops'."""
    @jwt_required()
    @wraps(fn)
    def wrapper(*args, **kwargs):
        identity = get_jwt_identity()
        if identity.get("role") != "ops":
            return jsonify({"error": "Access denied: Ops role required"}), 403
        return fn(*args, **kwargs)
    return wrapper