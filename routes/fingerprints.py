# app/routes/fingerprints.py
import traceback
from flask import Blueprint, request, jsonify
from services.fingerprint_extractor import extract_fingerprints_from_bytes

fingerprints_bp = Blueprint("fingerprints", __name__)

@fingerprints_bp.post("/")
def fingerprints_route():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "file missing"}), 400
    try:
        items = extract_fingerprints_from_bytes(f.read(), f.filename)
        # items already excludes empty names and empty sections
        return jsonify(items)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
