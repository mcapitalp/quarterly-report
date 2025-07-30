from flask import Blueprint, request, jsonify
import traceback
from services.kpi_extractor import extract_all_kpis_from_bytes

kpi_bp = Blueprint("kpi", __name__)

@kpi_bp.post("/status")
def kpi_status_route():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "file missing"}), 400
    try:
        content = f.read()
        rows = extract_all_kpis_from_bytes(content, f.filename)
        return jsonify(rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@kpi_bp.post("/extract")
def kpi_extract_route():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "file missing"}), 400
    try:
        content = f.read()
        kpi_data = extract_all_kpis_from_bytes(content, f.filename)
        return jsonify(kpi_data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
