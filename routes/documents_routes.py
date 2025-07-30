from flask import Blueprint, request, jsonify
import traceback
import io
from services.excel_parser import parse_portfolio, investments_to_docs

documents_bp = Blueprint("documents", __name__)

@documents_bp.post("/")
def docs_route():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "file missing"}), 400
    try:
        invs = parse_portfolio(io.BytesIO(f.read()), f.filename)
        return jsonify(investments_to_docs(invs))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
