from flask import Flask
from routes.kpi_routes import kpi_bp
from routes.documents_routes import documents_bp
from routes.fingerprints import fingerprints_bp

app = Flask(__name__)

# Register Blueprints
app.register_blueprint(kpi_bp, url_prefix="/kpi")
app.register_blueprint(documents_bp, url_prefix="/documents")
app.register_blueprint(fingerprints_bp, url_prefix="/fingerprints")


@app.route("/")
def home():
    return "Quarterly Report API is running!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
