import os
import uuid
import re
import duckdb
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS

# ────────────── Flask Setup ──────────────
app = Flask(__name__)
app.url_map.strict_slashes = False
app.config["JSON_AS_ASCII"] = False
app.config["MAX_CONTENT_LENGTH"] = 1000 * 1024 * 1024  # 1000 MB

CORS(app)

# ────────────── Constants ──────────────
DUCKDB_PATH = "/srv/data/grela_v0-2.duckdb"
PARQUET_DIR = "/srv/webserver/data/grela-api-out"
PUBLIC_URL_PREFIX = "https://ccs-lab.zcu.cz/grela-api-out"
MAX_ROWS = 500_000

# ────────────── Ensure Output Dir ──────────────
os.makedirs(PARQUET_DIR, exist_ok=True)

# ────────────── Security ──────────────
FORBIDDEN = {"DROP", "DELETE", "UPDATE", "INSERT"}

# ────────────── Query Cleaner ──────────────
def clean_query(raw_query: str) -> str:
    raw_query = raw_query.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    return raw_query.strip().rstrip(";")

# ────────────── Index ──────────────
@app.route("/")
def index():
    return "🌀 GreLa API is running. Use POST /api/query with JSON payload {'query': 'SQL'}."

# ────────────── API Endpoint ──────────────
@app.route("/api/query", methods=["POST"])
def run_query():
    try:
        data = request.get_json(force=True)
        if not data or "query" not in data:
            return jsonify({"error": "Missing 'query' parameter"}), 400

        query = clean_query(data["query"])
        params = data.get("params", [])
        fmt = data.get("format", "parquet").lower()

        # Validate format
        if fmt not in {"parquet", "csv", "json"}:
            return jsonify({"error": "Invalid format. Use 'parquet', 'csv', or 'json'."}), 400

        # Block unsafe operations
        if any(word in query.upper() for word in FORBIDDEN):
            return jsonify({"error": "Only SELECT queries are allowed"}), 403

        # Run query
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            df = conn.execute(query, params).fetchdf()
            conn.close()
        except Exception as e:
            return jsonify({"error": "Query failed", "details": str(e)}), 500

        # Row limit
        if len(df) > MAX_ROWS:
            return jsonify({"error": f"Result too large ({len(df)} rows). Limit is {MAX_ROWS}."}), 413

        if df.empty:
            return jsonify({
                "success": True,
                "message": "Query returned no results.",
                "row_count": 0,
                "columns": [],
                "preview": [],
                "query": query,
                "params": params
            }), 200

        # Check for complex DuckDB types
        complex_types = {"STRUCT", "LIST", "MAP"}
        has_complex = any(t.upper() in str(dtype).upper() for t in complex_types for dtype in df.dtypes)
        if has_complex and fmt != "parquet":
            return jsonify({
                "error": "This query returns complex types (STRUCT/LIST/MAP). Use 'parquet' format."
            }), 400

        # Save result to file
        file_id = str(uuid.uuid4())
        extension = {"parquet": "parquet", "csv": "csv", "json": "json"}[fmt]
        filename = f"{file_id}.{extension}"
        path = os.path.join(PARQUET_DIR, filename)

        try:
            if fmt == "parquet":
                df.to_parquet(path, index=False)
            elif fmt == "csv":
                df.to_csv(path, index=False, encoding="utf-8")
            elif fmt == "json":
                df.to_json(path, orient="records", force_ascii=False)
        except Exception as e:
            return jsonify({"error": "Failed to write output file", "details": str(e)}), 500

        # Metadata
        file_size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        public_url = f"{PUBLIC_URL_PREFIX}/{filename}"

        # Preview
        try:
            preview = df.head(5).fillna("").astype(str).to_dict(orient="records")
        except Exception:
            preview = []

        return jsonify({
            "success": True,
            "download_url": public_url,
            "file_format": fmt,
            "file_size_mb": file_size_mb,
            "row_count": len(df),
            "columns": [{"name": col, "dtype": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)],
            "preview": preview,
            "query": query,
            "params": params
        }), 200

    except Exception as e:
        return jsonify({"error": "Server error", "details": str(e)}), 500

# ────────────── Run Local Dev Server ──────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)