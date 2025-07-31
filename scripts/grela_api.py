import os
import uuid
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
QUERY_DIR = "/tmp/grela-queries"
PUBLIC_URL_PREFIX = "https://ccs-lab.zcu.cz/grela-api-out"
MAX_ROWS = 500_000_000
FORBIDDEN = {"DROP", "DELETE", "UPDATE", "INSERT"}

os.makedirs(PARQUET_DIR, exist_ok=True)
os.makedirs(QUERY_DIR, exist_ok=True)

# ────────────── Index ──────────────
@app.route("/")
def index():
    return "🌀 GreLa API is running. Use POST /api/query with JSON payload {'query': 'SQL'}."

# ────────────── Main Endpoint ──────────────
@app.route("/api/query", methods=["POST"])
def run_query():
    try:
        data = request.get_json(force=True)
        if not data or "query" not in data:
            return jsonify({"error": "Missing 'query' parameter"}), 400

        raw_query = data["query"]
        params = data.get("params", [])
        fmt = data.get("format", "parquet").lower()

        # Validate output format
        if fmt not in {"parquet", "csv", "json"}:
            return jsonify({"error": "Invalid format. Use 'parquet', 'csv', or 'json'."}), 400

        # Save query to file
        query_id = str(uuid.uuid4())
        query_path = os.path.join(QUERY_DIR, f"{query_id}.sql")
        with open(query_path, "w", encoding="utf-8") as f:
            f.write(raw_query)

        # Load and validate query text
        with open(query_path, "r", encoding="utf-8") as f:
            query_text = f.read().strip().rstrip(";")

        if any(forbidden in query_text.upper() for forbidden in FORBIDDEN):
            return jsonify({"error": "Only SELECT queries are allowed"}), 403

        # Execute query
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            df = conn.execute(query_text, params).fetchdf()
            conn.close()
        except Exception as e:
            return jsonify({"error": "Query failed", "details": str(e)}), 500

        if len(df) > MAX_ROWS:
            return jsonify({"error": f"Result too large ({len(df)} rows). Limit is {MAX_ROWS}."}), 413

        if df.empty:
            return jsonify({
                "success": True,
                "message": "Query returned no results.",
                "row_count": 0,
                "columns": [],
                "preview": [],
                "query": query_text,
                "params": params
            }), 200

        # Complex data types enforcement
        complex_types = {"STRUCT", "LIST", "MAP"}
        if any(t.upper() in str(dtype).upper() for t in complex_types for dtype in df.dtypes) and fmt != "parquet":
            return jsonify({"error": "This query returns complex types (STRUCT/LIST/MAP). Use 'parquet' format."}), 400

        # Save output
        file_id = str(uuid.uuid4())
        extension = fmt
        filename = f"{file_id}.{extension}"
        output_path = os.path.join(PARQUET_DIR, filename)

        try:
            if fmt == "parquet":
                df.to_parquet(output_path, index=False)
            elif fmt == "csv":
                df.to_csv(output_path, index=False, encoding="utf-8")
            elif fmt == "json":
                df.to_json(output_path, orient="records", force_ascii=False)
        except Exception as e:
            return jsonify({"error": "Failed to write output file", "details": str(e)}), 500

        # Return metadata and download URL
        public_url = f"{PUBLIC_URL_PREFIX}/{filename}"
        preview = df.head(5).fillna("").astype(str).to_dict(orient="records")
        return jsonify({
            "success": True,
            "download_url": public_url,
            "file_format": fmt,
            "file_size_mb": round(os.path.getsize(output_path) / (1024 * 1024), 2),
            "row_count": len(df),
            "columns": [{"name": col, "dtype": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)],
            "preview": preview,
            "query": query_text,
            "params": params
        }), 200

    except Exception as e:
        return jsonify({"error": "Server error", "details": str(e)}), 500

# ────────────── Dev Server ──────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)