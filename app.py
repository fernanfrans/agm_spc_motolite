import os
import json
from flask import Flask, send_from_directory, jsonify, request
from databricks.connect import DatabricksSession

app = Flask(__name__, static_folder="static")

TABLE = "bg_bida.proj_agm.silver_assy_aec_l14_compression_weight_data"


def get_spark():
    """Get Spark session via Databricks Connect (serverless)."""
    return DatabricksSession.builder.serverless(True).getOrCreate()


def query_sql(sql):
    """Execute SQL via Databricks Connect serverless Spark."""
    spark = get_spark()
    df = spark.sql(sql)
    columns = df.columns
    rows = [dict(zip(columns, row)) for row in df.collect()]
    return rows


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/data")
def get_data():
    """Get compression data. Query params: line, param (force/weight), limit."""
    line = request.args.get("line", "Long Line")
    limit = int(request.args.get("limit", "100"))

    sql = f"""
    SELECT line, timestamp, date, force, weight,
           force_lsl, force_usl, weight_lsl, weight_usl,
           status, source_datetime
    FROM {TABLE}
    WHERE line = '{line}'
    ORDER BY source_datetime DESC
    LIMIT {limit}
    """
    rows = query_sql(sql)
    return jsonify({"rows": rows})


@app.route("/api/lines")
def get_lines():
    """Get distinct line values."""
    sql = f"SELECT DISTINCT line FROM {TABLE} ORDER BY line"
    rows = query_sql(sql)
    return jsonify({"lines": [r["line"] for r in rows]})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
