import os
import json
from flask import Flask, send_from_directory, jsonify, request
from databricks.sdk import WorkspaceClient

app = Flask(__name__, static_folder="static")

TABLE = "bg_bida.proj_agm.silver_assy_aec_l14_compression_weight_data"


def get_ws_client():
    return WorkspaceClient()


def query_sql(sql):
    """Execute SQL via Databricks SDK statement execution."""
    w = get_ws_client()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        warehouses = list(w.warehouses.list())
        if not warehouses:
            raise RuntimeError("No SQL warehouse available")
        warehouse_id = warehouses[0].id

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    columns = [col.name for col in response.manifest.schema.columns]
    rows = []
    if response.result and response.result.data_array:
        for row in response.result.data_array:
            rows.append(dict(zip(columns, row)))
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
