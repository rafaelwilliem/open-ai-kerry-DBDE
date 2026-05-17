"""
Kerry Group Predictive Maintenance — REST API
Endpoints:
  GET /machines                              → list all machines + status (MySQL)
  GET /sensors/{machine_id}/latest           → latest sensor reading (InfluxDB)
  GET /sensors/{machine_id}/history?hours=24 → historical readings (InfluxDB)
"""
import os

import mysql.connector
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient

INFLUX_URL    = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN", "kerry-super-secret-token-2024")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG", "kerry_group")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "kerry_mes")
MYSQL_USER = os.getenv("MYSQL_USER", "kerry")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "kerry123")

app = FastAPI(title="Kerry Group Sensor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
    )


def influx_client():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


@app.get("/machines")
def get_machines():
    conn = mysql_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, name, type, location, status FROM machines")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


@app.get("/sensors/{machine_id}/latest")
def get_latest(machine_id: str):
    client = influx_client()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "sensor_readings" and r.machine_id == "{machine_id}")
      |> last()
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    tables = client.query_api().query(query, org=INFLUX_ORG)
    client.close()

    records = [
        {**r.values, "timestamp": r.values.get("_time")}
        for table in tables for r in table.records
    ]
    if not records:
        raise HTTPException(status_code=404, detail=f"No recent data for {machine_id}")
    return records[-1]


@app.get("/sensors/{machine_id}/history")
def get_history(machine_id: str, hours: int = Query(default=24, ge=1, le=168)):
    client = influx_client()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "sensor_readings" and r.machine_id == "{machine_id}")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    """
    tables = client.query_api().query(query, org=INFLUX_ORG)
    client.close()

    return [
        {**r.values, "timestamp": str(r.values.get("_time"))}
        for table in tables for r in table.records
    ]
