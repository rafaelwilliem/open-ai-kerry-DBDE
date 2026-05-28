"""
sensor_etl_dag.py — Hourly ETL: InfluxDB + MySQL → Parquet
Tasks: extract_influxdb → extract_mysql → transform → load_parquet
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

INFLUX_URL    = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN", "kerry-super-secret-token-2024")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG", "kerry_group")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "kerry_mes")
MYSQL_USER = os.getenv("MYSQL_USER", "kerry")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "kerry123")

OUTPUT_DIR = "/opt/airflow/data/processed"

default_args = {"owner": "kerry_de", "retries": 1, "retry_delay": timedelta(minutes=5)}


def extract_influxdb(**ctx):
    from influxdb_client import InfluxDBClient

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_readings")
      |> pivot(rowKey: ["_time", "machine_id"], columnKey: ["_field"], valueColumn: "_value")
    """
    df = client.query_api().query_data_frame(query, org=INFLUX_ORG)
    client.close()

    if df.empty:
        ctx["ti"].xcom_push(key="sensor_df", value=None)
        return

    df = df.rename(columns={"_time": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    ctx["ti"].xcom_push(key="sensor_df", value=df.to_json())


def extract_mysql(**ctx):
    import mysql.connector

    conn = mysql.connector.connect(
        host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
    )
    df = pd.read_sql(
        "SELECT machine_id, end_time, mtbf_hours, mttr_hours FROM maintenance_records ORDER BY end_time",
        conn,
    )
    conn.close()
    df["end_time"] = pd.to_datetime(df["end_time"], utc=True)
    ctx["ti"].xcom_push(key="maintenance_df", value=df.to_json())


ANOMALY_THRESHOLDS = {
    "vibration_max":        10.0,
    "temperature_max":      90.0,
    "electric_current_max": 40.0,
    "pressure_max":         12.0,
}


def transform(**ctx):
    sensor_json = ctx["ti"].xcom_pull(key="sensor_df", task_ids="extract_influxdb")
    maint_json  = ctx["ti"].xcom_pull(key="maintenance_df", task_ids="extract_mysql")

    if not sensor_json:
        print("[transform] No sensor data — skipping")
        ctx["ti"].xcom_push(key="result_df", value=None)
        return

    sensor_df = pd.read_json(sensor_json)
    maint_df  = pd.read_json(maint_json)

    sensor_df["timestamp"] = pd.to_datetime(sensor_df["timestamp"], utc=True)
    maint_df["end_time"]   = pd.to_datetime(maint_df["end_time"], utc=True)

    sensor_fields = ["vibration", "temperature", "electric_current", "pressure", "rpm", "flow_rate"]

    # 1. Aggregate per minute per machine
    sensor_df = sensor_df.set_index("timestamp").sort_index()
    agg = (
        sensor_df.groupby(["machine_id", pd.Grouper(freq="1min")])[sensor_fields]
        .agg(["mean", "max", "min"])
    )
    agg.columns = ["_".join(c) for c in agg.columns]
    agg = agg.reset_index()

    # Forward-fill missing values
    num_cols = agg.select_dtypes("number").columns
    agg[num_cols] = agg[num_cols].ffill()

    # 2. Timestamp alignment: merge_asof sensor (ms) ↔ maintenance records (hourly)
    #    For each machine, find the most recent maintenance end_time ≤ sensor timestamp
    results = []
    for machine_id, grp in agg.groupby("machine_id"):
        m = maint_df[maint_df["machine_id"] == machine_id].sort_values("end_time")
        grp = grp.sort_values("timestamp")
        merged = pd.merge_asof(
            grp,
            m[["end_time", "mtbf_hours", "mttr_hours"]],
            left_on="timestamp",
            right_on="end_time",
            tolerance=pd.Timedelta("1h"),
            direction="backward",
        )
        results.append(merged)

    agg = pd.concat(results, ignore_index=True)

    # 3. Feature engineering
    agg["hours_since_last_maintenance"] = (
        (agg["timestamp"] - agg["end_time"]).dt.total_seconds() / 3600
    ).round(2)

    agg = agg.sort_values(["machine_id", "timestamp"])
    agg["rolling_avg_vibration_1h"] = (
        agg.groupby("machine_id")["vibration_mean"]
        .transform(lambda x: x.rolling(window=60, min_periods=1).mean())
        .round(4)
    )

    agg["anomaly_flag"] = (
        (agg["vibration_max"]        > ANOMALY_THRESHOLDS["vibration_max"])
        | (agg["temperature_max"]    > ANOMALY_THRESHOLDS["temperature_max"])
        | (agg["electric_current_max"] > ANOMALY_THRESHOLDS["electric_current_max"])
        | (agg["pressure_max"]       > ANOMALY_THRESHOLDS["pressure_max"])
    )

    ctx["ti"].xcom_push(key="result_df", value=agg.to_json())


def load_parquet(**ctx):
    result_json = ctx["ti"].xcom_pull(key="result_df", task_ids="transform")
    if not result_json:
        print("[load_parquet] Nothing to write")
        return

    df = pd.read_json(result_json)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H")
    path = f"{OUTPUT_DIR}/sensor_{ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"[load_parquet] Written → {path} ({len(df)} rows)")


with DAG(
    dag_id="sensor_etl",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    tags=["kerry", "etl"],
) as dag:

    t1 = PythonOperator(task_id="extract_influxdb", python_callable=extract_influxdb)
    t2 = PythonOperator(task_id="extract_mysql",    python_callable=extract_mysql)
    t3 = PythonOperator(task_id="transform",        python_callable=transform)
    t4 = PythonOperator(task_id="load_parquet",     python_callable=load_parquet)

    [t1, t2] >> t3 >> t4
