"""
Kafka consumer — dual write:
  - ALL readings → InfluxDB (high-frequency TSDB)
  - Anomaly events only → MySQL shift_logs (low-frequency events)
"""
import json
import os
from datetime import datetime, timezone

import mysql.connector
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = os.getenv("KAFKA_TOPIC", "sensor.raw")

INFLUX_URL    = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUXDB_TOKEN", "kerry-super-secret-token-2024")
INFLUX_ORG    = os.getenv("INFLUXDB_ORG", "kerry_group")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "kerry_mes")
MYSQL_USER = os.getenv("MYSQL_USER", "kerry")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "kerry123")

SENSOR_FIELDS = ["vibration", "temperature", "electric_current", "pressure", "rpm", "flow_rate"]


def get_influx_write_api():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    return client, client.write_api(write_options=SYNCHRONOUS)


def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
    )


def write_to_influx(write_api, msg: dict):
    point = Point("sensor_readings").tag("machine_id", msg["machine_id"])
    for field in SENSOR_FIELDS:
        if field in msg:
            point = point.field(field, float(msg[field]))
    point = point.field("anomaly_flag", int(msg.get("anomaly_flag", False)))
    point = point.time(msg["timestamp"], WritePrecision.NS)
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)


def write_anomaly_to_mysql(conn, msg: dict):
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO shift_logs (machine_id, shift_date, shift_type, operator_name, machine_status, notes)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (
            msg["machine_id"],
            datetime.now(timezone.utc).date(),
            "morning",  # placeholder — real system would derive from timestamp
            "system",
            "fault",
            f"Anomaly detected: vib={msg.get('vibration')} temp={msg.get('temperature')}",
        ),
    )
    conn.commit()
    cursor.close()


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="kerry-consumer-group",
    )
    influx_client, write_api = get_influx_write_api()
    mysql_conn = get_mysql_conn()

    print(f"[consumer] Listening on '{TOPIC}' from {KAFKA_BROKER}")

    try:
        for kafka_msg in consumer:
            msg = kafka_msg.value
            write_to_influx(write_api, msg)

            if msg.get("anomaly_flag"):
                write_anomaly_to_mysql(mysql_conn, msg)
                print(f"[consumer] ⚠ ANOMALY logged to MySQL — {msg['machine_id']}")
            else:
                print(f"[consumer] ✓ {msg['machine_id']} → InfluxDB")
    finally:
        influx_client.close()
        mysql_conn.close()


if __name__ == "__main__":
    main()
