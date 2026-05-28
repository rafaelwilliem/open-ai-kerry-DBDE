"""
Kafka consumer — dual write:
  - ALL readings → InfluxDB (high-frequency TSDB)
  - Anomaly events only → MySQL shift_logs (low-frequency events)
"""
import json
import os
import selectors
import time
from datetime import datetime, timezone

# Monkeypatch selectors to avoid ValueError: Invalid file descriptor: -1 on Windows + Python 3.12+
def patch_selector(cls):
    if hasattr(cls, 'unregister'):
        orig_unreg = cls.unregister
        def safe_unreg(self, fileobj):
            try:
                return orig_unreg(self, fileobj)
            except (ValueError, KeyError, AttributeError):
                return None
        cls.unregister = safe_unreg

patch_selector(selectors.BaseSelector)
if hasattr(selectors, '_BaseSelectorImpl'):
    patch_selector(selectors._BaseSelectorImpl)
if hasattr(selectors, 'SelectSelector'):
    patch_selector(selectors.SelectSelector)

import mysql.connector
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer
from urllib3.util import Retry

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
    # Setup retry strategy for transient connection errors and status codes in urllib3
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, retries=retries)
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
    
    influx_client = None
    write_api = None
    mysql_conn = None
    
    try:
        influx_client, write_api = get_influx_write_api()
    except Exception as e:
        print(f"[consumer] Initial InfluxDB connection failed: {e}")
        
    try:
        mysql_conn = get_mysql_conn()
    except Exception as e:
        print(f"[consumer] Initial MySQL connection failed: {e}")

    print(f"[consumer] Listening on '{TOPIC}' from {KAFKA_BROKER}")

    try:
        for kafka_msg in consumer:
            try:
                msg = kafka_msg.value
                
                # Write to InfluxDB with reconnection support
                try:
                    # If initial connection failed, try to establish it now
                    if not write_api:
                        influx_client, write_api = get_influx_write_api()
                    write_to_influx(write_api, msg)
                except Exception as e:
                    print(f"[consumer] InfluxDB write failed: {e}. Reconnecting and retrying...")
                    try:
                        if influx_client:
                            influx_client.close()
                    except Exception:
                        pass
                    write_api = None
                    time.sleep(1)
                    try:
                        influx_client, write_api = get_influx_write_api()
                        write_to_influx(write_api, msg)
                    except Exception as retry_err:
                        print(f"[consumer] InfluxDB retry failed: {retry_err}")
                        write_api = None

                # Write anomaly to MySQL with reconnection support
                if msg.get("anomaly_flag"):
                    try:
                        # If initial connection failed or got closed, try to connect/reconnect
                        if not mysql_conn or not mysql_conn.is_connected():
                            mysql_conn = get_mysql_conn()
                        write_anomaly_to_mysql(mysql_conn, msg)
                    except Exception as e:
                        print(f"[consumer] MySQL write failed: {e}. Reconnecting and retrying...")
                        try:
                            if mysql_conn:
                                mysql_conn.close()
                        except Exception:
                            pass
                        mysql_conn = None
                        time.sleep(1)
                        try:
                            mysql_conn = get_mysql_conn()
                            write_anomaly_to_mysql(mysql_conn, msg)
                        except Exception as retry_err:
                            print(f"[consumer] MySQL retry failed: {retry_err}")
                            mysql_conn = None
                    
                    print(f"[consumer] ⚠ ANOMALY logged to MySQL — {msg['machine_id']}")
                else:
                    print(f"[consumer] ✓ {msg['machine_id']} → InfluxDB")
            except Exception as msg_err:
                print(f"[consumer] Error processing message: {msg_err}")
                time.sleep(2)
    finally:
        if influx_client:
            try:
                influx_client.close()
            except Exception:
                pass
        if mysql_conn:
            try:
                mysql_conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
