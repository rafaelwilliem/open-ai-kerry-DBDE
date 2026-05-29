import argparse
import json
import os
import time
import selectors
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
from kafka import KafkaProducer

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
if KAFKA_BROKER == "kafka:29092" and not os.path.exists('/.dockerenv'):
    KAFKA_BROKER = "localhost:9092"
TOPIC        = os.getenv("KAFKA_TOPIC", "sensor.raw")

# MySQL configs
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "kerry_mes")
MYSQL_USER = os.getenv("MYSQL_USER", "kerry")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "kerry123")

# Map Unit IDs to Kerry Group Machine IDs
MACHINE_MAP = {
    1: "pump_01",
    2: "mixer_01",
    3: "spray_dryer_01",
    4: "compressor_01"
}

# Target anomaly thresholds for reference in mapping
THRESHOLDS = {
    "vibration": 10.0,
    "temperature": 90.0,
    "electric_current": 40.0,
    "pressure": 12.0
}

def get_mysql_conn():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST, database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
        )
    except Exception as e:
        print(f"[warning] Could not connect to MySQL database: {e}")
        return None

def update_machine_status(machine_id: str, status: str):
    conn = get_mysql_conn()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE machines SET status = %s WHERE id = %s", (status, machine_id)
        )
        conn.commit()
        cursor.close()
        print(f"[db] Updated status of '{machine_id}' to '{status}' in MySQL")
    except Exception as e:
        print(f"[warning] Failed to update status in DB: {e}")
    finally:
        conn.close()

def parse_nasa_file(filepath: str):
    """
    Parses the CMAPSS txt file and groups rows by unit number.
    Returns: {unit_id: [list of raw rows]}
    """
    data = {}
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"NASA file not found: {filepath}")

    with open(filepath, "r") as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue
            unit_id = int(parts[0])
            if unit_id not in MACHINE_MAP:
                continue  # We only simulate units 1 to 4 mapped to our machines
            
            if unit_id not in data:
                data[unit_id] = []
            data[unit_id].append([float(x) for x in parts])
            
    print(f"[generator] Parsed file '{filepath}'. Loaded units: {list(data.keys())}")
    return data

def scale_sensor_values(machine_id: str, raw_row: list) -> dict:
    """
    Maps NASA sensor measurements to Kerry Group machine sensor ranges.
    Inputs:
        raw_row: list of floats representing a line in train_FD001.txt
    Output:
        dict containing: vibration, temperature, electric_current, pressure, rpm, flow_rate, anomaly_flag
    """
    # 0-indexed column positions based on CMAPSS layout:
    # 0: unit
    # 1: cycle
    # 6: Sensor 2 (temp, LPC outlet)
    # 7: Sensor 3 (temp, HPC outlet)
    # 8: Sensor 4 (temp, LPT outlet)
    # 11: Sensor 7 (pressure, HPC outlet)
    # 13: Sensor 9 (speed, Nc)
    # 15: Sensor 11 (static pressure at HPC outlet)
    # 16: Sensor 12 (ratio of fuel flow to Ps30)
    # 19: Sensor 15 (BPR)
    
    # Extract raw values from row
    s_temp_lpc = raw_row[6]
    s_temp_hpc = raw_row[7]
    s_temp_lpt = raw_row[8]
    s_speed_nc = raw_row[13]
    s_pres_static = raw_row[15]
    s_fuel_flow = raw_row[16]
    s_bpr = raw_row[19]

    # Normalize values between 0 and 1 using typical dataset ranges for FD001
    v_norm = max(0.0, min(1.0, (s_bpr - 8.35) / (8.58 - 8.35)))          # Vibration representation
    t_norm = max(0.0, min(1.0, (s_temp_hpc - 1582.0) / (1610.0 - 1582.0))) # Temp representation
    c_norm = max(0.0, min(1.0, (s_pres_static - 46.8) / (48.5 - 46.8)))  # Current representation
    p_norm = max(0.0, min(1.0, (s_temp_lpt - 1398.0) / (1430.0 - 1398.0))) # Pressure representation
    r_norm = max(0.0, min(1.0, (9050.0 - s_speed_nc) / (9050.0 - 9020.0))) # Speed representation (descending Nc)
    f_norm = max(0.0, min(1.0, (522.5 - s_fuel_flow) / (522.5 - 518.5)))  # Flow rate representation (descending)

    reading = {}

    if machine_id == "pump_01":
        reading["vibration"] = round(0.5 + v_norm * 11.0, 3) # min 0.5, max 11.5 (threshold: 10)
        reading["temperature"] = round(40.0 + t_norm * 55.0, 1) # min 40, max 95 (threshold: 90)
        reading["electric_current"] = round(8.0 + c_norm * 35.0, 1) # min 8, max 43 (threshold: 40)
        reading["pressure"] = round(2.0 + p_norm * 11.0, 2) # min 2, max 13 (threshold: 12)
        reading["rpm"] = int(1400 + r_norm * 100)
        reading["flow_rate"] = round(40.0 - f_norm * 20.0, 1)

    elif machine_id == "mixer_01":
        reading["vibration"] = round(1.0 + v_norm * 10.5, 3)
        reading["temperature"] = round(35.0 + t_norm * 60.0, 1)
        reading["electric_current"] = round(12.0 + c_norm * 30.0, 1)
        reading["pressure"] = round(0.5 + p_norm * 12.0, 2)
        reading["rpm"] = int(60 + r_norm * 60)
        reading["flow_rate"] = round(15.0 - f_norm * 10.0, 1)

    elif machine_id == "spray_dryer_01":
        reading["vibration"] = round(0.3 + v_norm * 11.2, 3)
        reading["temperature"] = round(150.0 + t_norm * 55.0, 1) # high temp, normal is 150-200. Will exceed 90.0
        reading["electric_current"] = round(20.0 + c_norm * 23.0, 1)
        reading["pressure"] = round(3.0 + p_norm * 10.0, 2)
        reading["rpm"] = int(3000 + r_norm * 600)
        reading["flow_rate"] = round(100.0 - f_norm * 50.0, 1)

    elif machine_id == "compressor_01":
        reading["vibration"] = round(1.5 + v_norm * 10.0, 3)
        reading["temperature"] = round(60.0 + t_norm * 35.0, 1)
        reading["electric_current"] = round(15.0 + c_norm * 28.0, 1)
        reading["pressure"] = round(6.0 + p_norm * 7.0, 2)
        reading["rpm"] = int(2800 + r_norm * 200)
        reading["flow_rate"] = round(60.0 - f_norm * 30.0, 1)

    # Determine anomaly flag based on plant thresholds (from DATA_CONTRACT)
    anomaly_flag = (
        reading["vibration"] > THRESHOLDS["vibration"] or
        reading["temperature"] > THRESHOLDS["temperature"] or
        reading["electric_current"] > THRESHOLDS["electric_current"] or
        reading["pressure"] > THRESHOLDS["pressure"]
    )
    reading["anomaly_flag"] = bool(anomaly_flag)
    return reading

def main():
    parser = argparse.ArgumentParser(description="NASA CMAPSS Data Stream Generator")
    parser.add_argument("--file", type=str, default="data/train_FD001.txt", help="Path to CMAPSS txt file")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds to wait between cycles")
    parser.add_argument("--broker", type=str, default=KAFKA_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", type=str, default=TOPIC, help="Kafka topic name")
    parser.add_argument("--dry", action="store_true", help="Dry run mode (print output without sending to Kafka)")
    args = parser.parse_args()

    producer = None
    if args.dry:
        print("[generator] Running in DRY-RUN mode. Messages will be printed but not sent to Kafka.")
    else:
        # Init Kafka Producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"[generator] Connected to Kafka at {args.broker}")
        except Exception as e:
            print(f"[warning] Failed to connect to Kafka broker ({e}). Falling back to DRY-RUN mode.")
            producer = None

    # Parse dataset
    try:
        units_data = parse_nasa_file(args.file)
    except Exception as e:
        print(f"[error] Failed to parse file: {e}")
        return

    cycle_idx = 0
    # Keep track of active state and countdowns for maintenance simulation
    # states: 'running', 'maintenance'
    machine_states = {mid: {"state": "running", "maint_cycles": 0, "current_row": 0} for mid in MACHINE_MAP.values()}

    print(f"[generator] Starting simulation loop on '{args.topic}'. Firing speed: {args.delay}s/cycle.")
    
    # Set all machines to running initially
    for mid in MACHINE_MAP.values():
        update_machine_status(mid, "running")

    try:
        while True:
            sent_any = False
            
            for unit_id, machine_id in MACHINE_MAP.items():
                state_info = machine_states[machine_id]
                rows = units_data.get(unit_id, [])
                
                if state_info["state"] == "maintenance":
                    # Simulating maintenance downtime
                    state_info["maint_cycles"] -= 1
                    if state_info["maint_cycles"] <= 0:
                        state_info["state"] = "running"
                        state_info["current_row"] = 0 # reset back to first cycle
                        print(f"\n[system] [REPAIR_COMPLETE] Maintenance complete for {machine_id}. Resetting cycle to 1.")
                        update_machine_status(machine_id, "running")
                    else:
                        print(f"[{machine_id}] [UNDER_MAINTENANCE] (Remaining: {state_info['maint_cycles']} cycles)...", end="\r")
                    continue
                
                curr_row_idx = state_info["current_row"]
                if curr_row_idx >= len(rows):
                    # Out of rows (reached failure cycle). Trigger maintenance simulation.
                    state_info["state"] = "maintenance"
                    state_info["maint_cycles"] = 10 # 10 cycles of maintenance
                    print(f"\n[system] [FAILURE_REACHED] Failure reached for {machine_id}! Scheduling maintenance shutdown...")
                    update_machine_status(machine_id, "maintenance")
                    continue
                
                # Fetch row and calculate metrics
                raw_row = rows[curr_row_idx]
                reading = scale_sensor_values(machine_id, raw_row)
                
                # Update status in db if it changes to anomaly/fault
                if reading["anomaly_flag"]:
                    update_machine_status(machine_id, "fault")
                
                # Add metadata
                payload = {
                    "machine_id": machine_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "vibration": reading["vibration"],
                    "temperature": reading["temperature"],
                    "electric_current": reading["electric_current"],
                    "pressure": reading["pressure"],
                    "rpm": reading["rpm"],
                    "flow_rate": reading["flow_rate"],
                    "anomaly_flag": reading["anomaly_flag"],
                    "nasa_cycle": int(raw_row[1])
                }
                
                # Publish to Kafka
                if producer:
                    producer.send(args.topic, value=payload)
                    sent_any = True
                
                flag = "WARNING" if payload["anomaly_flag"] else "OK"
                print(f"[{payload['timestamp']}] {machine_id} (Cycle {payload['nasa_cycle']}) | vib={payload['vibration']} temp={payload['temperature']} | {flag}")
                
                # Increment row index
                state_info["current_row"] += 1
            
            if sent_any and producer:
                producer.flush()
                
            time.sleep(args.delay)
            cycle_idx += 1
            
    except KeyboardInterrupt:
        print("[generator] Simulation stopped by user.")
    finally:
        # Reset statuses to running on exit
        for mid in MACHINE_MAP.values():
            update_machine_status(mid, "running")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
