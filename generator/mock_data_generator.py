"""
Mock sensor data generator — publishes to Kafka topic sensor.raw
Simulates 4 Kerry Group machines with realistic sensor ranges + 10% anomaly spikes
"""
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor.raw")

# Normal operating ranges per machine type: (min, max)
MACHINE_PROFILES = {
    "pump_01": {
        "vibration":       (0.5, 3.0),   # mm/s
        "temperature":     (40.0, 65.0), # °C
        "electric_current":(8.0, 15.0),  # A
        "pressure":        (2.0, 5.0),   # bar
        "rpm":             (1400, 1500), # RPM
        "flow_rate":       (20.0, 40.0), # L/min
    },
    "mixer_01": {
        "vibration":       (1.0, 4.0),
        "temperature":     (35.0, 55.0),
        "electric_current":(12.0, 22.0),
        "pressure":        (0.5, 2.0),
        "rpm":             (60, 120),
        "flow_rate":       (5.0, 15.0),
    },
    "spray_dryer_01": {
        "vibration":       (0.3, 2.0),
        "temperature":     (150.0, 200.0),
        "electric_current":(20.0, 35.0),
        "pressure":        (3.0, 8.0),
        "rpm":             (3000, 3600),
        "flow_rate":       (50.0, 100.0),
    },
    "compressor_01": {
        "vibration":       (1.5, 5.0),
        "temperature":     (60.0, 85.0),
        "electric_current":(15.0, 28.0),
        "pressure":        (6.0, 10.0),
        "rpm":             (2800, 3000),
        "flow_rate":       (30.0, 60.0),
    },
}

# Anomaly multipliers per parameter
ANOMALY_MULTIPLIERS = {
    "vibration": 3.5,
    "temperature": 1.4,
    "electric_current": 1.6,
    "pressure": 1.8,
    "rpm": 1.2,
    "flow_rate": 0.2,  # drop instead of spike
}


def generate_reading(machine_id: str) -> dict:
    profile = MACHINE_PROFILES[machine_id]
    is_anomaly = random.random() < 0.10  # 10% chance

    reading: dict[str, Any] = {"machine_id": machine_id, "timestamp": datetime.now(timezone.utc).isoformat()}
    for param, (lo, hi) in profile.items():
        value = random.uniform(lo, hi)
        if is_anomaly:
            mult = ANOMALY_MULTIPLIERS[param]
            value = value * mult if mult > 1 else lo * mult
        reading[param] = round(value, 3)

    reading["anomaly_flag"] = is_anomaly
    return reading


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[generator] Connected to Kafka at {KAFKA_BROKER}, publishing to '{TOPIC}'")

    while True:
        for machine_id in MACHINE_PROFILES:
            msg = generate_reading(machine_id)
            producer.send(TOPIC, value=msg)
            flag = "⚠ ANOMALY" if msg["anomaly_flag"] else "OK"
            print(f"[{msg['timestamp']}] {machine_id} | vib={msg['vibration']} temp={msg['temperature']} | {flag}")
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    main()
