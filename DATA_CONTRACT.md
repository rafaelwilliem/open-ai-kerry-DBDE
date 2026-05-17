# Data Contract — Kerry Group Predictive Maintenance PoC

**Version:** 1.0.0 | **Owner:** Data Engineering | **Status:** Draft

---

## 1. Parquet Output Schema (for ML Team)

File location: `./data/processed/sensor_YYYY-MM-DD_HH.parquet`  
Schedule: Generated hourly by Airflow DAG `sensor_etl`

| Column | Type | Description |
|---|---|---|
| `machine_id` | string | Machine identifier (`pump_01`, `mixer_01`, `spray_dryer_01`, `compressor_01`) |
| `timestamp` | datetime[UTC] | Minute-level bucket timestamp |
| `vibration_mean` | float64 | Mean vibration (mm/s) over 1-min window |
| `vibration_max` | float64 | Max vibration (mm/s) over 1-min window |
| `vibration_min` | float64 | Min vibration (mm/s) over 1-min window |
| `temperature_mean` | float64 | Mean temperature (°C) |
| `temperature_max` | float64 | Max temperature (°C) |
| `temperature_min` | float64 | Min temperature (°C) |
| `electric_current_mean` | float64 | Mean current (A) |
| `electric_current_max` | float64 | Max current (A) |
| `electric_current_min` | float64 | Min current (A) |
| `pressure_mean` | float64 | Mean pressure (bar) |
| `pressure_max` | float64 | Max pressure (bar) |
| `pressure_min` | float64 | Min pressure (bar) |
| `rpm_mean` | float64 | Mean RPM |
| `rpm_max` | float64 | Max RPM |
| `rpm_min` | float64 | Min RPM |
| `flow_rate_mean` | float64 | Mean flow rate (L/min) |
| `flow_rate_max` | float64 | Max flow rate (L/min) |
| `flow_rate_min` | float64 | Min flow rate (L/min) |
| `end_time` | datetime[UTC] | Timestamp of last completed maintenance |
| `mtbf_hours` | float64 | Mean Time Between Failures (hours) from last maintenance record |
| `mttr_hours` | float64 | Mean Time To Repair (hours) from last maintenance record |
| `hours_since_last_maintenance` | float64 | Hours elapsed since last maintenance end_time |
| `rolling_avg_vibration_1h` | float64 | 60-minute rolling mean of vibration_mean |
| `anomaly_flag` | bool | True if any parameter exceeds threshold (see thresholds below) |

**Anomaly Thresholds:**
- `vibration_max > 10.0 mm/s`
- `temperature_max > 90.0 °C`
- `electric_current_max > 40.0 A`
- `pressure_max > 12.0 bar`

**Load example:**
```python
import pandas as pd
df = pd.read_parquet("./data/processed/sensor_2026-05-17_10.parquet")
df.info()
```

---

## 2. REST API Endpoints (for Frontend Team)

Base URL: `http://localhost:8000`  
Docs (Swagger UI): `http://localhost:8000/docs`

### GET /machines
Returns all machines and their current status.

**Response:**
```json
[
  { "id": "pump_01", "name": "Feed Pump #1", "type": "pump", "location": "Line A - Wet Section", "status": "running" },
  { "id": "mixer_01", "name": "Ingredient Mixer #1", "type": "mixer", "location": "Line A - Mix Section", "status": "running" }
]
```

### GET /sensors/{machine_id}/latest
Returns the most recent sensor reading for a machine (within last 5 minutes).

**Example:** `GET /sensors/pump_01/latest`

**Response:**
```json
{
  "machine_id": "pump_01",
  "timestamp": "2026-05-17T10:23:45.123Z",
  "vibration": 2.341,
  "temperature": 58.7,
  "electric_current": 11.2,
  "pressure": 3.8,
  "rpm": 1462,
  "flow_rate": 31.5,
  "anomaly_flag": 0
}
```

**Error (404):** Machine not found or no data in last 5 minutes.

### GET /sensors/{machine_id}/history?hours=24
Returns historical readings for a machine. `hours` param: 1–168 (default: 24).

**Example:** `GET /sensors/spray_dryer_01/history?hours=6`

**Response:**
```json
[
  { "machine_id": "spray_dryer_01", "timestamp": "2026-05-17T04:00:00Z", "vibration": 1.2, "temperature": 175.3, ... },
  { "machine_id": "spray_dryer_01", "timestamp": "2026-05-17T04:00:01Z", "vibration": 1.3, "temperature": 176.1, ... }
]
```

---

## 3. Direct InfluxDB Access (Flux Query Examples)

Connection: `http://localhost:8086` | Org: `kerry_group` | Token: see `.env`

**Last 1 hour of vibration for pump_01:**
```flux
from(bucket: "sensor_data")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_readings" and r.machine_id == "pump_01" and r._field == "vibration")
```

**All anomaly events in last 24h:**
```flux
from(bucket: "sensor_data")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "sensor_readings" and r._field == "anomaly_flag" and r._value == 1)
```

---

## 4. Open Questions — ML Team

| # | Question | Why it matters |
|---|---|---|
| 1 | What is the definition of a "failure" label? (binary fault/no-fault, or multi-class per failure mode?) | Determines `anomaly_flag` threshold tuning and label generation strategy |
| 2 | What is the target prediction horizon for RUL? (e.g., predict failure within next 24h / 72h / 7 days?) | Affects feature window size and lag feature engineering |
| 3 | Is the Parquet schema sufficient as a Feature Store input, or do you need a dedicated feature store (Feast, Hopsworks)? | Determines whether we need to add a feature store layer |
| 4 | Do you need raw millisecond-level data, or is 1-minute aggregation sufficient for model training? | Affects InfluxDB query strategy and Parquet file size |
| 5 | What is the expected retraining cadence? (daily / weekly / on-drift?) | Determines Airflow DAG scheduling and data retention policy |

---

## 5. Open Questions — Frontend Team

| # | Question | Why it matters |
|---|---|---|
| 1 | What is the required dashboard refresh rate? (real-time WebSocket, or polling every N seconds?) | Determines whether we need to add a WebSocket endpoint or SSE |
| 2 | What alert channels are needed? (WhatsApp, Email, Mobile Push, or all three?) | Determines alert service integration (Twilio, SendGrid, FCM) |
| 3 | What are the user-configurable alert thresholds? (or are they fixed per machine type?) | Determines whether thresholds need to be stored in MySQL and exposed via API |
| 4 | Do you need aggregated trend data (hourly/daily averages) or only raw readings? | Determines whether we need additional API endpoints for aggregated data |
| 5 | What is the maximum acceptable API response time for the history endpoint? | Determines whether we need pagination or data downsampling for long time ranges |
