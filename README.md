# Kerry Group — Predictive Maintenance Data Pipeline (PoC)

Local data engineering environment for predictive maintenance of industrial equipment (pumps, mixers, spray dryers, compressors) at Kerry Group.

## Architecture

```
Mock Generator → Kafka → InfluxDB (TSDB, hot path)
                       → MySQL   (ERP/MES, events)
                            ↓
                        Airflow ETL (hourly batch)
                            ↓
                    Parquet (ML team) + FastAPI (Frontend)
```

## Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka |
| Time-Series DB | InfluxDB 2.7 |
| Relational DB | MySQL 8.0 |
| Scheduler / ETL | Apache Airflow 2.9 + Pandas |
| REST API | FastAPI |

## Quick Start

**1. Copy and configure environment:**
```bash
cp .env.example .env   # edit credentials if needed
```

**2. Start all containers:**
```bash
docker-compose up -d
```

**3. Install Python dependencies:**
```bash
pip install -r requirements.txt
```

**4. Run mock data generator:**
```bash
python generator/mock_data_generator.py
```

**5. Run Kafka consumer (separate terminal):**
```bash
python consumer/kafka_consumer.py
```

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| InfluxDB UI | http://localhost:8086 | see `.env` |
| Airflow UI | http://localhost:8080 | admin / admin |
| FastAPI Swagger | http://localhost:8000/docs | — |
| MySQL | localhost:3306 | see `.env` |

## Project Structure

```
├── docker-compose.yml       # All services
├── .env                     # Credentials (not committed)
├── init/mysql_init.sql      # DB schema + seed data
├── generator/               # Mock sensor data publisher
├── consumer/                # Kafka → InfluxDB + MySQL writer
├── etl/dags/                # Airflow DAG (hourly ETL → Parquet)
├── api/                     # FastAPI REST endpoints
├── data/processed/          # Parquet output (gitignored)
└── DATA_CONTRACT.md         # Schema docs for ML & Frontend teams
```

## Simulated Machines

| ID | Name | Type |
|---|---|---|
| `pump_01` | Feed Pump #1 | Pump |
| `mixer_01` | Ingredient Mixer #1 | Mixer |
| `spray_dryer_01` | Spray Dryer #1 | Spray Dryer |
| `compressor_01` | Air Compressor #1 | Compressor |

## Sensor Parameters

`vibration (mm/s)` · `temperature (°C)` · `electric_current (A)` · `pressure (bar)` · `rpm` · `flow_rate (L/min)`

10% of readings include anomaly spikes for realistic simulation.
