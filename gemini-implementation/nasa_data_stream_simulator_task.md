# Task: NASA CMAPSS Data Stream Simulator

- [x] Create `code/generator/nasa_data_generator.py` structure and argument parser.
- [x] Implement NASA file reader to group cycles by Unit.
- [x] Implement data mapping and scaling logic from NASA sensors to KERRYSIGHT schema.
- [x] Implement parallel streaming loop for Units 1, 2, 3, and 4.
- [x] Add exception handling, logging, and connection to Kafka.
- [x] Verify execution by running the consumer and generator together.
- [x] Fix Airflow ETL transform task failure (KeyError: 'vibration_mean').
- [x] Verify successful execution of Airflow sensor_etl DAG run and Parquet output generation.
- [x] Create FastAPI endpoints for AI Engineers to easily list and download Parquet datasets.
- [x] Terminate duplicate background Windows processes and sync WSL/Docker clocks.
- [x] Purge Kafka queue backlog and wipe InfluxDB/MySQL to support clean data restreaming.
