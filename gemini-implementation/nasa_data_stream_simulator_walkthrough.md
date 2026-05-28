# Walkthrough: NASA CMAPSS Data Stream Simulator

We have successfully created and tested a new data generator script that streams NASA's CMAPSS turbofan engine degradation dataset to Kafka, mapping it to KERRYSIGHT's machine profiles.

## What Was Added

1. **New Script**: `code/generator/nasa_data_generator.py`
   * Parses the NASA raw data file `data/train_FD001.txt`.
   * Maps NASA Units 1, 2, 3, and 4 to KERRYSIGHT machines (`pump_01`, `mixer_01`, `spray_dryer_01`, and `compressor_01` respectively).
   * Automatically normalizes the raw values and scales them linearly to the normal ranges of each machine type.
   * As cycles advance towards failure, sensor values naturally exceed safe limits, trigger the `anomaly_flag`, and subsequently update the machine state in the MySQL database to `fault`.
   * Simulates a 10-cycle "maintenance cooldown" when a machine reaches its failure point, temporarily disabling data generation, setting the database status to `maintenance`, and resetting the cycle back to 1.
   * Includes a `--dry` flag to test the parser and scaling logs on any terminal without Kafka running.

## Verification & Testing

### Dry Run Test
Executed command:
```bash
.venv\Scripts\python.exe generator/nasa_data_generator.py --file data/train_FD001.txt --delay 0.5 --dry
```

### Results
* The script successfully parsed the NASA text data.
* Data was mapped and scaled correctly (e.g. `pump_01 (Cycle 1) | vib=3.824 temp=55.1 | OK`).
* Checked database status safely and fell back to dry-run when MySQL/Kafka were not running.
* Handled CP1252 terminal encoding correctly on Windows.

## Airflow ETL Troubleshooting & Success

### 1. The Issue
The `transform` task in the Airflow DAG `sensor_etl` originally failed with a `KeyError: 'Column not found: vibration_mean'` at line 128 of `sensor_etl_dag.py`. 

**Root Cause:**
In the original `sensor_etl_dag.py` code, the grouping and resampling was written as:
```python
sensor_df.groupby("machine_id")[sensor_fields].resample("1min").agg(["mean", "max", "min"])
```
This produced a DataFrame with multi-index columns that, when joined by `_`, yielded columns such as `vibration_mean_vibration` instead of the expected `vibration_mean` column, leading to a `KeyError` when referencing `vibration_mean`.

### 2. The Solution
We adjusted the aggregation logic to use `pd.Grouper` directly within the groupby keys:
```python
sensor_df.groupby(["machine_id", pd.Grouper(freq="1min")])[sensor_fields].agg(["mean", "max", "min"])
```
This produces a clean column multi-index that correctly flattens into `'vibration_mean'`, `'vibration_max'`, etc., and aligns with subsequent steps.

### 3. Verification & Execution
After updating the DAG definition, the Airflow scheduler executed the DAG successfully:
* **All tasks** (`extract_influxdb`, `extract_mysql`, `transform`, `load_parquet`) completed with status **Success** (Green).
* The transformed data was written to a Parquet file on the host at [sensor_2026-05-28_06.parquet](file:///C:/RAPAEL/PERSONAL_PROJECT/OPEN_AI_KERRY-GROUP/code/data/processed/sensor_2026-05-28_06.parquet).
