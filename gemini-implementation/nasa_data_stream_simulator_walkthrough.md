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
