# Plan: NASA CMAPSS Data Stream Simulator

This plan outlines the design and implementation of the NASA CMAPSS turbofan data stream simulator for Kafka.

## Target Architecture

```
NASA train_FD001.txt -> nasa_data_generator.py -> Kafka (sensor.raw)
                                                       |
                                                       v
                                            kafka_consumer.py -> InfluxDB & MySQL
```

## Proposed Changes

### Generator Component

#### [NEW] [nasa_data_generator.py](file:///C:/RAPAEL/PERSONAL_PROJECT/OPEN_AI_KERRY-GROUP/code/generator/nasa_data_generator.py)
A Python script that:
1. **Reads CMAPSS Data**: Parses space-separated sensor values from the training data (`data/train_FD001.txt`).
2. **Aligns Machines**: Maps Unit 1 to `pump_01`, Unit 2 to `mixer_01`, Unit 3 to `spray_dryer_01`, and Unit 4 to `compressor_01`.
3. **Applies Custom Scaling**:
   - Map NASA Sensor 15 (Bypass ratio) to **vibration** (scaled to threshold `> 10.0` as cycle count increases).
   - Map NASA Sensor 2 (LPC temperature) or Sensor 3 (HPC temperature) to **temperature** (scaled to threshold `> 90.0`).
   - Map NASA Sensor 9 (Core speed) to **rpm**.
   - Map NASA Sensor 11 (Static pressure) to **pressure** (scaled to threshold `> 12.0`).
   - Map NASA Sensor 12 (Fuel flow ratio) to **electric_current** (scaled to threshold `> 40.0`).
   - Map NASA Sensor 20/21 (Coolant bleed) to **flow_rate**.
4. **Calculates Anomaly Flag**: Dynamically evaluates the scaled values against the plant's anomaly thresholds:
   - `vibration > 10.0 mm/s`
   - `temperature > 90.0 °C`
   - `electric_current > 40.0 A`
   - `pressure > 12.0 bar`
5. **Streams in Parallel**:
   - Loops through the data cycles (1, 2, 3, ...) for Units 1, 2, 3, and 4 in parallel.
   - When a unit reaches its final cycle (failure point), it can simulate a "maintenance shutdown" (stops sending data, changes machine state in MySQL to `maintenance`/`fault` or resets after a brief delay).
6. **Configurable Speed**: Command-line flag `--delay` (seconds between cycles) to control how fast the data fires (e.g. 0.05 seconds for ultra-fast firing).

---

## Verification Plan

### Execution
We will run the simulator using python:
```bash
python code/generator/nasa_data_generator.py --file code/data/train_FD001.txt --delay 0.1
```

### Manual Verification
1. Verify that messages are successfully published to Kafka topic `sensor.raw`.
2. Run the Kafka consumer (`python consumer/kafka_consumer.py`) and check if the messages are correctly parsed, saved to InfluxDB, and if anomalies trigger MySQL logging.
