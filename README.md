# ISE 589 Lab 3C – Conveyor Manufacturing Dashboard

Streamlit dashboard for the Lab 2 conveyor (Micro850 + Factory I/O).

**Pipeline:** Micro850 PLC → Modbus TCP → edge gateway (`lab3a.py`) → MQTT (`test.mosquitto.org`)
→ PostgreSQL on Render (`sensor_data`) → this Streamlit app.

## Features
- **Live status (MQTT):** subscribes to `FWH2311/+/status`; RUNNING/STOPPED, system enable,
  live package count, part-at-entry, message age (refreshes every 2 s, shows NO DATA when stale).
- **Historical data (PostgreSQL):** station and time-window filters; package count, rolling
  throughput, machine state; counter trend; parcels-per-minute bar chart (`throughput_per_minute` view);
  raw data table with CSV download.
- Handles empty time ranges and dropped database connections without crashing.

## Data model
`public.sensor_data(event_id, event_time UTC, ingest_time, asset_id, item_ready, at_entry, at_exit,
machine_running, entry_conveyor_status, exit_conveyor_status, counter_value, system_enable)`
and view `public.throughput_per_minute(asset_id, minute, parcels_per_minute)`.

## Run locally
```
pip install -r requirements.txt
copy .streamlit\secrets.toml.example .streamlit\secrets.toml   # then edit DATABASE_URL
streamlit run manufacturing_app.py
```

## Deploy
Streamlit Community Cloud → Create app → this repo, branch `main`, file `manufacturing_app.py`
→ App settings → Secrets (same lines as `secrets.toml.example`).
