# Bank Fraud Detection Pipeline

## Initial setup

1. `docker compose up -d`
2. Connect to Postgres DB and apply table schema from `db/schems.sql`.
    - Database name is in `docker-compose.yml`

## How to run

**IMPORTANT!!!** Order matter here.

1. `docker compose up -d` (if not running)
2. `uv run src/fraud_detector.py`
3. `uv run src/oltp_loader.py`
4. `uv run src/service_simulator.py` (This needs to run at last after all the setups)