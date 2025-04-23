import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Config
DATA_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
DOWNLOAD_DIR = "/tmp/aircraft_type_fuel_consumption_rates"
FILENAME = "aircraft_type_fuel_consumption_rates.json"


def download_fuel_consumption_rates(**context):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, FILENAME)
    if os.path.exists(file_path):
        return  # Idempotency: skip if already downloaded
    resp = requests.get(DATA_URL, timeout=30)
    resp.raise_for_status()
    with open(file_path, "wb") as f:
        f.write(resp.content)


def prepare_fuel_consumption_rates(**context):
    # Dummy prepare step: just touch a flag file (replace with real logic if needed)
    file_path = os.path.join(DOWNLOAD_DIR, FILENAME)
    prepared_flag = os.path.join(DOWNLOAD_DIR, "_prepared")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found. Run download first.")
    if os.path.exists(prepared_flag):
        return  # Idempotency
    with open(prepared_flag, "w") as f:
        f.write("prepared")

with DAG(
    dag_id="download_aircraft_type_fuel_consumption_rates",
    start_date=datetime(2023, 11, 1),
    schedule=None,  # Only run manually or trigger as needed
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    description="Download and prepare aircraft_type_fuel_consumption_rates.json from GitHub.",
    tags=["aircraft", "fuel", "download", "prepare"],
) as dag:
    download_task = PythonOperator(
        task_id="download_fuel_consumption_rates",
        python_callable=download_fuel_consumption_rates,
    )
    prepare_task = PythonOperator(
        task_id="prepare_fuel_consumption_rates",
        python_callable=prepare_fuel_consumption_rates,
    )
    download_task >> prepare_task
