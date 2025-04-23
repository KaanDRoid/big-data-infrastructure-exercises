import json
import os
from datetime import datetime, timedelta

import boto3
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

# DAG config
START_DATE = datetime(2023, 11, 1)
END_DATE = datetime(2024, 11, 1)
DATA_URL = "https://samples.adsbexchange.com/readsb-hist"
DOWNLOAD_DIR = "/tmp/readsb_hist_data"
S3_BUCKET = "bdi-aircraft-kaan"
s3_client = boto3.client("s3")

DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 5432))
DB_NAME = os.getenv("PG_DB", "aircraft")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "postgres")

# Only run for the 1st day of each month
def is_first_of_month(execution_date):
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date)
    return execution_date.day == 1

def upload_to_s3(local_path, s3_key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return  # idempotency
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)

def download_files(execution_date, **context):
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date)
    if not is_first_of_month(execution_date):
        return
    date_str = execution_date.strftime("%Y-%m-%d")
    day_dir = os.path.join(DOWNLOAD_DIR, date_str)
    os.makedirs(day_dir, exist_ok=True)
    s3_raw_prefix = f"raw/day={date_str.replace('-', '')}/"
    for i in range(1, 101):
        filename = f"readsb_{date_str}_{i:03d}.json.gz"
        file_path = os.path.join(day_dir, filename)
        s3_key = s3_raw_prefix + filename
        if os.path.exists(file_path):
            upload_to_s3(file_path, s3_key)
            continue  # Idempotency: skip if already downloaded
        url = f"{DATA_URL}/{date_str}/{filename}"
        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            upload_to_s3(file_path, s3_key)
        else:
            # Optionally log missing files
            pass

def insert_prepared_to_postgres(prepared_dir, execution_date):
    date_str = execution_date.strftime("%Y%m%d")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_positions (
            icao TEXT,
            timestamp BIGINT,
            lat FLOAT,
            lon FLOAT,
            altitude FLOAT,
            speed FLOAT,
            emergency BOOLEAN,
            day TEXT
        )
    """)
    for file in os.listdir(prepared_dir):
        if file.endswith(".prepared"):
            file_path = os.path.join(prepared_dir, file)
            with open(file_path, encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception:
                    continue
                for record in data if isinstance(data, list) else [data]:
                    cur.execute(
                        """
                        INSERT INTO aircraft_positions (icao, timestamp, lat, lon, altitude, speed, emergency, day)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            record.get("icao"),
                            record.get("timestamp"),
                            record.get("lat"),
                            record.get("lon"),
                            record.get("altitude"),
                            record.get("speed"),
                            record.get("emergency"),
                            date_str,
                        ),
                    )
    conn.commit()
    cur.close()
    conn.close()

def prepare_files(execution_date, **context):
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date)
    if not is_first_of_month(execution_date):
        return
    date_str = execution_date.strftime("%Y-%m-%d")
    day_dir = os.path.join(DOWNLOAD_DIR, date_str)
    prepared_flag = os.path.join(day_dir, "_prepared")
    s3_prepared_prefix = f"prepared/day={date_str.replace('-', '')}/"
    if os.path.exists(prepared_flag):
        return  # Idempotency: already prepared
    # Dummy prepare step
    for file in os.listdir(day_dir):
        if file.endswith(".json.gz"):
            local_path = os.path.join(day_dir, file)
            prepared_local_path = os.path.join(day_dir, file.replace(".json.gz", ".prepared"))
            with open(local_path, "rb") as src, open(prepared_local_path, "wb") as dst:
                dst.write(src.read())  # Gerçek işleme burada yapılabilir
            s3_key = s3_prepared_prefix + os.path.basename(prepared_local_path)
            upload_to_s3(prepared_local_path, s3_key)
    # Prepare the data for PostgreSQL
    insert_prepared_to_postgres(day_dir, execution_date)
    with open(prepared_flag, "w") as f:
        f.write("prepared")

with DAG(
    dag_id="readsb_hist_download_prepare",
    start_date=START_DATE,
    end_date=END_DATE,
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Download and prepare readsb-hist data for the 1st day of each month.",
    tags=["readsb", "hist", "download", "prepare"],
) as dag:
    download_task = PythonOperator(
        task_id="download_readsb_hist",
        python_callable=download_files,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    prepare_task = PythonOperator(
        task_id="prepare_readsb_hist",
        python_callable=prepare_files,
        op_kwargs={"execution_date": "{{ ds }}"},
    )
    download_task >> prepare_task
