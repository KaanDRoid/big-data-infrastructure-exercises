import json
import os
<<<<<<< HEAD
import shutil
import boto3  # AWS SDK for Python
import requests
from datetime import datetime, timedelta
from typing import Annotated
=======
import requests
import json
import logging
import pandas as pd
from tqdm import tqdm
from typing import Annotated
from bs4 import BeautifulSoup
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc
from fastapi import APIRouter, status
from fastapi.params import Query
from urllib.parse import urljoin
from bdi_api.settings import Settings

# Initialize settings
settings = Settings()
RAW_DATA_PATH = os.path.join(settings.raw_dir, "day=20231101")
PREPARED_DATA_PATH = os.path.join(settings.prepared_dir, "day=20231101")

# Initialize AWS S3 client
S3_BUCKET_NAME = "bdi-aircraft-kaan"  # Replace with your actual S3 bucket
s3_client = boto3.client("s3")

# FastAPI router setup
s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

<<<<<<< HEAD

def clean_folder(path: str):
    """Cleans the specified folder by removing all its contents."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def download_gzip(url, save_path):
    """Downloads a GZIP file, saves it locally, and uploads it to S3."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            # Save locally
            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {url} -> {save_path}")

            # Upload to S3
            s3_key = os.path.relpath(save_path, settings.raw_dir)  # Relative S3 path
            s3_key = s3_key.replace("\\", "/")  # Ensure correct S3 path format

            s3_client.upload_file(save_path, S3_BUCKET_NAME, s3_key)
            print(f"Uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")

        else:
            print(f"Failed to download {url} (Status: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


=======
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc
@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
<<<<<<< HEAD
            ..., description="""Limits the number of files to download.""",
=======
            ...,
            description="Limits the number of files to download. I'll test with increasing number of files starting from 100.",
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc
        ),
    ] = 1000,
) -> str:
<<<<<<< HEAD
    clean_folder(RAW_DATA_PATH)

    base_url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
    current_time = datetime.strptime("000000", "%H%M%S")

    for _ in range(file_limit):
        filename = current_time.strftime("%H%M%SZ.json.gz")
        file_url = base_url + filename
        save_path = os.path.join(RAW_DATA_PATH, filename)
        download_gzip(file_url, save_path)

        # Increment by 5 seconds
        current_time += timedelta(seconds=5)
        if current_time.second == 60:
            current_time = current_time.replace(second=0)

    return f"Downloaded {file_limit} files to {RAW_DATA_PATH} and uploaded to S3 bucket {S3_BUCKET_NAME}"
=======
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

    os.makedirs(download_dir, exist_ok=True)
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc

    # Clean download folder
    for file in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    try:
        response = requests.get(base_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        files = [
            a["href"] for a in soup.find_all("a")
            if a["href"].endswith(".json.gz")
        ][:file_limit]

        downloaded_count = 0
        for file_name in tqdm(files, desc="Downloading files"):
            file_url = urljoin(base_url, file_name)
            response = requests.get(file_url, stream=True)
            if response.status_code == 200:
                file_path = os.path.join(download_dir, file_name[:-3])
                with open(file_path, "wb") as f:
                    f.write(response.content)
                downloaded_count += 1
            else:
                logging.warning(f"Failed to download {file_name}")

        return f"Downloaded {downloaded_count} files to {download_dir}"

    except requests.RequestException as e:
        logging.error(f"Error accessing URL: {str(e)}")
        return f"Error accessing URL: {str(e)}"
    except Exception as e:
        logging.error(f"Error during download: {str(e)}")
        return f"Error during download: {str(e)}"

@s1.post("/aircraft/prepare")
def prepare_data() -> str:
<<<<<<< HEAD
    clean_folder(PREPARED_DATA_PATH)

    for file_name in os.listdir(RAW_DATA_PATH):
        print(file_name)
        raw_file_path = os.path.join(RAW_DATA_PATH, file_name)
        prepared_file_path = os.path.join(PREPARED_DATA_PATH, file_name.replace(".gz", ""))

        with open(raw_file_path, encoding="utf-8") as raw_file:
            data = json.load(raw_file)
            timestamp = data["now"]
            aircraft_data = data["aircraft"]
            processed_data = []

            for record in aircraft_data:
                processed_data.append({
                    "icao": record.get("hex", None),
                    "registration": record.get("r", None),
                    "type": record.get("t", None),
                    "lat": record.get("lat", None),
                    "lon": record.get("lon", None),
                    "alt_baro": record.get("alt_baro", None),
                    "timestamp": timestamp,
                    "max_altitude_baro": record.get("alt_baro", None),
                    "max_ground_speed": record.get("gs", None),
                    "had_emergency": record.get("alert", 0) == 1
                })

            with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
                json.dump(processed_data, prepared_file)
            print("Process finished")

    return f"Prepared data saved to {PREPARED_DATA_PATH}"
=======
    raw_folder = os.path.join(settings.raw_dir, "day=20231101")
    prepared_folder = os.path.join(settings.prepared_dir, "day=20231101")

    os.makedirs(prepared_folder, exist_ok=True)

    # Clean prepared folder
    for file in os.listdir(prepared_folder):
        file_path = os.path.join(prepared_folder, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    processed_count = 0
    skipped_count = 0

    for file_name in os.listdir(raw_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(raw_folder, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    file_data = json.load(f)

                if "aircraft" not in file_data:
                    logging.warning(f"'aircraft' key missing in {file_name}")
                    skipped_count += 1
                    continue
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc

                aircraft_data = [
                    {
                        "icao": aircraft.get("hex"),
                        "registration": aircraft.get("r"),
                        "type": aircraft.get("t"),
                        "latitude": aircraft.get("lat"),
                        "longitude": aircraft.get("lon"),
                        "timestamp": file_data.get("now"),
                    }
                    for aircraft in file_data["aircraft"]
                ]

                if not aircraft_data:
                    logging.warning(f"No aircraft data in {file_name}")
                    skipped_count += 1
                    continue

                df = pd.DataFrame(aircraft_data)
                df = df.dropna(subset=["icao", "registration", "type", "latitude", "longitude", "timestamp"])

                output_csv = os.path.join(prepared_folder, f"{os.path.splitext(file_name)[0]}.csv")
                df.to_csv(output_csv, index=False)
                processed_count += 1

            except json.JSONDecodeError:
                logging.error(f"Invalid JSON in file {file_name}")
                skipped_count += 1
            except Exception as e:
                logging.error(f"Failed to process {file_name}: {e}")
                skipped_count += 1

    return f"Prepared data in {prepared_folder}. Processed: {processed_count}, Skipped: {skipped_count}"

@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
<<<<<<< HEAD
    aircraft = set()

    for file_name in os.listdir(PREPARED_DATA_PATH):
        with open(os.path.join(PREPARED_DATA_PATH, file_name), encoding="utf-8") as file:
            data = json.load(file)
            aircraft.update(
                (record["icao"], record.get("registration", "Unknown"), record.get("type", "Unknown"))
                for record in data if record["icao"]
            )

    aircraft_list = sorted(aircraft, key=lambda x: x[0])
    start = page * num_results
    end = start + num_results

    return [
        {"icao": entry[0], "registration": entry[1], "type": entry[2]}
        for entry in aircraft_list[start:end]
    ]
=======
    prepared_folder = os.path.join(settings.prepared_dir, "day=20231101")
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc

    if not os.path.exists(prepared_folder):
        logging.warning(f"Prepared folder {prepared_folder} does not exist.")
        return []

    csv_files = [file for file in os.listdir(prepared_folder) if file.endswith(".csv")]

    if page >= len(csv_files):
        logging.warning(f"Page {page} does not exist. Total available files: {len(csv_files)}.")
        return []

    file_path = os.path.join(prepared_folder, csv_files[page])
    try:
        df = pd.read_csv(file_path)
        if not {"icao", "registration", "type", "latitude", "longitude", "timestamp"}.issubset(df.columns):
            logging.warning(f"CSV file {csv_files[page]} is missing required columns.")
            return []

        df.sort_values(by=["icao", "timestamp"], inplace=True)
        result_data = df.head(num_results)

        return result_data.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Failed to read file {csv_files[page]}: {e}")
        return []

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
<<<<<<< HEAD
    positions = []

    for file_name in os.listdir(PREPARED_DATA_PATH):
        with open(os.path.join(PREPARED_DATA_PATH, file_name), encoding="utf-8") as file:
            data = json.load(file)
            positions.extend(
                {
                    "timestamp": record.get("timestamp"),
                    "lat": record.get("lat"),
                    "lon": record.get("lon"),
                }
                for record in data if record["icao"] == icao
            )

    positions = sorted(positions, key=lambda x: x["timestamp"])
    start = page * num_results
    end = start + num_results
    return positions[start:end]
=======
    prepared_folder = os.path.join(settings.prepared_dir, "day=20231101")
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc

    if not os.path.exists(prepared_folder):
        logging.warning(f"Prepared folder {prepared_folder} does not exist.")
        return []

    positions = []

    for file in os.listdir(prepared_folder):
        if file.endswith(".csv"):
            file_path = os.path.join(prepared_folder, file)
            try:
                df = pd.read_csv(file_path)
                required_columns = {"icao", "timestamp", "latitude", "longitude"}
                if not required_columns.issubset(df.columns):
                    logging.warning(f"File {file} is missing required columns.")
                    continue

                filtered_rows = df[df["icao"] == icao]
                for _, row in filtered_rows.iterrows():
                    positions.append({
                        "timestamp": row["timestamp"],
                        "lat": row["latitude"],
                        "lon": row["longitude"],
                    })

            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")

    if not positions:
        logging.info(f"No positions found for ICAO {icao} in any file.")
        return []

    positions.sort(key=lambda x: x["timestamp"])
    start_index = page * num_results
    end_index = start_index + num_results
    return positions[start_index:end_index]

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
<<<<<<< HEAD
    alt = 0
    speed = 0
    emergency = False
    data_found = False

    for file_name in os.listdir(PREPARED_DATA_PATH):
        file_path = os.path.join(PREPARED_DATA_PATH, file_name)

        try:
            with open(file_path, encoding="utf-8") as file:
                data = json.load(file)

                for record in data:
                    if record.get("icao", "").lower() == icao.lower():
                        data_found = True
                        alt = max(alt, record.get("max_altitude_baro", 0))
                        speed = max(speed, record.get("max_ground_speed", 0))
                        emergency = emergency or record.get("had_emergency", False)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {file_name}: {e}")
        except Exception as e:
            print(f"Unexpected error reading {file_name}: {e}")

    if not data_found:
        return {"error": f"No data found for ICAO {icao}"}

    return {
        "max_altitude_baro": alt,
        "max_ground_speed": speed,
        "had_emergency": emergency,
=======
    prepared_folder = os.path.join(settings.prepared_dir, "day=20231101")

    if not os.path.exists(prepared_folder):
        logging.warning(f"Prepared folder {prepared_folder} does not exist.")
        return {
            "max_altitude_baro": None,
            "max_ground_speed": None,
            "had_emergency": None
        }

    max_altitude_baro = None
    max_ground_speed = None
    had_emergency = False

    for file in os.listdir(prepared_folder):
        if file.endswith(".csv"):
            file_path = os.path.join(prepared_folder, file)
            try:
                df = pd.read_csv(file_path)
                required_columns = {"icao", "alt_baro", "gs", "emergency"}
                if not required_columns.issubset(df.columns):
                    logging.warning(f"File {file} is missing required columns: {required_columns - set(df.columns)}")
                    continue

                filtered_rows = df[df["icao"] == icao]
                if not filtered_rows.empty:
                    max_altitude_baro = max(max_altitude_baro or 0, filtered_rows["alt_baro"].max())
                    max_ground_speed = max(max_ground_speed or 0, filtered_rows["gs"].max())
                    had_emergency = had_emergency or filtered_rows["emergency"].any()

            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")

    return {
        "max_altitude_baro": max_altitude_baro,
        "max_ground_speed": max_ground_speed,
        "had_emergency": had_emergency
>>>>>>> 5b2675403b8f603eb486d863867f7f3a74ceaebc
    }
