import gzip
import json
import logging
from io import BytesIO

import boto3
import psycopg2
from fastapi import APIRouter, HTTPException, status
from psycopg2.extras import execute_batch

from bdi_api.settings import DBCredentials, Settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load settings and database credentials
settings = Settings()
db_credentials = DBCredentials()

# FastAPI Router
s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

# S3 Client (Credentials are loaded from environment variables)
s3_client = boto3.client("s3")

# Database Connection Helper
def get_db_connection():
    """Establish a connection to the PostgreSQL database using credentials."""
    try:
        return psycopg2.connect(
            dbname=db_credentials.dbname,
            user=db_credentials.username,
            password=db_credentials.password,
            host=db_credentials.host,
            port=db_credentials.port
        )
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection failed") from e

# Fetch Data from S3 and Write to PostgreSQL
@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Fetch raw data from S3 and insert it into PostgreSQL RDS."""

    bucket_name = settings.s3_bucket
    prefix = "prepared/day=20231101/"

    # List objects in S3 bucket
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = response.get('Contents', [])
        logger.info(f"Bucket: {bucket_name}, Prefix: {prefix}, Found files: {len(files)}")
    except Exception as e:
        logger.error(f"Failed to list objects in S3 bucket: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to access S3 bucket: {str(e)}") from e

    if not files:
        logger.warning(f"No files found in S3 bucket: {bucket_name} with prefix: {prefix}")
        raise HTTPException(status_code=404, detail="No files found in S3 bucket")

    # Connect to the database
    with get_db_connection() as conn, conn.cursor() as cur:
        # Create tables if they don't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR(6) PRIMARY KEY,
                registration VARCHAR(10),
                type VARCHAR(10)
            );

            CREATE TABLE IF NOT EXISTS positions (
                id SERIAL PRIMARY KEY,
                icao VARCHAR(6) REFERENCES aircraft(icao),
                timestamp FLOAT,
                lat FLOAT,
                lon FLOAT,
                altitude_baro INTEGER,
                ground_speed INTEGER,
                emergency BOOLEAN
            );
        """)

        # Add indexes for performance optimization
        cur.execute("CREATE INDEX IF NOT EXISTS idx_icao ON positions(icao);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON positions(timestamp);")

        # Process and insert data
        aircraft_data_list = []
        positions_data_list = []

        for obj in files:
            try:
                # Get the object from S3
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                # Check if the file is gzip compressed (ends with .gz)
                if obj['Key'].endswith('.gz'):
                    # Decompress the gzip file
                    compressed_data = s3_object['Body'].read()
                    with gzip.GzipFile(fileobj=BytesIO(compressed_data), mode='rb') as gz:
                        data = json.loads(gz.read().decode('utf-8'))
                else:
                    # If not gzip, read directly as JSON
                    data = json.loads(s3_object['Body'].read().decode('utf-8'))
            except Exception as e:
                logger.error(f"Failed to read or parse S3 object {obj['Key']}: {str(e)}")
                continue

            for aircraft in data.get('aircraft', []):
                aircraft_data_list.append((
                    aircraft['icao'],
                    aircraft.get('registration', None),
                    aircraft.get('type', None)
                ))

                for pos in aircraft.get('positions', []):
                    positions_data_list.append((
                        aircraft['icao'],
                        pos['timestamp'],
                        pos['lat'],
                        pos['lon'],
                        pos.get('altitude_baro', None),
                        pos.get('ground_speed', None),
                        pos.get('emergency', False)
                    ))

        # Batch insert data into the database
        execute_batch(cur, """
            INSERT INTO aircraft (icao, registration, type)
            VALUES (%s, %s, %s)
            ON CONFLICT (icao) DO UPDATE SET
                registration = EXCLUDED.registration,
                type = EXCLUDED.type
        """, aircraft_data_list)

        execute_batch(cur, """
            INSERT INTO positions (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, positions_data_list)

        conn.commit()
        logger.info(
            f"Inserted {len(aircraft_data_list)} aircraft and {len(positions_data_list)} positions into the database"
        )

    return "Data successfully loaded into RDS!"

# List Aircraft
@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List aircraft with pagination."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT icao, registration, type
            FROM aircraft
            ORDER BY icao ASC
            LIMIT %s OFFSET %s
        """, (num_results, page * num_results))
        return [{"icao": r[0], "registration": r[1], "type": r[2]} for r in cur.fetchall()]

# List Aircraft Positions
@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """List positions for a specific aircraft with pagination."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, lat, lon
            FROM positions
            WHERE icao = %s
            ORDER BY timestamp ASC
            LIMIT %s OFFSET %s
        """, (icao, num_results, page * num_results))
        return [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in cur.fetchall()]

# Get Aircraft Statistics
@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Get statistics for a specific aircraft."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                MAX(altitude_baro) as max_altitude_baro,
                MAX(ground_speed) as max_ground_speed,
                BOOL_OR(emergency) as had_emergency
            FROM positions
            WHERE icao = %s
        """, (icao,))
        result = cur.fetchone()
        return {
            "max_altitude_baro": result[0] or 0,
            "max_ground_speed": result[1] or 0,
            "had_emergency": result[2] or False
        }
