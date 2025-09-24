#!/usr/bin/env python3
"""
Ingest Melbourne Microclimate Sensor Data into PostgreSQL
--------------------------------------------------------
Fetches data from the City of Melbourne's open API and loads it into
a PostgreSQL table for analysis of temperature and humidity trends.
"""

import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values

API_URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/microclimate-sensors-data/records"

def fetch_data(limit=10000):
    """Fetch records from the Melbourne Microclimate Sensors dataset API."""
    records = []
    offset = 0
    batch_size = 100
    while len(records) < limit:
        params = {
            "limit": batch_size,
            "offset": offset,
        }
        resp = requests.get(API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("results", [])
        if not rows:
            break
        records.extend(rows)
        offset += batch_size
    return records[:limit]


def init_db(conn):
    """Create table if it does not exist"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS microclimateapi (
                id SERIAL PRIMARY KEY,
                device_id TEXT,
                received_at TIMESTAMP,
                sensorlocation TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                minimumwinddirection DOUBLE PRECISION,
                averagewinddirection DOUBLE PRECISION,
                maximumwinddirection DOUBLE PRECISION,
                minimumwindspeed DOUBLE PRECISION,
                averagewindspeed DOUBLE PRECISION,
                gustwindspeed DOUBLE PRECISION,
                airtemperature DOUBLE PRECISION,
                relativehumidity DOUBLE PRECISION,
                atmosphericpressure DOUBLE PRECISION,
                pm25 DOUBLE PRECISION,
                pm10 DOUBLE PRECISION,
                noise DOUBLE PRECISION
            );
        """)
        conn.commit()


def insert_data(conn, records):
    """Insert fetched records into PostgreSQL"""
    rows = []
    for r in records:
        latlong = r.get("latlong") or {}  # if None, use empty dict
        rows.append((
            r .get("device_id"),
            r.get("received_at"),
            r.get("sensorlocation"),
            latlong.get("lat"),
            latlong.get("lon"),
            r.get("minimumwinddirection"),
            r.get("averagewinddirection"),
            r.get("maximumwinddirection"),
            r.get("minimumwindspeed"),
            r.get("averagewindspeed"),
            r.get("gustwindspeed"),
            r.get("airtemperature"),
            r.get("relativehumidity"),
            r.get("atmosphericpressure"),
            r.get("pm25"),
            r.get("pm10"),
            r.get("noise")
            ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO microclimateapi
            (device_id, received_at, sensorlocation, latitude, longitude, minimumwinddirection, averagewinddirection, maximumwinddirection,minimumwindspeed,averagewindspeed,gustwindspeed, airtemperature,relativehumidity,atmosphericpressure,pm25,pm10,noise)
            VALUES %s
        """, rows)
        conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Load Melbourne Microclimate Sensor Data into PostgreSQL")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", default=5433, type=int, help="PostgreSQL port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--limit", default=10000, type=int, help="Number of records to fetch (min 10,000)")
    args = parser.parse_args()
 
    print(f"Fetching {args.limit} records from Melbourne Microclimate API...")
    records = fetch_data(limit=args.limit)

    if len(records) < 10000:
        raise RuntimeError("Not enough records fetched. Try increasing --limit or check API availability.")

    print(f"Fetched {len(records)} records.")

    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password
    )

    init_db(conn)
    insert_data(conn, records)

    conn.close()
    print("✅ Data successfully loaded into PostgreSQL.")


if __name__ == "__main__":
    main()
