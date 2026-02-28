import os
import json
from kafka import KafkaConsumer
import psycopg2

TOPIC = "temperature_raw"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is missing. Set it to your Neon connection string.")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],  # Python runs on host; Kafka exposed to localhost
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="heat-consumer-final",
    value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
)

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True

INSERT_SENSOR_SQL = """
INSERT INTO sensor_points (sensor_id, temperature, humidity, recorded_at, geom)
VALUES (%s, %s, %s, %s, ST_SetSRID(ST_Point(%s, %s), 4326))
RETURNING id;
"""

print(f"Listening on topic: {TOPIC}")

with conn.cursor() as cur:
    for msg in consumer:
        raw = msg.value

        # Skip empty messages
        if not raw or not raw.strip():
            print("Skip empty message")
            continue

        # Parse JSON safely
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            print("Skip non-JSON message:", repr(raw[:100]))
            continue

        # Validate required fields
        try:
            sensor_id = data["sensor_id"]
            ts = data["timestamp"]          # ISO string; Postgres parses (TIMESTAMPTZ recommended)
            lat = float(data["latitude"])
            lon = float(data["longitude"])
            temp = float(data["temperature"])
            hum = float(data["humidity"])
        except KeyError as e:
            print("Missing key:", e)
            continue
        except (TypeError, ValueError) as e:
            print("Bad value:", e)
            continue

        # 1) Insert raw sensor point
        cur.execute(INSERT_SENSOR_SQL, (sensor_id, temp, hum, ts, lon, lat))
        sensor_point_id = cur.fetchone()[0]

        # 2) Spatial enrichment in DB (inserts into sensor_enriched_events if point is within a building polygon)
        cur.execute("SELECT enrich_sensor_point(%s);", (sensor_point_id,))

        # 3) Fetch enrichment result (building_id + event time)
        cur.execute(
            """
            SELECT building_id, recorded_at
            FROM sensor_enriched_events
            WHERE sensor_point_id = %s
            """,
            (sensor_point_id,),
        )
        enriched = cur.fetchone()
        if not enriched:
            print(f"Inserted but no building match | sensor={sensor_id} point_id={sensor_point_id}")
            continue

        building_id, recorded_at = enriched

        # 4) Update 10-minute window summary (UPSERT)
        cur.execute(
        "SELECT update_building_window_summary(%s, %s::timestamp);",
        (building_id, recorded_at)
        )

        # 5) Get the latest window_start from the summary (most reliable; avoids mismatched window calculations)
        cur.execute(
            """
            SELECT window_start
            FROM building_heat_summary
            WHERE building_id = %s
            ORDER BY window_start DESC
            LIMIT 1
            """,
            (building_id,),
        )
        row = cur.fetchone()
        if row:
            window_start = row[0]

            # 6) Check/Upsert alert (anti-spam: 1 alert per building per window)
            # Requires DB function: check_building_heat_alert(building_id, window_start, threshold)
            cur.execute(
                "SELECT check_building_heat_alert(%s, %s, %s);",
                (building_id, window_start, 35),
            )

        print(f"Processed | sensor={sensor_id} building={building_id} point_id={sensor_point_id}")