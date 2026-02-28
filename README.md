# Real-Time Geospatial Heat Monitoring Pipeline

## Overview

This project implements a real-time streaming pipeline that integrates IoT sensor data, geospatial processing, and time-based aggregation.

Temperature data from sensors is streamed via Kafka, spatially enriched using PostGIS, aggregated in 10-minute event-time windows, and evaluated against a business threshold to generate alerts for overheating buildings.

The system demonstrates the integration of streaming ingestion, geospatial transformation, and stateful window-based processing within a unified architecture.

---

## Architecture

### Ingest Layer
IoT Sensors → Kafka ('temperature_raw' topic)

Real-time temperature events are continuously published to a Kafka topic.

### Processing Layer
Python Kafka Consumer → PostGIS Spatial Join → 10-Minute Window Aggregation

- JSON deserialization
- Coordinate transformation into geometry ('ST_Point')
- Spatial join with building polygons ('ST_Within')
- Event-time window aggregation (10-minute fixed window)
- Stateful UPSERT into summary table

### Output Layer
- 'sensor_enriched_events'
- 'building_heat_summary'
- 'building_heat_alert'

Alerts are generated when the average temperature exceeds 35°C.

---

## Tech Stack

- Apache Kafka
- Python (Kafka Consumer)
- PostgreSQL (NeonDB)
- PostGIS
- FastAPI (optional API layer)
- Docker (local Kafka setup)

---

## Database Schema

Main tables:

- 'buildings'
- 'sensor_points'
- 'sensor_enriched_events'
- 'building_heat_summary'
- 'building_heat_alert'

Functions:

- enrich_sensor_point()
- update_building_window_summary()
- check_building_heat_alert()

See 'schema.sql' for full definition.

---

## How to Run

### 1. Start Kafka (Docker)

--- # bash
make kafka-up

### 2. Set Database URL
--- # bash
export DATABASE_URL="postgresql://neondb_owner:npg_jt6hLdEBJ2UG@ep-green-boat-a1pi7z4d-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

### 3. Run Consumer
--- # bash
python kafka/script/consumer_to_postgis.py

### 4. (optional) Run API
uvicorn api:app --host 0.0.0.0 --port 8000
--- notes: due to no public URL from firebase, attached the result in sample_alerts_response.json when curl the API.



## Business Logic
- Aggregation uses event-time processing
- Fixed window durationL 10 minutes
- Threshold : 35°C average temperature
- Alert level: HIGH_HEAT


## Key Features Demonstrated
- Real-time Kafka ingestion
- Geospatial enrinchment using POSTGIS
- Event-time window aggregation
- Stateful streaming via UPSERT
- Threshold-based alert generation


## PROJECT STRUCTURE
finalproject/
├── api/
│   └── api.py
├── docs/
│   └── sample_alerts_response.json
├── kafka/
│   ├── script/
│   │   ├── consumer.py
│   │   └── consumer_to_postgis.py
│   └── seed_events.jsonl
├── docker-compose.yaml
├── README.md
├── schema.sql
└── dev.nix




#### Author
Irma Prameshwari
Senior GIS Specialist -  Esri Indonesia
