import os
import psycopg2
from fastapi import FastAPI

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

app = FastAPI(title="Heat Monitoring API")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@app.get("/")
def root():
    return {"message": "API is running. Go to /docs"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/buildings")
def list_buildings():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT building_id, building_name, function FROM buildings ORDER BY building_id;")
    rows = cur.fetchall()
    cur.close(); conn.close()

    return [
        {"building_id": r[0], "building_name": r[1], "function": r[2]}
        for r in rows
    ]

@app.get("/buildings/status")
def building_status():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT building_id, building_name, window_start, window_end, avg_temperature, max_temperature
        FROM building_heat_summary
        ORDER BY window_start DESC
        LIMIT 50;
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()

    return [
        {
            "building_id": r[0],
            "building_name": r[1],
            "window_start": r[2],
            "window_end": r[3],
            "avg_temperature": float(r[4]) if r[4] is not None else None,
            "max_temperature": float(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]

@app.get("/alerts")
def alerts():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT building_id, building_name, window_start, window_end, avg_temperature, alert_level, created_at
        FROM building_heat_alert
        ORDER BY created_at DESC
        LIMIT 50;
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()

    return [
        {
            "building_id": r[0],
            "building_name": r[1],
            "window_start": r[2],
            "window_end": r[3],
            "avg_temperature": float(r[4]) if r[4] is not None else None,
            "alert_level": r[5],
            "created_at": r[6],
        }
        for r in rows
    ]

@app.get("/buildings/{building_id}")
def building_detail(building_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT window_start, window_end, avg_temperature, max_temperature
        FROM building_heat_summary
        WHERE building_id = %s
        ORDER BY window_start DESC
        LIMIT 20;
    """, (building_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    return {
        "building_id": building_id,
        "recent_windows": [
            {
                "window_start": r[0],
                "window_end": r[1],
                "avg_temperature": float(r[2]) if r[2] is not None else None,
                "max_temperature": float(r[3]) if r[3] is not None else None,
            }
            for r in rows
        ],
    }