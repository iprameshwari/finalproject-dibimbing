-- =========================================
-- Real-Time Building Heat Monitoring Schema
-- Postgres + PostGIS (NeonDB)
-- =========================================

-- 0) Extensions
CREATE EXTENSION IF NOT EXISTS postgis;

-- 1) Buildings (static polygon reference)
CREATE TABLE IF NOT EXISTS buildings (
  building_id   SERIAL PRIMARY KEY,
  building_name TEXT NOT NULL,
  function      TEXT,
  geom          geometry(Polygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_buildings_geom
  ON buildings
  USING GIST (geom);

-- 2) Raw sensor points (streaming ingestion output)
CREATE TABLE IF NOT EXISTS sensor_points (
  id           BIGSERIAL PRIMARY KEY,
  sensor_id    TEXT NOT NULL,
  temperature  DOUBLE PRECISION NOT NULL,
  humidity     DOUBLE PRECISION,
  recorded_at  TIMESTAMPTZ NOT NULL,
  geom         geometry(Point, 4326) NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_points_geom
  ON sensor_points
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_sensor_points_recorded_at
  ON sensor_points (recorded_at);

-- 3) Enriched events (point mapped to building polygon)
CREATE TABLE IF NOT EXISTS sensor_enriched_events (
  id              BIGSERIAL PRIMARY KEY,
  sensor_point_id BIGINT NOT NULL REFERENCES sensor_points(id) ON DELETE CASCADE,
  building_id     INT NOT NULL REFERENCES buildings(building_id) ON DELETE CASCADE,
  sensor_id       TEXT NOT NULL,
  temperature     DOUBLE PRECISION NOT NULL,
  humidity        DOUBLE PRECISION,
  recorded_at     TIMESTAMPTZ NOT NULL,
  point_geom      geometry(Point, 4326) NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (sensor_point_id)
);

CREATE INDEX IF NOT EXISTS idx_enriched_building_time
  ON sensor_enriched_events (building_id, recorded_at);

-- 4) Window summary (state table)
CREATE TABLE IF NOT EXISTS building_heat_summary (
  id              BIGSERIAL PRIMARY KEY,
  building_id     INT NOT NULL REFERENCES buildings(building_id) ON DELETE CASCADE,
  building_name   TEXT NOT NULL,
  window_start    TIMESTAMPTZ NOT NULL,
  window_end      TIMESTAMPTZ NOT NULL,
  avg_temperature DOUBLE PRECISION,
  max_temperature DOUBLE PRECISION,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (building_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_summary_window
  ON building_heat_summary (window_start DESC);

-- 5) Alert table (threshold-based)
CREATE TABLE IF NOT EXISTS building_heat_alert (
  id              BIGSERIAL PRIMARY KEY,
  building_id     INT NOT NULL REFERENCES buildings(building_id) ON DELETE CASCADE,
  building_name   TEXT NOT NULL,
  window_start    TIMESTAMPTZ NOT NULL,
  window_end      TIMESTAMPTZ NOT NULL,
  avg_temperature DOUBLE PRECISION NOT NULL,
  alert_level     TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (building_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_alert_created
  ON building_heat_alert (created_at DESC);

-- =========================================
-- Functions
-- =========================================

-- A) Enrich a newly inserted sensor_points row into sensor_enriched_events
--    Only enriches if the point falls within a building polygon.
CREATE OR REPLACE FUNCTION enrich_sensor_point(p_sensor_point_id BIGINT)
RETURNS VOID AS $$
BEGIN
  INSERT INTO sensor_enriched_events (
    sensor_point_id,
    building_id,
    sensor_id,
    temperature,
    humidity,
    recorded_at,
    point_geom
  )
  SELECT
    sp.id,
    b.building_id,
    sp.sensor_id,
    sp.temperature,
    sp.humidity,
    sp.recorded_at,
    sp.geom
  FROM sensor_points sp
  JOIN buildings b
    ON ST_Within(sp.geom, b.geom)
  WHERE sp.id = p_sensor_point_id
  ON CONFLICT (sensor_point_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;


-- B) Update (UPSERT) 10-minute window summary for a given building and event time
CREATE OR REPLACE FUNCTION update_building_window_summary(
  p_building_id INT,
  p_recorded_at TIMESTAMPTZ
)
RETURNS VOID AS $$
DECLARE
  v_window_start TIMESTAMPTZ;
  v_window_end   TIMESTAMPTZ;
BEGIN
  -- floor to 10-minute window based on event-time
  v_window_start :=
    date_trunc('minute', p_recorded_at)
    - INTERVAL '1 minute' * (EXTRACT(minute FROM p_recorded_at)::int % 10);

  v_window_end := v_window_start + INTERVAL '10 minutes';

  INSERT INTO building_heat_summary (
    building_id, building_name, window_start, window_end, avg_temperature, max_temperature, updated_at
  )
  SELECT
    b.building_id,
    b.building_name,
    v_window_start,
    v_window_end,
    AVG(e.temperature) AS avg_temperature,
    MAX(e.temperature) AS max_temperature,
    NOW() AS updated_at
  FROM sensor_enriched_events e
  JOIN buildings b ON e.building_id = b.building_id
  WHERE e.building_id = p_building_id
    AND e.recorded_at >= v_window_start
    AND e.recorded_at <  v_window_end
  GROUP BY b.building_id, b.building_name
  ON CONFLICT (building_id, window_start)
  DO UPDATE SET
    building_name   = EXCLUDED.building_name,
    window_end      = EXCLUDED.window_end,
    avg_temperature = EXCLUDED.avg_temperature,
    max_temperature = EXCLUDED.max_temperature,
    updated_at      = NOW();
END;
$$ LANGUAGE plpgsql;


-- C) Check threshold and upsert alert for that building+window
CREATE OR REPLACE FUNCTION check_building_heat_alert(
  p_building_id INT,
  p_window_start TIMESTAMPTZ,
  p_threshold DOUBLE PRECISION DEFAULT 35
)
RETURNS VOID AS $$
DECLARE
  v_name TEXT;
  v_window_end TIMESTAMPTZ;
  v_avg DOUBLE PRECISION;
  v_level TEXT;
BEGIN
  SELECT building_name INTO v_name
  FROM buildings
  WHERE building_id = p_building_id;

  SELECT window_end, avg_temperature INTO v_window_end, v_avg
  FROM building_heat_summary
  WHERE building_id = p_building_id
    AND window_start = p_window_start;

  IF v_avg IS NULL THEN
    RETURN;
  END IF;

  IF v_avg > p_threshold THEN
    v_level := 'HIGH_HEAT';

    INSERT INTO building_heat_alert (
      building_id, window_start, window_end, building_name, avg_temperature, alert_level
    )
    VALUES (
      p_building_id, p_window_start, v_window_end, v_name, v_avg, v_level
    )
    ON CONFLICT (building_id, window_start)
    DO UPDATE SET
      window_end      = EXCLUDED.window_end,
      building_name   = EXCLUDED.building_name,
      avg_temperature = EXCLUDED.avg_temperature,
      alert_level     = EXCLUDED.alert_level,
      created_at      = NOW();
  END IF;
END;
$$ LANGUAGE plpgsql;