CREATE TABLE IF NOT EXISTS live_measurements (
  sensor_id     text             NOT NULL,
  greenhouse_id text             NOT NULL,
  metric        text             NOT NULL,
  event_time    timestamptz      NOT NULL,
  raw_value     double precision,
  raw_unit      text,
  value         double precision,           -- unité canonique (°C, %, ppm) ; NULL si invalide
  status        text             NOT NULL,  -- ok | null | sentinel | out_of_range
  received_at   timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (sensor_id, event_time)
);
CREATE INDEX IF NOT EXISTS idx_live_time ON live_measurements (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_live_gh   ON live_measurements (greenhouse_id, metric, event_time DESC);
