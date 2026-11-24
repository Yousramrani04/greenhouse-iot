"""Prédit, avec DuckDB, ce que doit produire l'intégration des retardataires (étape 2.5)."""
import argparse
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

ap = argparse.ArgumentParser()
ap.add_argument("--cutoff", default="2026-11-20 22:00:00")
ap.add_argument("--source", default="s3://bronze/historique/capteurs/*/*.parquet")
args = ap.parse_args()

con = duckdb.connect()
con.sql("SET TimeZone='UTC'")
if args.source.startswith("s3://"):
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql(f"""CREATE SECRET silo (TYPE S3, KEY_ID '{os.environ['MINIO_ROOT_USER']}',
      SECRET '{os.environ['MINIO_ROOT_PASSWORD']}', ENDPOINT 'localhost:9000',
      URL_STYLE 'path', USE_SSL false)""")

con.sql(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{args.source}')")


def silver_minutes(name, where):
    con.sql(f"""
    CREATE TABLE {name} AS
    WITH src AS (SELECT * FROM bronze WHERE {where}),
    dedup AS (
      SELECT * EXCLUDE rn FROM (
        SELECT *, row_number() OVER (PARTITION BY sensor_id, event_time ORDER BY _ingested_at) AS rn
        FROM src) WHERE rn = 1),
    conv AS (
      SELECT *, CASE WHEN unit = 'F' THEN (value - 32) * 5.0 / 9 ELSE value END AS value_c FROM dedup),
    ok AS (
      SELECT * FROM conv
      WHERE value IS NOT NULL AND NOT isnan(value) AND value <> -999
        AND CASE metric
              WHEN 'temperature'   THEN value_c BETWEEN -10 AND 60
              WHEN 'air_humidity'  THEN value_c BETWEEN 0 AND 100
              WHEN 'soil_humidity' THEN value_c BETWEEN 0 AND 100
              WHEN 'co2'           THEN value_c BETWEEN 0 AND 3000 ELSE false END)
    SELECT sensor_id, date_trunc('minute', event_time) AS minute_ts,
           count(*) AS n_readings, avg(value_c) AS value
    FROM ok GROUP BY ALL""")


silver_minutes("run1", f"_ingested_at < TIMESTAMPTZ '{args.cutoff}+00'")
silver_minutes("run2", "true")

print(f"== cutoff = {args.cutoff} UTC ==")
print("\n== Silver après l'exécution 1 / après l'exécution 2 ==")
con.sql("""SELECT (SELECT count(*) FROM run1) AS lignes_run1,
                  (SELECT count(*) FROM run2) AS lignes_run2,
                  (SELECT sum(n_readings) FROM run1) AS mesures_run1,
                  (SELECT sum(n_readings) FROM run2) AS mesures_run2""").show()

print("== Ce que le MERGE de l'exécution 2 doit faire ==")
con.sql(f"""
SELECT
  count(*) FILTER (WHERE a.sensor_id IS NULL AND b.minute_ts <  TIMESTAMPTZ '{args.cutoff}+00') AS inserees_retardataires,
  count(*) FILTER (WHERE a.sensor_id IS NULL AND b.minute_ts >= TIMESTAMPTZ '{args.cutoff}+00') AS inserees_nouvelles,
  count(*) FILTER (WHERE a.sensor_id IS NOT NULL AND a.n_readings <> b.n_readings)              AS mises_a_jour,
  count(*) FILTER (WHERE a.sensor_id IS NOT NULL AND a.n_readings =  b.n_readings)              AS inchangees
FROM run2 b LEFT JOIN run1 a USING (sensor_id, minute_ts)""").show()

print("== Minutes qui existaient déjà et ont changé : extrait ==")
con.sql("""
SELECT sensor_id, minute_ts, a.n_readings AS n_avant, b.n_readings AS n_apres,
       round(a.value, 3) AS valeur_avant, round(b.value, 3) AS valeur_apres
FROM run2 b JOIN run1 a USING (sensor_id, minute_ts)
WHERE a.n_readings <> b.n_readings
ORDER BY minute_ts, sensor_id LIMIT 8""").show()

print("== Garde-fou : une minute ne perd jamais de mesures ==")
con.sql("""SELECT count(*) AS minutes_qui_perdent_des_mesures
           FROM run2 b JOIN run1 a USING (sensor_id, minute_ts) WHERE b.n_readings < a.n_readings""").show()
