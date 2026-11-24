"""Valeurs attendues pour Silver : DuckDB sur le Bronze + vérité terrain du générateur."""
import os
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(f"""CREATE SECRET silo (TYPE S3, KEY_ID '{os.environ['MINIO_ROOT_USER']}',
  SECRET '{os.environ['MINIO_ROOT_PASSWORD']}', ENDPOINT 'localhost:9000',
  URL_STYLE 'path', USE_SSL false)""")

con.sql("""
CREATE VIEW classified AS
WITH bronze AS (
  SELECT * FROM read_parquet('s3://bronze/historique/capteurs/*/*.parquet')
), dedup AS (
  SELECT * EXCLUDE rn FROM (
    SELECT *, row_number() OVER (PARTITION BY sensor_id, event_time ORDER BY _ingested_at) AS rn
    FROM bronze) WHERE rn = 1
), conv AS (
  SELECT *, CASE WHEN unit = 'F' THEN (value - 32) * 5.0 / 9 ELSE value END AS value_c FROM dedup
)
SELECT *, CASE
  WHEN value IS NULL OR isnan(value) THEN 'null'
  WHEN value = -999 THEN 'sentinel'
  WHEN NOT (CASE metric
              WHEN 'temperature'   THEN value_c BETWEEN -10 AND 60
              WHEN 'air_humidity'  THEN value_c BETWEEN 0 AND 100
              WHEN 'soil_humidity' THEN value_c BETWEEN 0 AND 100
              WHEN 'co2'           THEN value_c BETWEEN 0 AND 3000
              ELSE false END) THEN 'out_of_range'
  END AS reason
FROM conv
""")

print("== DuckDB : messages uniques par statut (NULL = valide) ==")
con.sql("SELECT reason, count(*) AS n FROM classified GROUP BY reason ORDER BY reason").show()

print("== DuckDB : lignes Silver attendues ==")
con.sql("""SELECT count(*) AS lignes, sum(n) AS mesures_valides FROM (
  SELECT sensor_id, date_trunc('minute', event_time) AS m, count(*) AS n
  FROM classified WHERE reason IS NULL GROUP BY ALL)""").show()

truth = pd.read_parquet(ROOT / "data/ground_truth/history_defects.parquet")
def keys(d):
    t = truth[truth.defect == d]
    return set(zip(t.sensor_id, t.event_time))
miss, outl = keys("missing"), keys("outlier")
print("== Vérité terrain du générateur ==")
print("null         :", len(miss))
print("sentinel     :", len(keys("sentinel_outage")))
print("out_of_range :", len(outl - miss), f"({len(outl)} aberrations, dont {len(outl & miss)} aussi nulles)")