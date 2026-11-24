"""Contrôle du Bronze historique : volumes, défauts visibles, doublons."""
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql(f"""CREATE SECRET silo (TYPE S3, KEY_ID '{os.environ['MINIO_ROOT_USER']}',
  SECRET '{os.environ['MINIO_ROOT_PASSWORD']}', ENDPOINT 'localhost:9000',
  URL_STYLE 'path', USE_SSL false)""")
SRC = "read_parquet('s3://bronze/historique/capteurs/*/*.parquet', hive_partitioning=true)"

print("== Par jour ==")
con.sql(f"""SELECT event_date, count(*) AS lignes, count(DISTINCT sensor_id) AS capteurs,
  count(*) FILTER (WHERE value IS NULL) AS nulls,
  count(*) FILTER (WHERE value = -999) AS sentinel,
  count(*) FILTER (WHERE _ingested_at - event_time > INTERVAL 1 MINUTE) AS retards
FROM {SRC} GROUP BY ALL ORDER BY 1""").show()

print("== Global ==")
con.sql(f"""SELECT count(*) AS lignes,
  count(*) - count(DISTINCT (sensor_id, event_time)) AS doublons,
  count(*) FILTER (WHERE unit = 'F') AS lignes_fahrenheit,
  count(*) FILTER (WHERE metric = 'temperature' AND unit = 'C' AND value > 60) AS aberrantes_temp
FROM {SRC}""").show()

print("== Température : deux échelles dans la même colonne ==")
con.sql(f"""SELECT unit, count(DISTINCT sensor_id) AS capteurs,
  round(median(value) FILTER (WHERE value > -900), 1) AS mediane
FROM {SRC} WHERE metric = 'temperature' GROUP BY unit""").show()
