"""Vérifie que DuckDB lit bien l'export Silver (la couche Gold en dépendra)."""
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
con = duckdb.connect()
con.sql("SET TimeZone='UTC'; INSTALL httpfs; LOAD httpfs;")
con.sql(f"""CREATE SECRET silo (TYPE S3, KEY_ID '{os.environ['MINIO_ROOT_USER']}',
  SECRET '{os.environ['MINIO_ROOT_PASSWORD']}', ENDPOINT 'localhost:9000',
  URL_STYLE 'path', USE_SSL false)""")
src = "read_parquet('s3://silver/export/measurements_1min/*.parquet')"

print("== types lus par DuckDB ==")
con.sql(f"DESCRIBE SELECT * FROM {src}").show()
print("== volume, plage de temps, capteurs ==")
con.sql(f"""SELECT count(*) AS lignes, count(DISTINCT sensor_id) AS capteurs,
                   min(minute_ts) AS premiere_minute, max(minute_ts) AS derniere_minute
            FROM {src}""").show()
print("== part du live (minutes du 24/11) ==")
con.sql(f"""SELECT count(*) AS lignes_live, count(DISTINCT sensor_id) AS capteurs_live
            FROM {src} WHERE minute_ts >= TIMESTAMPTZ '2026-11-24 00:00:00+00'""").show()
