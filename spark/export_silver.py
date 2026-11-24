"""Exporte lake.silver.measurements_1min en Parquet (s3://silver/export/) pour dbt + DuckDB.

Pourquoi : DuckDB ne lit pas un catalogue Iceberg stocké dans PostgreSQL ; un export Parquet
propre (un seul fichier, timestamps en microsecondes UTC) est lu directement par dbt-duckdb.
"""
from session import get_spark

spark = get_spark("export-silver")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

df = spark.table("lake.silver.measurements_1min")
n = df.count()
(df.coalesce(1).write.mode("overwrite")
   .parquet("s3a://silver/export/measurements_1min/"))
print(f"export terminé : {n} lignes -> s3://silver/export/measurements_1min/")
