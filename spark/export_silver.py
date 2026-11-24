"""Exporte Silver en Parquet (s3://silver/export/) pour dbt + DuckDB.

Pourquoi : DuckDB ne lit pas un catalogue Iceberg stocké dans PostgreSQL ; un export Parquet
propre (un seul fichier, timestamps en microsecondes UTC) est lu directement par dbt-duckdb.
Deux exports : les mesures valides (measurements_1min) et les rejets (measurements_rejected),
ces derniers servant au classement de fiabilité des capteurs (Gold).
"""
from session import get_spark

spark = get_spark("export-silver")
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

for table, target in (("measurements_1min", "measurements_1min"),
                      ("measurements_rejected", "measurements_rejected")):
    df = spark.table(f"lake.silver.{table}")
    n = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(f"s3a://silver/export/{target}/")
    print(f"export terminé : {n} lignes -> s3://silver/export/{target}/")
