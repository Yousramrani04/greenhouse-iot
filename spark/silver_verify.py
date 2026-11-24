"""Vérifie la table Silver Iceberg : à comparer à scripts/expected_silver.py (DuckDB)."""
from pyspark.sql import functions as F

from session import get_spark

spark = get_spark("silver-verify")
T = "lake.silver.measurements_1min"
R = "lake.silver.measurements_rejected"

print("== rejets par raison (attendu : null 24184, sentinel 996, out_of_range ~5944) ==")
spark.table(R).groupBy("reason").count().orderBy("reason").show()

print("== lignes Silver et mesures valides (attendu : ~603500 et ~1177600) ==")
spark.table(T).agg(F.count("*").alias("lignes"), F.sum("n_readings").alias("mesures_valides")).show()

print("== garde-fous (attendu : 0, 0, 0) ==")
spark.sql(f"""
SELECT
  (SELECT count(*) FROM {T} WHERE metric = 'temperature' AND unit <> 'C')           AS unites_non_canoniques,
  (SELECT count(*) FROM {T}
     WHERE (metric = 'temperature'   AND value NOT BETWEEN -10 AND 60)
        OR (metric IN ('air_humidity','soil_humidity') AND value NOT BETWEEN 0 AND 100)
        OR (metric = 'co2'           AND value NOT BETWEEN 0 AND 3000))             AS hors_plage,
  (SELECT count(*) FROM (SELECT sensor_id, minute_ts FROM {T}
                         GROUP BY 1, 2 HAVING count(*) > 1))                        AS doublons
""").show()

print("== médiane de température par serre (attendu : 22.5 à 25.5 (24 + biais de serre), jamais ~75 pour G04/G09/G13) ==")
spark.sql(f"""
SELECT greenhouse_id, round(percentile(value, 0.5), 2) AS mediane_c
FROM {T} WHERE metric = 'temperature' GROUP BY greenhouse_id ORDER BY greenhouse_id
""").show(20)

print("== fichiers de données de la table (attendu : quelques dizaines au plus) ==")
spark.sql(f"SELECT count(*) AS fichiers, round(sum(file_size_in_bytes)/1e6, 1) AS mo FROM {T}.files").show()
