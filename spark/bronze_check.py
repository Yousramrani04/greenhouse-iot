"""Lecture du Bronze historique avec Spark, comparée aux chiffres de DuckDB."""
from pyspark.sql import functions as F

from session import get_spark

spark = get_spark("bronze-check")

# Spark découvre la colonne event_date à partir des dossiers event_date=YYYY-MM-DD
bronze = spark.read.parquet("s3a://bronze/historique/capteurs/")

print("== schéma ==")
bronze.printSchema()

print("== lignes par jour (à comparer à DuckDB) ==")
bronze.groupBy("event_date").count().orderBy("event_date").show()

print("== total, plage de temps, capteurs ==")
bronze.agg(
    F.count("*").alias("lignes"),
    F.countDistinct("sensor_id").alias("capteurs"),
    F.min("event_time").alias("premier_event"),
    F.max("event_time").alias("dernier_event"),
).show(truncate=False)
