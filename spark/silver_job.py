"""Job Silver : Bronze (fichiers bruts) -> lake.silver.measurements_1min (Iceberg).

Principe (idempotent) :
  1. repérer les NOUVEAUX messages du Bronze (arrivés après le watermark d'ingestion) ;
  2. en déduire les (capteur, minute) touchés ;
  3. RECALCULER ces minutes depuis tout le Bronze (jamais depuis le lot seul) ;
  4. MERGE : met à jour les minutes qui ont changé, insère les nouvelles.

Ordre de nettoyage : doublons -> nulls -> sentinelle -999 -> conversion °F -> plage physique.
(La sentinelle est testée AVANT la conversion : sinon -999 °F deviendrait -573 °C.)

Usage :
  silver_job.py --reset                      # repart de zéro (démo)
  silver_job.py                              # traite les nouveaux messages
  silver_job.py --cutoff "2026-11-20 22:00:00"   # ne voit que les messages arrivés avant
  silver_job.py --reprocess                  # ignore le watermark (preuve d'idempotence)
"""
import argparse

from pyspark.errors import AnalysisException
from pyspark.sql import Window, functions as F

from session import get_spark

JOB = "silver_1min"
SENTINEL = -999.0
EPOCH = "1970-01-01 00:00:00"
FAR_FUTURE = "2100-01-01 00:00:00"
HISTORY_SOURCES = ["s3a://bronze/historique/capteurs/"]
LIVE_SOURCES = ["s3a://bronze/live/capteurs/"]

# Plage physiquement plausible, en unité canonique (°C, %, ppm)
PLAUSIBLE = {
    "temperature": (-10.0, 60.0),
    "air_humidity": (0.0, 100.0),
    "soil_humidity": (0.0, 100.0),
    "co2": (0.0, 3000.0),
}

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS lake.silver",
    """CREATE TABLE IF NOT EXISTS lake.silver.measurements_1min (
         sensor_id STRING, greenhouse_id STRING, metric STRING,
         minute_ts TIMESTAMP, value DOUBLE, unit STRING,
         n_readings INT, last_ingested_at TIMESTAMP)
       USING iceberg
       PARTITIONED BY (days(minute_ts))
       TBLPROPERTIES ('format-version' = '2',
                      'write.parquet.compression-codec' = 'zstd',
                      'write.merge.mode' = 'copy-on-write',
                      'write.update.mode' = 'copy-on-write',
                      'write.delete.mode' = 'copy-on-write')""",
    """CREATE TABLE IF NOT EXISTS lake.silver.measurements_rejected (
         sensor_id STRING, greenhouse_id STRING, metric STRING,
         event_time TIMESTAMP, raw_value DOUBLE, raw_unit STRING,
         reason STRING, ingested_at TIMESTAMP)
       USING iceberg
       PARTITIONED BY (days(event_time))""",
    """CREATE TABLE IF NOT EXISTS lake.silver.etl_state (
         job STRING, high_watermark TIMESTAMP) USING iceberg""",
]


def plausible():
    """Condition : la valeur (en unité canonique) est dans la plage de sa mesure."""
    cond = F.lit(False)
    for metric, (lo, hi) in PLAUSIBLE.items():
        cond = cond | ((F.col("metric") == metric) & F.col("value_c").between(lo, hi))
    return cond


def canonical_unit():
    return (F.when(F.col("metric") == "temperature", "C")
             .when(F.col("metric").isin("air_humidity", "soil_humidity"), "%")
             .when(F.col("metric") == "co2", "ppm"))


def read_bronze(spark, paths):
    """Lit chaque racine Bronze séparément puis les unit : Spark refuse de mélanger
    deux racines de partitionnement différentes dans un seul read.parquet."""
    out = None
    for path in paths:
        try:
            df = spark.read.parquet(path)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"Bronze absent, ignoré : {path}")
                continue
            raise
        out = df if out is None else out.unionByName(df, allowMissingColumns=True)
    if out is None:
        raise RuntimeError("Aucune donnée Bronze trouvée (historique et live absents)")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--reprocess", action="store_true")
    ap.add_argument("--history-only", action="store_true",
                    help="ne lit que l'historique (reproduit les chiffres de référence)")
    ap.add_argument("--cutoff", default=FAR_FUTURE)
    args = ap.parse_args()
    SOURCES = HISTORY_SOURCES if args.history_only else HISTORY_SOURCES + LIVE_SOURCES

    spark = get_spark("silver-1min")
    spark.conf.set("spark.sql.shuffle.partitions", "8")   # peu de données : peu de petits fichiers

    spark.sql(DDL[0])
    if args.reset:
        for t in ("measurements_1min", "measurements_rejected", "etl_state"):
            spark.sql(f"DROP TABLE IF EXISTS lake.silver.{t} PURGE")
    for stmt in DDL[1:]:
        spark.sql(stmt)

    # Watermark lu en SQL puis relu comme chaîne : pas de conversion de fuseau côté Python
    wm = EPOCH if args.reprocess else spark.sql(
        f"""SELECT CAST(COALESCE(MAX(high_watermark), TIMESTAMP '{EPOCH}') AS STRING)
            FROM lake.silver.etl_state WHERE job = '{JOB}'""").first()[0]
    print(f"watermark : {wm} | cutoff : {args.cutoff}")

    def ts(s):
        return F.lit(s).cast("timestamp")

    # 1. Bronze visible à cet instant, avec la minute de l'EVENT time
    bronze = (read_bronze(spark, SOURCES)
              .filter(F.col("_ingested_at") < ts(args.cutoff))
              .withColumn("minute_ts", F.date_trunc("minute", "event_time")))

    new_rows = bronze.filter(F.col("_ingested_at") > ts(wm))
    n_new = new_rows.count()
    print(f"nouveaux messages : {n_new}")
    if n_new == 0:
        print("rien à faire (idempotent)")
        return

    # 2-3. minutes touchées -> TOUS les messages de ces minutes (pas seulement les nouveaux)
    touched = new_rows.select("sensor_id", "minute_ts").distinct()
    candidates = bronze.join(touched, ["sensor_id", "minute_ts"], "left_semi")

    # Doublons : même (capteur, event_time) -> on garde la première copie reçue
    w = Window.partitionBy("sensor_id", "event_time").orderBy(F.col("_ingested_at").asc())
    dedup = candidates.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    classified = (
        dedup
        .withColumn("value_c", F.when(F.col("unit") == "F", (F.col("value") - 32) * 5 / 9)
                                .otherwise(F.col("value")))
        .withColumn("reason",
                    F.when(F.col("value").isNull() | F.isnan("value"), "null")
                     .when(F.col("value") == SENTINEL, "sentinel")
                     .when(~plausible(), "out_of_range"))      # reste NULL si la mesure est valide
        .cache()
    )

    rejected_new = (classified.filter(F.col("reason").isNotNull())
                    .select("sensor_id", "greenhouse_id", "metric", "event_time",
                            F.col("value").alias("raw_value"), F.col("unit").alias("raw_unit"),
                            "reason", F.col("_ingested_at").alias("ingested_at")))

    silver_new = (classified.filter(F.col("reason").isNull())
                  .groupBy("sensor_id", "greenhouse_id", "metric", "minute_ts")
                  .agg(F.avg("value_c").alias("value"),
                       F.count(F.lit(1)).cast("int").alias("n_readings"),
                       F.max("_ingested_at").alias("last_ingested_at"))
                  .withColumn("unit", canonical_unit())
                  .select("sensor_id", "greenhouse_id", "metric", "minute_ts",
                          "value", "unit", "n_readings", "last_ingested_at"))

    print(f"minutes recalculées : {touched.count()}")
    print("messages uniques par statut (reason NULL = valide) :")
    classified.groupBy("reason").count().orderBy("reason").show()

    rejected_new.createOrReplaceTempView("rejected_new")
    silver_new.createOrReplaceTempView("silver_new")

    # 4a. rejets : insérés une seule fois (rejouer ne duplique rien)
    spark.sql("""
        MERGE INTO lake.silver.measurements_rejected t
        USING rejected_new s
        ON t.sensor_id = s.sensor_id AND t.event_time = s.event_time
        WHEN NOT MATCHED THEN INSERT *""")

    # 4b. Silver : mise à jour SEULEMENT si la minute a changé (nouveau message reçu)
    spark.sql("""
        MERGE INTO lake.silver.measurements_1min t
        USING silver_new s
        ON t.sensor_id = s.sensor_id AND t.minute_ts = s.minute_ts
        WHEN MATCHED AND (t.n_readings <> s.n_readings
                          OR t.last_ingested_at <> s.last_ingested_at) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

    # 5. watermark : ne recule jamais
    new_rows.createOrReplaceTempView("new_rows")
    spark.sql(f"""
        MERGE INTO lake.silver.etl_state t
        USING (SELECT '{JOB}' AS job, MAX(_ingested_at) AS high_watermark FROM new_rows) s
        ON t.job = s.job
        WHEN MATCHED THEN UPDATE SET t.high_watermark = GREATEST(t.high_watermark, s.high_watermark)
        WHEN NOT MATCHED THEN INSERT *""")

    classified.unpersist()
    print("Silver à jour.")


if __name__ == "__main__":
    main()