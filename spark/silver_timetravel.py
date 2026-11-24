"""IOT-03 : time travel Iceberg. Montre ce que l'intégration des retardataires a changé."""
import argparse

from pyspark.sql import functions as F

from session import get_spark

ap = argparse.ArgumentParser()
ap.add_argument("--cutoff", default="2026-11-20 22:00:00")
ap.add_argument("--before", type=int, help="snapshot_id avant (défaut : avant-dernier)")
ap.add_argument("--after", type=int, help="snapshot_id après (défaut : dernier)")
args = ap.parse_args()

spark = get_spark("silver-timetravel")
T = "lake.silver.measurements_1min"

print("== 1. Historique des snapshots (un par écriture) ==")
snaps = spark.sql(f"""SELECT snapshot_id, committed_at, operation,
                             summary['added-records'] AS ajoutees,
                             summary['deleted-records'] AS supprimees,
                             summary['total-records'] AS total
                      FROM {T}.snapshots ORDER BY committed_at""")
snaps.show(truncate=False)

ids = [r["snapshot_id"] for r in snaps.select("snapshot_id").collect()]
if len(ids) < 2 and not (args.before and args.after):
    raise SystemExit("Il faut au moins 2 snapshots : lance d'abord l'exécution 1 puis l'exécution 2.")
before = args.before or ids[-2]
after = args.after or ids[-1]
print(f"comparaison : snapshot {before} (avant)  ->  snapshot {after} (après)\n")

b = spark.sql(f"SELECT * FROM {T} VERSION AS OF {before}").alias("b")
a = spark.sql(f"SELECT * FROM {T} VERSION AS OF {after}").alias("a")

print("== 2. Lignes dans chaque version ==")
print(f"avant : {b.count()}   après : {a.count()}\n")

diff = (a.join(b, ["sensor_id", "minute_ts"], "full_outer")
        .select("sensor_id", "minute_ts",
                F.col("b.n_readings").alias("n_avant"), F.col("a.n_readings").alias("n_apres"),
                F.round("b.value", 3).alias("valeur_avant"), F.round("a.value", 3).alias("valeur_apres"))
        .withColumn("statut",
                    F.when(F.col("n_avant").isNull(), "inseree")
                     .when(F.col("n_apres").isNull(), "supprimee")
                     .when(F.col("n_avant") != F.col("n_apres"), "mise_a_jour"))
        .filter(F.col("statut").isNotNull())
        .withColumn("zone", F.when(F.col("minute_ts") < F.lit(args.cutoff).cast("timestamp"),
                                   "avant_cutoff (retardataire)").otherwise("apres_cutoff (nouveau)"))
        .cache())

print("== 3. Ce que le MERGE a changé (attendu : ~50 mises à jour, ~2 insertions avant cutoff) ==")
diff.groupBy("zone", "statut").count().orderBy("zone", "statut").show(truncate=False)

print("== 4. Minutes retardataires : avant / après ==")
(diff.filter(F.col("zone").startswith("avant_cutoff"))
     .orderBy("minute_ts", "sensor_id").show(15, truncate=False))

print("== 5. La même requête 'à la date de l'exécution 1' (TIMESTAMP AS OF) ==")
t1 = snaps.filter(F.col("snapshot_id") == before).first()["committed_at"]
spark.sql(f"""SELECT count(*) AS lignes, max(minute_ts) AS derniere_minute
              FROM {T} TIMESTAMP AS OF '{t1}'""").show(truncate=False)
