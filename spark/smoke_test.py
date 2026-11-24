"""Test de bout en bout : Spark -> catalogue Postgres -> fichiers dans Silo."""
from session import get_spark

spark = get_spark("smoke-test")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.iot")
# PURGE : supprime aussi les fichiers dans Silo (rend le test relançable)
spark.sql("DROP TABLE IF EXISTS lake.iot.smoke_test PURGE")
spark.sql("CREATE TABLE lake.iot.smoke_test (id INT, label STRING) USING iceberg")
spark.sql("INSERT INTO lake.iot.smoke_test VALUES (1, 'a'), (2, 'b')")

print("== contenu ==")
spark.sql("SELECT * FROM lake.iot.smoke_test").show()
print("== snapshots ==")
spark.sql("SELECT snapshot_id, operation FROM lake.iot.smoke_test.snapshots").show(truncate=False)
print("== fichiers de données ==")
spark.sql("SELECT file_path, record_count FROM lake.iot.smoke_test.files").show(truncate=False)