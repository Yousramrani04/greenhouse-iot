"""Vérifications après 'dbt build' en CI : le Gold existe et la canicule de G05 est bien une alerte."""
import duckdb

con = duckdb.connect("dbt/gold.duckdb", read_only=True)
n_sensors = con.sql("select count(*) from dim_sensor").fetchone()[0]
alert = con.sql("select count(*) from gold_alert_periods where greenhouse_id = 'G05' and is_alert").fetchone()[0]
quality = con.sql("select count(*) from gold_sensor_quality").fetchone()[0]
assert n_sensors == 60, f"dim_sensor : {n_sensors} capteurs au lieu de 60"
assert alert >= 1, "aucune période d'alerte détectée en G05"
assert quality == 60, f"gold_sensor_quality : {quality} lignes au lieu de 60"
print("Gold OK : 60 capteurs, alerte G05 détectée")
