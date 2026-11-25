# Couverture du cahier des charges (Sujet 7)

Statuts : FAIT = vérifié avec preuve ; À CONFIRMER = livré mais preuve (capture ou sortie) pas encore enregistrée dans `docs/preuves/`.
Mettre à jour la colonne « Statut » uniquement après avoir vu la preuve.

| Exigence | Type | Statut | Preuve (fichier ou commande) | Capture à enregistrer |
|---|---|---|---|---|
| IOT-01 60 capteurs en direct + 7 jours d'historique, 6 défauts | OBL | FAIT | `src/generator/ingest_capteurs.py`, `src/streaming/producer_capteurs.py`, `data/ground_truth/` ; `scripts/verify_bronze.py` | `docs/preuves/iot01_verify_bronze.txt` |
| IOT-02 deux groupes Kafka indépendants sur le même topic | OBL | FAIT | Kafka UI : retard de bronze-writer élevé, grafana-writer à 0 | `docs/preuves/iot02_kafka_ui.png` |
| IOT-03 Silver Iceberg, grain capteur × minute, °C | OBL | FAIT | `spark/silver_job.py` ; 603 526 lignes (historique) ; garde-fous 0/0/0 ; time travel `spark/silver_timetravel.py` | `docs/preuves/iot03_silver_verify.txt` |
| IOT-04 Gold : moyennes horaires, périodes de dépassement, disponibilité | OBL | FAIT | `dbt build` PASS ; `gold_greenhouse_1h`, `gold_alert_periods`, `gold_sensor_availability` | `docs/preuves/iot04_dbt_build.txt` |
| IOT-05 Soda : -10 à 60 °C en Silver, fraîcheur < 10 min | OBL | FAIT | `soda/checks_silver.yml`, `soda/checks_live.yml` ; échec de fraîcheur démontré | `docs/preuves/iot05_soda.txt` |
| IOT-06 Grafana : température par serre en direct, jauges, capteurs silencieux | OBL | À CONFIRMER | `grafana/dashboards/greenhouse-live.json` (provisionné) | `docs/preuves/iot06_grafana.png` |
| IOT-07 alerte > 35 °C pendant 10 min | OBL | FAIT | `grafana/provisioning/alerting/rules.yml` ; démo `--heat G05` ; alerte observée en état Alerting ; notifications e-mail non configurées (SMTP) | `docs/preuves/iot07_alerte_firing.png` |
| IOT-08 Streamlit : historique d'une serre, capteurs les moins fiables | OPT | FAIT | `app/streamlit_app.py`, `gold_sensor_quality` ; classement vérifié : les 8 pannes planifiées sont dans les 9 premiers rangs sur 60 | `docs/preuves/iot08_streamlit.png` |
| IOT-09 Isolation Forest comparé aux seuils fixes | OPT | FAIT | `src/ml/isolation_forest.py`, `docs/iot09_isolation_forest.md` | sortie du script |

## Livrables communs
| Livrable | Statut | Preuve |
|---|---|---|
| README (membres, sujet, schéma, lancement < 5 commandes) | À CONFIRMER (test sur clone propre) | `README.md` |
| Un seul docker-compose.yml | FAIT | `docker-compose.yml` |
| `.env.example` sans secret | FAIT | `.env.example` |
| Projet dbt, `dbt build` sans erreur | FAIT | `docs/preuves/iot04_dbt_build.txt` |
| Contrôles Soda | FAIT | `soda/` |
| Dagster avec planification | FAIT (run vert + échec volontaire dans l'historique Dagster) | `orchestration/definitions.py` (`chaine_horaire`) |
| Application de présentation lancée par le README | FAIT | `app/streamlit_app.py` |
| Git + GitHub Actions | FAIT (runs #1 et #2 verts) | `.github/workflows/ci.yml` |
| Rapport 10 à 15 pages | À FAIRE | voir `docs/PLAN_RAPPORT.md` |
