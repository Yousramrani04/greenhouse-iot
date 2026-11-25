# Démonstration de 7 minutes (chronométrée)

## Préparation (à faire 15 minutes avant, jamais en direct)
1. `docker compose up -d` ; vérifier avec `bash scripts/validate_all.sh`.
2. Terminal A : `python -m src.streaming.producer_capteurs --interval 5 --heat G05` (la canicule doit durer plus de 11 min pour que l'alerte soit `Firing`).
3. Terminal B : `python -m src.streaming.grafana_writer`. NE PAS lancer bronze_writer avant la démo Kafka UI (le retard de ce groupe doit monter).
4. Terminal C : `bash scripts/run_dagster.sh` ; terminal D : `streamlit run app/streamlit_app.py`.
5. Ouvrir 5 onglets : Kafka UI, Grafana, Dagster, Streamlit, README (schéma).
6. Plan B si le réseau ou Docker tombe : captures dans `docs/preuves/`.

## Déroulé
| Temps | Écran | Message et preuve |
|---|---|---|
| 0:00 à 0:45 | README (schéma Mermaid) | Deux chemins, trois couches, 60 capteurs, un `docker compose up -d` |
| 0:45 à 1:45 | Kafka UI, topic `capteurs`, groupes | Deux groupes indépendants : `bronze-writer` en retard (messages en attente), `grafana-writer` à 0 (IOT-02). Lancer `bronze_writer` : son retard retombe |
| 1:45 à 3:15 | Grafana | Température des 15 serres en direct, jauges, capteurs silencieux (IOT-06) ; alerte G05 `Firing` (IOT-07) |
| 3:15 à 4:30 | Dagster | Planification horaire ; un run vert ; un run en échec volontaire (`simulate_exit_code: 2`) qui arrête la chaîne : « Soda a vu un problème : la chaîne s'arrête, le Gold est signalé non conforme et les étapes suivantes ne s'exécutent pas » |
| 4:30 à 5:15 | Terminal ou fichier `dbt build` | dbt : 30 éléments (8 modèles, 22 tests) ; Soda Silver : 12 contrôles réussis, 1 avertissement (disponibilité minimale 88,06 %) ; Soda live : 6 sur 6 |
| 5:15 à 6:15 | Streamlit | Historique d'une serre (G05 avec dépassement), classement de fiabilité : les 8 capteurs ayant subi une panne planifiée occupent 8 des 9 premières places sur 60, vérifié avec la vérité terrain (IOT-08) |
| 6:15 à 7:00 | `docs/iot09_isolation_forest.md` | Seuils fixes parfaits sur les défauts grossiers, aveugles aux anomalies contextuelles ; Isolation Forest les voit (IOT-09) ; conclusion : complémentaires |

## Questions probables du jury
- Pourquoi la disponibilité avait-elle fait échouer la chaîne ? Le 24 novembre, le producteur a tourné par sessions : le modèle comptait les trous de la source comme des pannes de capteurs (moyenne 90,3 %). Cause vérifiée par un comptage par heure dans PostgreSQL (aucune mesure de 12 h à 19 h, 60 capteurs sur les heures actives). Correction : minutes attendues = minutes où au moins la moitié des capteurs ont émis ; l'historique est inchangé, aucune donnée supprimée.
- Pourquoi un export Parquet ? DuckDB ne lit pas un catalogue Iceberg JDBC ; l'export découple le Gold de Spark.
- Que se passe-t-il si Kafka redémarre ? Messages non consommés perdus (pas de volume) : limite assumée.
- Comment sont traités les retardataires ? Recalcul des minutes touchées par MERGE en Silver, recalcul des 2 dernières heures en Gold.
- Pourquoi deux outils de qualité (dbt et Soda) ? Tests de modèle d'un côté, fraîcheur et seuils sur le flux live de l'autre.
