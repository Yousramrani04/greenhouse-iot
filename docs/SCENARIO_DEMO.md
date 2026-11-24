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
| 3:15 à 4:30 | Dagster | Planification horaire ; un run vert ; un run en échec volontaire (`simulate_exit_code: 2`) qui arrête la chaîne : « Soda a vu un problème, rien n'est publié » |
| 4:30 à 5:15 | Terminal ou fichier `dbt build` | 19 tests + nouveaux ; Soda : 12 contrôles réussis, 1 avertissement (disponibilité) |
| 5:15 à 6:15 | Streamlit | Historique d'une serre (G05 avec dépassement), classement : les capteurs en panne sentinelle/silencieux en tête (IOT-08) |
| 6:15 à 7:00 | `docs/iot09_isolation_forest.md` | Seuils fixes parfaits sur les défauts grossiers, aveugles aux anomalies contextuelles ; Isolation Forest les voit (IOT-09) ; conclusion : complémentaires |

## Questions probables du jury
- Pourquoi un export Parquet ? DuckDB ne lit pas un catalogue Iceberg JDBC ; l'export découple le Gold de Spark.
- Que se passe-t-il si Kafka redémarre ? Messages non consommés perdus (pas de volume) : limite assumée.
- Comment sont traités les retardataires ? Recalcul des minutes touchées par MERGE en Silver, recalcul des 2 dernières heures en Gold.
- Pourquoi deux outils de qualité (dbt et Soda) ? Tests de modèle d'un côté, fraîcheur et seuils sur le flux live de l'autre.
