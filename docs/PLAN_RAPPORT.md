# Plan du rapport (10 à 15 pages)

Rappel : ne jamais coller de clés S3 ni de mots de passe (attention aux sorties de `dbt debug` et au fichier `dbt/profiles.yml`).

| # | Section | Pages | Contenu |
|---|---|---|---|
| 1 | Contexte métier | 1 | Serres connectées, 60 capteurs, pourquoi : alerte canicule, suivi de disponibilité ; données synthétiques et défauts simulés (tableau des 6 défauts et de leur taux : retardataires 3 %, doublons 1 %, manquants 2 %, aberrants 0,5 %, pannes sentinelle -999 et silencieuses, °F pour G04/G09/G13) |
| 2 | Architecture et justification des outils | 3 | Schéma Mermaid ; deux chemins (lots, flux) ; un tableau « outil → problème résolu que les autres ne résolvent pas » (voir ci-dessous) |
| 3 | Ingestion et stockage (Bronze) | 1,5 | Historique (nom fixe, écriture idempotente) ; live (fichiers `p<partition>-<offset min>-<offset max>.parquet`, offsets validés après écriture) ; IOT-02 : deux groupes, capture Kafka UI |
| 4 | Transformation (Silver) | 2 | Grain capteur × minute ; règle « sentinelle avant conversion °F » ; dédoublonnage (premier message reçu) ; MERGE incrémental et retardataires ; time travel ; chiffres de référence |
| 5 | Modèle Gold et grain | 1,5 | Un tableau : table, grain, rôle (voir ci-dessous) ; recalcul incrémental sur 2 h ; moyenne pondérée par n_readings |
| 6 | Stratégie de qualité | 1,5 | Quatre niveaux : règles Silver (rejets par raison), tests dbt (19 + nouveaux), contrôles Soda (Silver, fraîcheur live), réconciliation Silver → Gold ; code de sortie Soda et arrêt de la chaîne |
| 7 | Orchestration et temps réel | 1,5 | Dagster : chaîne horaire, un seul run à la fois, échec géré (code 1 passe, 2 et 3 arrêtent), capture run vert + rouge ; Grafana : provisioning, alerte > 35 °C pendant 10 min |
| 8 | Présentation et IOT-09 | 1 | Streamlit (instantané du Gold pour éviter le verrou DuckDB) ; Isolation Forest vs seuils : tableau A et B, et l'honnêteté sur l'injection |
| 9 | Ressources et limites | 1 | Relevé `docker stats` (voir `docs/preuves/memoire.txt`) ; limites : Kafka sans volume, petits fichiers Bronze non compactés, recalcul Gold limité à 2 h, dépendance à l'export Parquet, DuckDB < 1.1.0 |
| 10 | Tableau de couverture | 0,5 | Reprendre `docs/COUVERTURE.md` avec les captures |

## Justification des outils (une ligne chacune)
- **Docker Compose** : une seule commande démarre tous les services, reproductibilité.
- **MinIO (pgsty/silo)** : stockage objet compatible S3 pour Bronze et pour l'entrepôt Iceberg ; un système de fichiers simple n'offre pas l'API S3 que Spark, DuckDB et boto3 utilisent.
- **Kafka (KRaft)** : transport du flux avec ordre garanti par clé et plusieurs groupes de consommateurs indépendants (IOT-02) ; un simple fichier ou une file PostgreSQL ne permet pas deux lectures indépendantes avec offsets.
- **Spark + Iceberg** : MERGE, time travel et fichiers partitionnés sur du stockage objet ; permet de corriger les minutes touchées par les messages en retard.
- **dbt + DuckDB** : modèles Gold en SQL, tests, documentation ; DuckDB lit le Parquet exporté sans serveur supplémentaire.
- **Soda Core** : contrôles de qualité déclaratifs avec codes de sortie exploitables par l'orchestrateur ; les tests dbt ne contrôlent pas la fraîcheur du flux live.
- **Dagster** : planification, relances, historique des exécutions et échec géré ; un cron n'offre ni l'historique ni la visualisation.
- **PostgreSQL** : table live lue par Grafana (clé primaire pour l'idempotence) et catalogue JDBC d'Iceberg.
- **Grafana** : rafraîchissement en secondes, jauges, alerte avec durée (« pendant 10 min ») ; Streamlit n'a pas d'alerte native.
- **Streamlit** : exploration interactive de l'historique Gold et du classement de fiabilité.
- **Isolation Forest (scikit-learn)** : détecte des anomalies contextuelles que des seuils fixes ne voient pas.

## Grain des tables
| Table | Grain |
|---|---|
| `lake.silver.measurements_1min` | un capteur sur une minute |
| `lake.silver.measurements_rejected` | un message rejeté (null, sentinel, out_of_range) |
| `dim_sensor` | un capteur |
| `gold_greenhouse_5min` | une serre × une mesure × une fenêtre de 5 min |
| `gold_greenhouse_1h` | une serre × une mesure × une heure |
| `gold_sensor_availability` | un capteur × un jour |
| `gold_alert_periods` | une période continue de dépassement > 35 °C dans une serre |
| `gold_sensor_quality` | un capteur (période entière) |

## Décisions à citer (une phrase chacune)
Sentinelle testée avant la conversion °F ; dédoublonnage par premier message reçu ; export Parquet car DuckDB ne lit pas un catalogue Iceberg JDBC ; Dagster dans un venv séparé (conflits de versions) ; Soda live non bloquant ; un seul run Dagster à la fois (verrou DuckDB, mémoire Spark) ; Streamlit lit un instantané du Gold ; anomalies de l'expérience B injectées par le script.
