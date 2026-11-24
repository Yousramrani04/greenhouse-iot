#!/usr/bin/env bash
# Enchaîne à la main la chaîne que Dagster automatisera (étape 6) :
#   Bronze -> Silver (Spark) -> export Parquet -> Gold (dbt) -> contrôles (Soda)
# Usage : bash scripts/refresh_chain.sh
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
mkdir -p logs

spark_job() {                      # spark_job <script.py> : journal complet dans logs/<script>.log
  local log="logs/$1.log"
  echo "==> Spark : $1"
  if ! docker compose run --rm spark /opt/spark/bin/spark-submit "/opt/jobs/$1" > "$log" 2>&1; then
    echo "ÉCHEC de $1 ; dernières lignes :"; tail -n 40 "$log"; exit 1
  fi
  grep -v "INFO\|WARN\|SLF4J\|^$\|Container\|Using Spark" "$log" | tail -n 12 || true
}

soda_scan() {                      # soda_scan <source> <fichier> <bloquant: oui|non>
  echo "==> Soda : $2"
  set +e
  soda scan -d "$1" -c soda/configuration.yml "$2" 2>&1 | grep -E "checks PASSED|WARNED|FAILED|Oops|All is good|Only"
  local code=${PIPESTATUS[0]}
  set -e
  echo "    code de sortie Soda : $code (0=ok, 1=avertissement, 2=échec, 3=erreur)"
  if [ "$code" -ge 2 ] && [ "$3" = "oui" ]; then echo "Contrôle bloquant en échec : arrêt."; exit "$code"; fi
}

spark_job silver_job.py
spark_job export_silver.py

echo "==> dbt build"
( cd dbt && dbt build --profiles-dir . | tail -n 3 )

soda_scan silver soda/checks_silver.yml oui
soda_scan live   soda/checks_live.yml   non
echo "Chaîne terminée."
