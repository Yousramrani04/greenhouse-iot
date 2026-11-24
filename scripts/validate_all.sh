#!/usr/bin/env bash
# Liste de contrôle de toute la plateforme. Lancer avec le venv actif, depuis n'importe où.
# Écrit un journal dans docs/preuves/validation.txt. Ne modifie aucune donnée.
cd "$(dirname "$0")/.."
set -a; source .env; set +a
mkdir -p docs/preuves
OUT=docs/preuves/validation.txt
: > "$OUT"
ok=0; ko=0
check() {  # check "<libellé>" <commande...>
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "[OK] $label" | tee -a "$OUT"; ok=$((ok+1)); else echo "[KO] $label" | tee -a "$OUT"; ko=$((ko+1)); fi
}
info() { echo "     $*" | tee -a "$OUT"; }
echo "Validation du $(date -u '+%Y-%m-%d %H:%M:%S') UTC" | tee -a "$OUT"

# 1. Conteneurs
for s in silo kafka kafka-ui postgres grafana; do
  check "conteneur $s démarré" bash -c "docker compose ps --status running --services | grep -qx $s"
done

# 2. Kafka : topic et groupes
check "topic capteurs : 6 partitions" bash -c "docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic capteurs | grep -q 'PartitionCount: 6'"
GROUPS_OUT=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
check "groupe bronze-writer existe" bash -c "echo '$GROUPS_OUT' | grep -qx bronze-writer"
check "groupe grafana-writer existe" bash -c "echo '$GROUPS_OUT' | grep -qx grafana-writer"
for g in bronze-writer grafana-writer; do
  info "retard $g : $(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $g 2>/dev/null | awk 'NR>2 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}') messages"
done

# 3. PostgreSQL live
LIVE=$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "select count(*), count(distinct sensor_id), to_char(max(event_time) at time zone 'UTC','YYYY-MM-DD HH24:MI:SS') from live_measurements" 2>/dev/null)
info "live_measurements (lignes | capteurs | dernière mesure UTC) : $LIVE"
check "live_measurements non vide" bash -c "[ \"\$(echo '$LIVE' | cut -d'|' -f1)\" -gt 0 ]"
check "live : 60 capteurs ont émis" bash -c "[ \"\$(echo '$LIVE' | cut -d'|' -f2)\" -eq 60 ]"

# 4. Grafana
GU="${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD:-admin}"
GP="${GRAFANA_PORT:-3000}"
check "Grafana en bonne santé" bash -c "curl -sf http://localhost:$GP/api/health | grep -q '\"database\": \"ok\"'"
check "dashboard greenhouse-live provisionné" curl -sf -u "$GU" "http://localhost:$GP/api/dashboards/uid/greenhouse-live"
check "règle d'alerte provisionnée" curl -sf -u "$GU" "http://localhost:$GP/api/v1/provisioning/alert-rules/temp-sup-35-10min"
STATES=$(curl -s -u "$GU" "http://localhost:$GP/api/prometheus/grafana/api/v1/rules" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin); out=[]
    for g in d['data']['groups']:
        for r in g['rules']:
            for a in r.get('alerts', []) or [{'state': r.get('state'), 'labels': {}}]:
                out.append(r['name'] + ' / ' + str(a['labels'].get('metric','-')) + ' : ' + str(a['state']))
    print('; '.join(out) or 'aucune alerte évaluée')
except Exception as e:
    print('lecture impossible', e)")
info "état des alertes : $STATES"

# 5. Dagster, Streamlit (informatif : doivent tourner pour la démo)
check "Dagster répond (http://localhost:3100)" curl -sf -o /dev/null http://localhost:3100
check "Streamlit répond (http://localhost:8501)" curl -sf -o /dev/null http://localhost:8501

# 6. Gold
GOLD=$(python - <<'PY' 2>&1
import duckdb
c = duckdb.connect("dbt/gold.duckdb", read_only=True)
q = lambda s: c.sql(s).fetchone()[0]
print(q("select count(*) from dim_sensor"), q("select count(*) from gold_sensor_quality"),
      q("select max(minute_ts) from stg_measurements"))
PY
)
info "Gold (capteurs | fiabilité | dernière minute) : $GOLD"
check "Gold : 60 capteurs et 60 lignes de fiabilité" bash -c "echo '$GOLD' | grep -q '^60 60 '"

# 7. Qualité : Soda (0 ou 1 acceptés)
for pair in "silver soda/checks_silver.yml" "live soda/checks_live.yml"; do
  set -- $pair
  soda scan -d "$1" -c soda/configuration.yml "$2" > "docs/preuves/soda_$1.txt" 2>&1; code=$?
  info "Soda $1 : code $code (0 ok, 1 avertissement, 2 échec, 3 erreur) ; détail : docs/preuves/soda_$1.txt"
  [ "$code" -le 1 ] && { echo "[OK] Soda $1 sans échec" | tee -a "$OUT"; ok=$((ok+1)); } || { echo "[KO] Soda $1 en échec" | tee -a "$OUT"; ko=$((ko+1)); }
done

# 8. Tests et livrables
check "pytest" python -m pytest -q
check "résultats IOT-09 présents" test -s docs/iot09_isolation_forest.md
check ".env absent du dépôt git" bash -c "! git ls-files --error-unmatch .env"
check "aucun secret dans .env.example" bash -c "! grep -E '^(MINIO_ROOT_PASSWORD|POSTGRES_PASSWORD|GRAFANA_ADMIN_PASSWORD)=' .env.example | grep -vq change_me"

echo "Résultat : $ok OK, $ko KO" | tee -a "$OUT"
[ "$ko" -eq 0 ]
