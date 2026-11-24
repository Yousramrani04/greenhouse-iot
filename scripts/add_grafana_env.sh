#!/usr/bin/env bash
# Ajoute les variables Grafana à .env.example et à .env si elles manquent.
for f in .env.example .env; do
  [ -f "$f" ] || continue
  grep -q '^GRAFANA_ADMIN_USER=' "$f" || printf '\n# --- Grafana ---\nGRAFANA_PORT=3000\nGRAFANA_ADMIN_USER=admin\nGRAFANA_ADMIN_PASSWORD=change_me_grafana\n' >> "$f"
done
echo "variables Grafana ok"
