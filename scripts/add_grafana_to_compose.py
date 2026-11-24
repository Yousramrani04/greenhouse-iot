"""Ajoute le service grafana et le volume grafana_data à docker-compose.(yaml|yml). Idempotent."""
import pathlib, sys
p = next((pathlib.Path(n) for n in ("docker-compose.yml", "docker-compose.yaml") if pathlib.Path(n).exists()), None)
if p is None: sys.exit("docker-compose introuvable : lance le script depuis la racine du projet")
raw = p.read_bytes().decode("utf-8")
nl = "\r\n" if "\r\n" in raw else "\n"
s = raw.replace("\r\n", "\n")
if "  grafana:\n" in s:
    print("grafana déjà présent"); sys.exit(0)
svc = """  grafana:
    image: grafana/grafana-oss:11.4.0
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "${GRAFANA_PORT:-3000}:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    mem_limit: 512m

"""
assert s.count("\nvolumes:\n") == 1, "bloc volumes: introuvable"
s = s.replace("\nvolumes:\n", "\n" + svc + "volumes:\n")
s = s.rstrip("\n") + "\n  grafana_data:\n"
p.write_bytes(s.replace("\n", nl).encode("utf-8"))
print("service grafana ajouté à", p)
