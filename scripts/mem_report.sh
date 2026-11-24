#!/usr/bin/env bash
# Relevé mémoire pour le rapport (plateforme en marche). Écrit docs/preuves/memoire.txt.
cd "$(dirname "$0")/.."
mkdir -p docs/preuves
{
  echo "Relevé du $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
  echo; echo "== Conteneurs (docker stats) =="
  docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"
  echo; echo "== Processus hors Docker (Dagster, Streamlit, producteur, consommateurs), RSS en Mo =="
  ps -eo rss,args | grep -E "dagster|streamlit|producer_capteurs|bronze_writer|grafana_writer" | grep -v grep \
    | awk '{printf "%7.0f Mo  ", $1/1024; $1=""; print substr($0,1,90)}'
  echo; echo "== Mémoire de la machine =="
  free -h
} | tee docs/preuves/memoire.txt
