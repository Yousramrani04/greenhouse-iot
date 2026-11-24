#!/usr/bin/env bash
# Crée un venv séparé pour Dagster (évite les conflits de versions avec dbt, Soda et duckdb<1.1.0).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv-dagster
.venv-dagster/bin/pip install --quiet --upgrade pip
.venv-dagster/bin/pip install --quiet dagster dagster-webserver
.venv-dagster/bin/dagster --version
