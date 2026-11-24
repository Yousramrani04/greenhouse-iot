#!/usr/bin/env bash
# Lance l'interface et le démon de planification Dagster : http://localhost:3100
set -euo pipefail
cd "$(dirname "$0")/.."
export DAGSTER_HOME="$PWD/.dagster_home"
mkdir -p "$DAGSTER_HOME" logs
cp orchestration/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
exec .venv-dagster/bin/dagster dev -w workspace.yaml -p 3100
