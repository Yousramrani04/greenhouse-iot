#!/usr/bin/env bash
cd "$(dirname "$0")/.."
for l in ".venv-dagster/" ".dagster_home/"; do grep -qxF "$l" .gitignore || printf '%s\n' "$l" >> .gitignore; done
echo ".gitignore ok"
