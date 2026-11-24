#!/usr/bin/env bash
# Ajoute streamlit et plotly à requirements.txt s'ils manquent, puis les installe dans le venv actif.
cd "$(dirname "$0")/.."
for pkg in streamlit plotly; do grep -qix "$pkg" requirements.txt || echo "$pkg" >> requirements.txt; done
pip install --quiet streamlit plotly
python -c "import streamlit, plotly, duckdb; print('streamlit', streamlit.__version__, '| duckdb', duckdb.__version__)"
