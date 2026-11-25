"""IOT-08 : historique d'une serre et classement des capteurs les moins fiables.

Lecture du Gold (dbt/gold.duckdb) SANS verrouiller le fichier : on travaille sur un instantané
copié dans le dossier temporaire. Si dbt est en train d'écrire, on garde l'instantané précédent.
Lancement : streamlit run app/streamlit_app.py
"""
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "dbt" / "gold.duckdb"
SNAP = Path(tempfile.gettempdir()) / "greenhouse_gold_snapshot.duckdb"
METRIC_LABELS = {"temperature": "Température (°C)", "air_humidity": "Humidité de l'air (%)",
                 "soil_humidity": "Humidité du sol (%)", "co2": "CO2 (ppm)"}

st.set_page_config(page_title="Serres connectées", layout="wide")


def _valid(path: Path) -> bool:
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.sql("select count(*) from gold_sensor_quality").fetchone()
        con.close()
        return True
    except Exception:
        return False


def snapshot() -> Path:
    if not SRC.exists():
        st.error("dbt/gold.duckdb introuvable : lance d'abord la chaîne (Dagster ou scripts/refresh_chain.sh).")
        st.stop()
    if not SNAP.exists() or SRC.stat().st_mtime > SNAP.stat().st_mtime:
        tmp = SNAP.with_name(SNAP.name + ".tmp")
        try:
            shutil.copy2(SRC, tmp)
            if _valid(tmp):
                os.replace(tmp, SNAP)
        except Exception:
            pass
        finally:
            tmp.unlink(missing_ok=True)
    if not SNAP.exists():
        st.warning("Le Gold est en cours de reconstruction (dbt) ou incomplet : réessaie dans quelques secondes.")
        st.stop()
    return SNAP


@st.cache_data(ttl=60)
def query(sql: str, version: float):
    con = duckdb.connect(str(SNAP), read_only=True)
    try:
        return con.sql(sql).df()
    finally:
        con.close()


snap = snapshot()
ver = snap.stat().st_mtime
st.title("Serres connectées : historique et fiabilité des capteurs")
st.caption(f"Source : Gold (dbt/DuckDB), instantané du {datetime.fromtimestamp(ver):%Y-%m-%d %H:%M:%S}")

tab1, tab2 = st.tabs(["Historique d'une serre", "Capteurs les moins fiables"])

with tab1:
    ghs = query("select distinct greenhouse_id from dim_sensor order by 1", ver)["greenhouse_id"].tolist()
    c1, c2, c3 = st.columns(3)
    gh = c1.selectbox("Serre", ghs)
    metric = c2.selectbox("Mesure", list(METRIC_LABELS), format_func=METRIC_LABELS.get)
    days = c3.slider("Derniers jours", 1, 8, 7)
    df = query(f"""select bucket_start, avg_value, min_value, max_value
                   from gold_greenhouse_1h
                   where greenhouse_id = '{gh}' and metric = '{metric}'
                     and bucket_start >= (select max(bucket_start) from gold_greenhouse_1h) - interval '{days} days'
                   order by 1""", ver)
    if df.empty:
        st.info("Aucune donnée pour cette sélection.")
    else:
        # heures sans mesure -> NaN : un trou apparaît au lieu d'une fausse ligne droite
        full = pd.date_range(df["bucket_start"].min(), df["bucket_start"].max(), freq="1h")
        df = df.set_index("bucket_start").reindex(full).rename_axis("bucket_start").reset_index()
        fig = px.line(df, x="bucket_start", y=["avg_value", "min_value", "max_value"],
                      labels={"bucket_start": "Heure (UTC)", "value": METRIC_LABELS[metric], "variable": ""})
        fig.update_traces(connectgaps=False)
        if metric == "temperature":
            fig.add_hline(y=35, line_dash="dash", line_color="red", annotation_text="Seuil 35 °C")
        st.plotly_chart(fig, use_container_width=True)
    if metric == "temperature":
        st.subheader("Périodes au-dessus de 35 °C")
        per = query(f"""select start_ts, end_ts, duration_minutes, round(peak_value, 1) as pic_c, is_alert
                        from gold_alert_periods where greenhouse_id = '{gh}' order by start_ts desc""", ver)
        st.dataframe(per, use_container_width=True) if not per.empty else st.success("Aucun dépassement.")

with tab2:
    n = st.slider("Nombre de capteurs affichés", 5, 60, 15)
    q = query(f"""select sensor_id, greenhouse_id, metric, availability_pct, n_null, n_sentinel,
                         n_out_of_range, n_rejected, rejection_pct, reliability_pct
                  from gold_sensor_quality order by reliability_pct asc, sensor_id limit {n}""", ver)
    st.caption("Fiabilité = disponibilité × (1 − part de messages rejetés). Plus bas = moins fiable. L'axe est zoomé : les écarts sont de quelques points.")
    fig2 = px.bar(q, x="reliability_pct", y="sensor_id", orientation="h", color="metric",
                  labels={"reliability_pct": "Fiabilité (%)", "sensor_id": "Capteur"})
    # ordre des barres = ordre du classement (sinon plotly les regroupe par couleur), axe zoomé sur les valeurs
    fig2.update_yaxes(autorange="reversed", categoryorder="array", categoryarray=q["sensor_id"].tolist())
    fig2.update_xaxes(range=[max(0.0, float(q["reliability_pct"].min()) - 2), 100])
    st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(q, use_container_width=True)
