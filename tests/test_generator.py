"""Le générateur doit être reproductible (même graine = mêmes données) : base de toutes les preuves du rapport."""
import numpy as np
import pandas as pd

from src.common.sensors import FAHRENHEIT_GREENHOUSES, build_registry
from src.generator.ingest_capteurs import generate_sensor, plan_outages

TS = np.arange(np.datetime64("2026-11-16T00:00:00"), np.datetime64("2026-11-19T00:00:00"),
               np.timedelta64(30, "s"))


def _generate(seed: int, n_sensors: int = 6):
    rng = np.random.default_rng(seed)
    registry = build_registry(seed)
    outages = plan_outages(registry, TS[0], TS[-1] + np.timedelta64(30, "s"), rng)
    frames = [generate_sensor(s, TS, rng, outages.get(s.sensor_id))[0] for s in registry[:n_sensors]]
    return pd.concat(frames, ignore_index=True)


def test_registre_60_capteurs_dont_3_serres_en_fahrenheit():
    registry = build_registry(42)
    assert len(registry) == 60
    assert len({s.greenhouse_id for s in registry}) == 15
    f_sensors = [s for s in registry if s.unit == "F"]
    assert len(f_sensors) == len(FAHRENHEIT_GREENHOUSES) == 3
    assert {s.greenhouse_id for s in f_sensors} == {"G04", "G09", "G13"}


def test_meme_graine_memes_donnees():
    pd.testing.assert_frame_equal(_generate(42), _generate(42))


def test_graine_differente_donnees_differentes():
    a, b = _generate(42), _generate(43)
    assert not a["value"].fillna(0).equals(b["value"].fillna(0))


def test_taux_de_manquants_proche_de_2_pourcent():
    df = _generate(42, n_sensors=60)
    rate = df["value"].isna().mean()
    assert 0.01 < rate < 0.03
