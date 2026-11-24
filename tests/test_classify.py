"""Classement d'une mesure live : mêmes règles que Silver (src/streaming/classify.py)."""
import math

import pytest

from src.streaming.classify import classify


def test_valeur_normale_celsius():
    assert classify("temperature", 24.0, "C") == (24.0, "ok")


def test_conversion_fahrenheit():
    v, status = classify("temperature", 75.2, "F")
    assert status == "ok" and v == pytest.approx(24.0)


def test_sentinelle_testee_avant_conversion():
    # -999 °F converti vaudrait -573 °C (hors plage) : la bonne réponse est "sentinel", pas "out_of_range"
    assert classify("temperature", -999.0, "F") == (None, "sentinel")


@pytest.mark.parametrize("value", [None, math.nan])
def test_valeur_manquante(value):
    assert classify("co2", value, "ppm") == (None, "null")


@pytest.mark.parametrize("metric,unit,low,high", [
    ("temperature", "C", -10.0, 60.0),
    ("air_humidity", "%", 0.0, 100.0),
    ("soil_humidity", "%", 0.0, 100.0),
    ("co2", "ppm", 0.0, 3000.0),
])
def test_bornes_incluses_et_hors_plage(metric, unit, low, high):
    assert classify(metric, low, unit)[1] == "ok"
    assert classify(metric, high, unit)[1] == "ok"
    assert classify(metric, low - 0.01, unit) == (None, "out_of_range")
    assert classify(metric, high + 0.01, unit) == (None, "out_of_range")
