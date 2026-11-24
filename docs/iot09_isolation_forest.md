# IOT-09 : Isolation Forest comparé aux seuils fixes

Source : minio ; graine 42 ; contamination 0.01 ; entraînement : 5 premiers jours ; évaluation : le reste.

## A. Défauts réels du générateur (aberrants + sentinelle -999)
Évalué sur 338469 mesures dont 1857 anomalies.

| Méthode | Précision | Rappel | F1 | ROC-AUC | VP | FP | FN |
|---|---|---|---|---|---|---|---|
| Seuils fixes | 1.000 | 1.000 | 1.000 | n/a | 1857 | 0 | 0 |
| Isolation Forest | 0.529 | 1.000 | 0.692 | 0.999 | 1857 | 1652 | 0 |

## B. Anomalies contextuelles injectées (valeurs plausibles, anormales à l'heure considérée)
Évalué sur 336612 mesures dont 1684 anomalies injectées (décalages : {'temperature': 8.0, 'air_humidity': -25.0, 'soil_humidity': -20.0, 'co2': 900.0}).

| Méthode | Précision | Rappel | F1 | ROC-AUC | VP | FP | FN |
|---|---|---|---|---|---|---|---|
| Seuils fixes | 0.000 | 0.000 | 0.000 | n/a | 0 | 0 | 1684 |
| Isolation Forest | 0.263 | 0.996 | 0.417 | 0.998 | 1678 | 4691 | 6 |
