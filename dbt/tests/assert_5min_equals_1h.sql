-- Cohérence interne : les fenêtres de 5 min et d'1 h comptent les mêmes mesures.
select a.n5, b.n60
from (select sum(n_readings) as n5  from {{ ref('gold_greenhouse_5min') }}) a,
     (select sum(n_readings) as n60 from {{ ref('gold_greenhouse_1h') }}) b
where a.n5 <> b.n60
