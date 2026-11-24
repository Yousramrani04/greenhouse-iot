-- Réconciliation Silver -> Gold : aucune mesure perdue ni comptée deux fois.
select s.total_silver, g.total_gold
from (select sum(n_readings) as total_silver from {{ ref('stg_measurements') }}) s,
     (select sum(n_readings) as total_gold   from {{ ref('gold_greenhouse_5min') }}) g
where s.total_silver <> g.total_gold
