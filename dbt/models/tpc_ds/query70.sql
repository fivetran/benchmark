-- query70
SELECT {{ 'TOP 100' if target.name == 'synapse' }} Sum(ss_net_profit)                     AS total_sum,
               s_state,
               s_county,
               Rank()
                 OVER (
                   PARTITION BY s_state, s_county
                   ORDER BY Sum(ss_net_profit) DESC)  AS rank_within_parent
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'date_dim')}} d1,
       {{source('src__tpc_ds', 'store')}}
WHERE  d1.d_month_seq BETWEEN 1200 AND 1200 + 11
       AND d1.d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN (SELECT s_state
                       FROM   (SELECT s_state                               AS
                                      s_state,
                                      Rank()
                                        OVER (
                                          partition BY s_state
                                          ORDER BY Sum(ss_net_profit) DESC) AS
                                      ranking
                               FROM   {{source('src__tpc_ds', 'store_sales')}},
                                      {{source('src__tpc_ds', 'store')}},
                                      {{source('src__tpc_ds', 'date_dim')}}
                               WHERE  d_month_seq BETWEEN 1200 AND 1200 + 11
                                      AND d_date_sk = ss_sold_date_sk
                                      AND s_store_sk = ss_store_sk
                               GROUP  BY s_state) tmp1
                       WHERE  ranking <= 5)
GROUP  BY s_state, s_county
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY s_state,
          rank_within_parent
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
