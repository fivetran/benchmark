-- query62
SELECT {{ substr('w_warehouse_name', 1, 20) }} as substrname,
               sm_type,
               web_name,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS days_30,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 30 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 60 ) THEN 1
                     ELSE 0
                   END) AS days_31_60,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 60 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 90 ) THEN 1
                     ELSE 0
                   END) AS days_61_90,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 90 )
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 120 ) THEN
                     1
                     ELSE 0
                   END) AS days_91_120,
               Sum(CASE
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS days_over_120
FROM   {{source('src__tpc_ds', 'web_sales')}},
       {{source('src__tpc_ds', 'warehouse')}},
       {{source('src__tpc_ds', 'ship_mode')}},
       {{source('src__tpc_ds', 'web_site')}},
       {{source('src__tpc_ds', 'date_dim')}}
WHERE  d_month_seq BETWEEN 1222 AND 1222 + 11
       AND ws_ship_date_sk = d_date_sk
       AND ws_warehouse_sk = w_warehouse_sk
       AND ws_ship_mode_sk = sm_ship_mode_sk
       AND ws_web_site_sk = web_site_sk
GROUP  BY {{ substr('w_warehouse_name', 1, 20) }},
          sm_type,
          web_name
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY 1,
          sm_type,
          web_name
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
