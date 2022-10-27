

-- query99
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
               {{ substr('w_warehouse_name', 1, 20) }} as substr_w_warehouse_name,
               sm_type,
               cc_name,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS days_30,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 30 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 60 ) THEN 1
                     ELSE 0
                   END) AS days_31_60,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 60 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 90 ) THEN 1
                     ELSE 0
                   END) AS days_61_90,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 90 )
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 120 ) THEN
                     1
                     ELSE 0
                   END) AS days_91_120,
               Sum(CASE
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS days_over_120
FROM   {{source('src__tpc_ds', 'catalog_sales')}},
       {{source('src__tpc_ds', 'warehouse')}},
       {{source('src__tpc_ds', 'ship_mode')}},
       {{source('src__tpc_ds', 'call_center')}},
       {{source('src__tpc_ds', 'date_dim')}}
WHERE  d_month_seq BETWEEN 1200 AND 1200 + 11
       AND cs_ship_date_sk = d_date_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_ship_mode_sk = sm_ship_mode_sk
       AND cs_call_center_sk = cc_call_center_sk
GROUP  BY {{ substr('w_warehouse_name', 1, 20) }}, sm_type, cc_name
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY 1, 2, 3
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
