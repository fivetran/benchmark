
-- query90
SELECT {{ 'TOP 100' if target.name == 'synapse' }} amc / pmc AS am_pm_ratio
FROM   (SELECT Count(*) amc
        FROM   {{source('src__tpc_ds', 'web_sales')}},
               {{source('src__tpc_ds', 'household_demographics')}},
               {{source('src__tpc_ds', 'time_dim')}},
               {{source('src__tpc_ds', 'web_page')}}
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 12 AND 12 + 1
               AND household_demographics.hd_dep_count = 8
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1,
       (SELECT Count(*) pmc
        FROM   {{source('src__tpc_ds', 'web_sales')}},
               {{source('src__tpc_ds', 'household_demographics')}},
               {{source('src__tpc_ds', 'time_dim')}},
               {{source('src__tpc_ds', 'web_page')}}
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 20 AND 20 + 1
               AND household_demographics.hd_dep_count = 8
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY am_pm_ratio
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
