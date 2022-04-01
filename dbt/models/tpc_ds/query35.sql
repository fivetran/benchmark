-- query35
SELECT {{ 'TOP 100' if target.name == 'synapse' }} ca_state,
               cd_gender,
               cd_marital_status,
               cd_dep_count,
               Count(*) cnt1,
               {{ stddev('cd_dep_count') }} as s{{ 'Stddev_samp' if target.type != 'synapse' else 'Stdev' }}_cd_dep_count,
               Avg(cd_dep_count) as avg_cd_dep_count,
               Max(cd_dep_count) as max_cd_dep_count,
               cd_dep_employed_count,
               Count(*) cnt2,
               {{ stddev('cd_dep_employed_count') }} as {{ 'Stddev_samp' if target.type != 'synapse' else 'Stdev' }}_cd_dep_employed_count,
               Avg(cd_dep_employed_count) as avg_cd_dep_employed_count,
               Max(cd_dep_employed_count) as max_cd_dep_employed_count,
               cd_dep_college_count,
               Count(*) cnt3,
               {{ stddev('cd_dep_college_count') }} {{ 'Stddev_samp' if target.type != 'synapse' else 'Stdev' }}_cd_dep_college_count,
               Avg(cd_dep_college_count) as avg_cd_dep_college_count,
               Max(cd_dep_college_count) as max_cd_dep_college_count
FROM   {{source('src__tpc_ds', 'customer')}} c,
       {{source('src__tpc_ds', 'customer_address')}} ca,
       {{source('src__tpc_ds', 'customer_demographics')}}
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   {{source('src__tpc_ds', 'store_sales')}},
                          {{source('src__tpc_ds', 'date_dim')}}
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2001
                          AND d_qoy < 4)
       AND ( EXISTS (SELECT *
                     FROM   {{source('src__tpc_ds', 'web_sales')}},
                            {{source('src__tpc_ds', 'date_dim')}}
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 2001
                            AND d_qoy < 4)
              OR EXISTS (SELECT *
                         FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                                {{source('src__tpc_ds', 'date_dim')}}
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 2001
                                AND d_qoy < 4) )
GROUP  BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
