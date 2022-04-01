-- query96
SELECT {{ 'TOP 100' if target.name == 'synapse' }} Count(*) as cnt
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'household_demographics')}},
       {{source('src__tpc_ds', 'time_dim')}},
       {{source('src__tpc_ds', 'store')}}
WHERE  ss_sold_time_sk = time_dim.t_time_sk
       AND ss_hdemo_sk = household_demographics.hd_demo_sk
       AND ss_store_sk = s_store_sk
       AND time_dim.t_hour = 15
       AND time_dim.t_minute >= 30
       AND household_demographics.hd_dep_count = 7
       AND store.s_store_name = 'ese'
ORDER  BY Count(*)
{{ 'LIMIT 100' if target.type != 'synapse' }}
