-- query79
SELECT {{ 'TOP 100' if target.name == 'synapse' }} c_last_name,
               c_first_name,
               {{ substr('s_city', 1, 30) }} as substr_s_city,
               ss_ticket_number,
               amt,
               profit
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               store.s_city,
               Sum(ss_coupon_amt) amt,
               Sum(ss_net_profit) profit
        FROM   {{source('src__tpc_ds', 'store_sales')}},
               {{source('src__tpc_ds', 'date_dim')}},
               {{source('src__tpc_ds', 'store')}},
               {{source('src__tpc_ds', 'household_demographics')}}
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ( household_demographics.hd_dep_count = 8
                      OR household_demographics.hd_vehicle_count > 4 )
               AND date_dim.d_dow = 1
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 )
               AND store.s_number_employees BETWEEN 200 AND 295
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  store.s_city) ms,
       {{source('src__tpc_ds', 'customer')}}
WHERE  ss_customer_sk = c_customer_sk
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY c_last_name,
          c_first_name,
 {{ substr('s_city', 1, 30) }},
          profit
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
