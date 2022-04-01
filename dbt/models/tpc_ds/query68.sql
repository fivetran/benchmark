-- query68
SELECT {{ 'TOP 100' if target.name == 'synapse' }} c_last_name,
               c_first_name,
               ca_city,
               bought_city,
               ss_ticket_number,
               extended_price,
               extended_tax,
               list_price
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               ca_city                 bought_city,
               Sum(ss_ext_sales_price) extended_price,
               Sum(ss_ext_list_price)  list_price,
               Sum(ss_ext_tax)         extended_tax
        FROM   {{source('src__tpc_ds', 'store_sales')}},
               {{source('src__tpc_ds', 'date_dim')}},
               {{source('src__tpc_ds', 'store')}},
               {{source('src__tpc_ds', 'household_demographics')}},
               {{source('src__tpc_ds', 'customer_address')}}
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk
               AND date_dim.d_dom BETWEEN 1 AND 2
               AND ( household_demographics.hd_dep_count = 8
                      OR household_demographics.hd_vehicle_count = 3 )
               AND date_dim.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
               AND store.s_city IN ( 'Fairview', 'Midway' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  ca_city) dn,
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}} current_addr
WHERE  ss_customer_sk = c_customer_sk
       AND customer.c_current_addr_sk = current_addr.ca_address_sk
       AND current_addr.ca_city <> bought_city
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY c_last_name,
          ss_ticket_number
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
