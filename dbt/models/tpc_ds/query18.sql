-- query18
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_item_id,
               ca_country,
               ca_state,
               ca_county,
               Avg(cs_quantity)      agg1,
               Avg(cs_list_price)    agg2,
               Avg(cs_coupon_amt)    agg3,
               Avg(cs_sales_price)   agg4,
               Avg(cs_net_profit)    agg5,
               Avg(c_birth_year)     agg6,
               Avg(cd1.cd_dep_count) agg7
FROM   {{source('src__tpc_ds', 'catalog_sales')}},
       {{source('src__tpc_ds', 'customer_demographics')}} cd1,
       {{source('src__tpc_ds', 'customer_demographics')}} cd2,
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd1.cd_demo_sk
       AND cs_bill_customer_sk = c_customer_sk
       AND cd1.cd_gender = 'F'
       AND cd1.cd_education_status = 'Secondary'
       AND c_current_cdemo_sk = cd2.cd_demo_sk
       AND c_current_addr_sk = ca_address_sk
       AND c_birth_month IN ( 8, 4, 2, 5,
                              11, 9 )
       AND d_year = 2001
       AND ca_state IN ( 'KS', 'IA', 'AL', 'UT',
                         'VA', 'NC', 'TX' )
GROUP  BY i_item_id, ca_country, ca_state, ca_county
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ca_country,
          ca_state,
          ca_county,
          i_item_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
