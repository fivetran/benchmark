-- query19
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_brand_id              brand_id,
               i_brand                 brand,
               i_manufact_id,
               i_manufact,
               Sum(ss_ext_sales_price) ext_price
FROM   {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'item')}},
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'store')}}
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 38
       AND d_moy = 12
       AND d_year = 1998
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND {{ substr('ca_zip', 1, 5) }} <> {{ substr('s_zip', 1, 5) }}
       AND ss_store_sk = s_store_sk
GROUP  BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
