-- query27
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_item_id,
               s_state,
               Avg(ss_quantity)    agg1,
               Avg(ss_list_price)  agg2,
               Avg(ss_coupon_amt)  agg3,
               Avg(ss_sales_price) agg4
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'customer_demographics')}},
       {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'store')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_store_sk = s_store_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND cd_gender = 'M'
       AND cd_marital_status = 'D'
       AND cd_education_status = 'College'
       AND d_year = 2000
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN',
                        'TN', 'TN' )
GROUP  BY i_item_id, s_state
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY i_item_id,
          s_state
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
