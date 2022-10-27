-- query7
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_item_id,
               Avg(ss_quantity)    agg1,
               Avg(ss_list_price)  agg2,
               Avg(ss_coupon_amt)  agg3,
               Avg(ss_sales_price) agg4
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'customer_demographics')}},
       {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'item')}},
       {{source('src__tpc_ds', 'promotion')}}
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND ss_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'W'
       AND cd_education_status = '2 yr Degree'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 1998
GROUP  BY i_item_id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY i_item_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
