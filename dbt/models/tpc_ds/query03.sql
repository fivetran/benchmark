-- query3
SELECT {{ 'TOP 100' if target.name == 'synapse' }} dt.d_year,
               item.i_brand_id          brand_id,
               item.i_brand             brand,
               Sum(ss_ext_discount_amt) sum_agg
FROM   {{source('src__tpc_ds', 'date_dim')}} dt,
       {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manufact_id = 427
       AND dt.d_moy = 11
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY dt.d_year,
          sum_agg DESC,
          brand_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
