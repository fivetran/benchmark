-- query52
SELECT {{ 'TOP 100' if target.name == 'synapse' }} dt.d_year,
               item.i_brand_id         brand_id,
               item.i_brand            brand,
               Sum(ss_ext_sales_price) ext_price
FROM   {{source('src__tpc_ds', 'date_dim')}} dt,
       {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 11
       AND dt.d_year = 1999
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY dt.d_year,
          ext_price DESC,
          brand_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
