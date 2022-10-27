-- query37
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         i_item_id ,
         i_item_desc ,
         i_current_price
FROM     {{source('src__tpc_ds', 'item')}},
         {{source('src__tpc_ds', 'inventory')}},
         {{source('src__tpc_ds', 'date_dim')}},
         {{source('src__tpc_ds', 'catalog_sales')}}
WHERE    i_current_price BETWEEN 20 AND      20 + 30
AND      inv_item_sk = i_item_sk
AND      d_date_sk=inv_date_sk
AND      Cast(d_date AS DATE) BETWEEN Cast('1999-03-06' AS DATE) AND Cast('1999-05-06' AS DATE)
AND      i_manufact_id IN (843,815,850,840)
AND      inv_quantity_on_hand BETWEEN 100 AND      500
AND      cs_item_sk = i_item_sk
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY i_item_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

