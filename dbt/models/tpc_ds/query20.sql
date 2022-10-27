-- query20
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price ,
         Sum(cs_ext_sales_price)                                                              AS itemrevenue ,
         Sum(cs_ext_sales_price)*100/Sum(Sum(cs_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     {{source('src__tpc_ds', 'catalog_sales')}} ,
         {{source('src__tpc_ds', 'item')}} ,
         {{source('src__tpc_ds', 'date_dim')}}
WHERE    cs_item_sk = i_item_sk
AND      i_category IN ('Children',
                        'Women',
                        'Electronics')
AND      cs_sold_date_sk = d_date_sk
AND      Cast(d_date AS DATE) BETWEEN Cast('2001-02-03' AS DATE) AND      (
                  Cast('2001-03-03' AS DATE))
GROUP BY i_item_id ,
         i_item_desc ,
         i_category ,
         i_class ,
         i_current_price
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY i_category ,
         i_class ,
         i_item_id ,
         i_item_desc ,
         revenueratio
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

