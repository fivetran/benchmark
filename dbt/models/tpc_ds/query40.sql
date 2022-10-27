-- query40
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
                w_state ,
                i_item_id ,
                Sum(
                CASE
                                WHEN (
                                                                Cast(d_date AS DATE) < Cast ('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash,0)
                                ELSE 0
                END) AS sales_before ,
                Sum(
                CASE
                                WHEN (
                                                                Cast(d_date AS DATE) >= Cast ('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash,0)
                                ELSE 0
                END) AS sales_after
FROM            {{source('src__tpc_ds', 'catalog_sales')}}
LEFT OUTER JOIN {{source('src__tpc_ds', 'catalog_returns')}}
ON              (
                                cs_order_number = cr_order_number
                AND             cs_item_sk = cr_item_sk) ,
                {{source('src__tpc_ds', 'warehouse')}} ,
                {{source('src__tpc_ds', 'item')}} ,
                {{source('src__tpc_ds', 'date_dim')}}
WHERE           i_current_price BETWEEN 0.99 AND             1.49
AND             i_item_sk = cs_item_sk
AND             cs_warehouse_sk = w_warehouse_sk
AND             cs_sold_date_sk = d_date_sk
AND             Cast(d_date AS DATE) BETWEEN (Cast ('2002-05-01' AS DATE)) AND cast ('2002-07-01' AS date)
GROUP BY        w_state,
                i_item_id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY        w_state,
                i_item_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

