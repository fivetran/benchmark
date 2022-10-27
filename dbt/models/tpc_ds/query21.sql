-- query21
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         *
FROM    (
                  SELECT   w_warehouse_name ,
                           i_item_id ,
                           Sum(
                           CASE
                                    WHEN (
                                                      Cast(d_date AS DATE) < Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                                    ELSE 0
                           END) AS inv_before ,
                           Sum(
                           CASE
                                    WHEN (
                                                      Cast(d_date AS DATE) >= Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                                    ELSE 0
                           END) AS inv_after
                  FROM     {{source('src__tpc_ds', 'inventory')}} ,
                           {{source('src__tpc_ds', 'warehouse')}} ,
                           {{source('src__tpc_ds', 'item')}} ,
                           {{source('src__tpc_ds', 'date_dim')}}
                  WHERE    i_current_price BETWEEN 0.99 AND      1.49
                  AND      i_item_sk = inv_item_sk
                  AND      inv_warehouse_sk = w_warehouse_sk
                  AND      inv_date_sk = d_date_sk
                  AND      Cast(d_date AS DATE) BETWEEN (Cast ('2000-04-13' AS DATE)) AND      (
                                    cast ('2000-06-13' AS        date))
                  GROUP BY w_warehouse_name,
                           i_item_id) x
WHERE    (
                  CASE
                           WHEN inv_before > 0 THEN inv_after / inv_before
                           ELSE NULL
                  END) BETWEEN 2.0/3.0 AND      3.0/2.0
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY w_warehouse_name ,
         i_item_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

