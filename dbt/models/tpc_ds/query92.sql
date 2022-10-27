-- query92
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         Sum(ws_ext_discount_amt) AS excess_discount_amount
FROM     {{source('src__tpc_ds', 'web_sales')}} ,
         {{source('src__tpc_ds', 'item')}} ,
         {{source('src__tpc_ds', 'date_dim')}}
WHERE    i_manufact_id = 718
AND      i_item_sk = ws_item_sk
AND      Cast(d_date AS DATE) BETWEEN Cast('2002-03-29' AS DATE) AND      (
                  Cast('2002-06-28' AS DATE))
AND      d_date_sk = ws_sold_date_sk
AND      ws_ext_discount_amt >
         (
                SELECT 1.3 * avg(ws_ext_discount_amt)
                FROM   {{source('src__tpc_ds', 'web_sales')}} ,
                       {{source('src__tpc_ds', 'date_dim')}}
                WHERE  ws_item_sk = i_item_sk
                AND    Cast(d_date AS DATE) BETWEEN Cast('2002-03-29' AS DATE) AND    (
                              cast('2002-06-28' AS date))
                AND    d_date_sk = ws_sold_date_sk )
ORDER BY sum(ws_ext_discount_amt)
{{ 'LIMIT 100' if target.type != 'synapse' }}

