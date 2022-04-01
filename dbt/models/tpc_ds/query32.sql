-- query32
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
       Sum(cs_ext_discount_amt) AS excess_discount_amount
FROM   {{source('src__tpc_ds', 'catalog_sales')}} ,
       {{source('src__tpc_ds', 'item')}} ,
       {{source('src__tpc_ds', 'date_dim')}}
WHERE  i_manufact_id = 610
AND    i_item_sk = cs_item_sk
AND    Cast(d_date AS DATE) BETWEEN Cast('2001-03-04' AS DATE) AND    (
              Cast('2001-06-03' AS DATE))
AND    d_date_sk = cs_sold_date_sk
AND    cs_ext_discount_amt >
       (
              SELECT 1.3 * avg(cs_ext_discount_amt)
              FROM   {{source('src__tpc_ds', 'catalog_sales')}} ,
                     {{source('src__tpc_ds', 'date_dim')}}
              WHERE  cs_item_sk = i_item_sk
              AND    Cast(d_date AS DATE) BETWEEN Cast('2001-03-04' AS DATE) AND    (
                            Cast('2001-06-03' AS DATE))
              AND    d_date_sk = cs_sold_date_sk )
{{ 'LIMIT 100' if target.type != 'synapse' }}

