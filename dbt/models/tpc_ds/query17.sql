-- query17
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_item_id,
               i_item_desc,
               s_state,
               Count(ss_quantity)                                        AS
               store_sales_quantitycount,
               Avg(ss_quantity)                                          AS
               store_sales_quantityave,
               {{ stddev('ss_quantity') }}                                  AS
               store_sales_quantitystdev,
               {{ stddev('ss_quantity') }} / Avg(ss_quantity)               AS
               store_sales_quantitycov,
               Count(sr_return_quantity)                                 AS
               store_returns_quantitycount,
               Avg(sr_return_quantity)                                   AS
               store_returns_quantityave,
               {{ stddev('sr_return_quantity') }}                           AS
               store_returns_quantitystdev,
               {{ stddev('sr_return_quantity') }} / Avg(sr_return_quantity) AS
               store_returns_quantitycov,
               Count(cs_quantity)                                        AS
               catalog_sales_quantitycount,
               Avg(cs_quantity)                                          AS
               catalog_sales_quantityave,
               {{ stddev('cs_quantity') }} / Avg(cs_quantity)               AS
               catalog_sales_quantitystdev,
               {{ stddev('cs_quantity') }} / Avg(cs_quantity)               AS
               catalog_sales_quantitycov
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'store_returns')}},
       {{source('src__tpc_ds', 'catalog_sales')}},
       {{source('src__tpc_ds', 'date_dim')}} d1,
       {{source('src__tpc_ds', 'date_dim')}} d2,
       {{source('src__tpc_ds', 'date_dim')}} d3,
       {{source('src__tpc_ds', 'store')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  d1.d_quarter_name = '1999Q1'
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' )
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' )
GROUP  BY i_item_id,
          i_item_desc,
          s_state
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY i_item_id,
          i_item_desc,
          s_state
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
