-- query45
SELECT {{ 'TOP 100' if target.name == 'synapse' }} ca_zip,
               ca_state,
               Sum(ws_sales_price) as sum_ws_sales_price
FROM   {{source('src__tpc_ds', 'web_sales')}},
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  ws_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ws_item_sk = i_item_sk
       AND ( {{ substr('ca_zip', 1, 5) }} IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
              OR i_item_id IN (SELECT i_item_id
                               FROM   {{source('src__tpc_ds', 'item')}}
                               WHERE  i_item_sk IN ( 2, 3, 5, 7,
                                                     11, 13, 17, 19,
                                                     23, 29 )) )
       AND ws_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 2000
GROUP  BY ca_zip,
          ca_state
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ca_zip,
          ca_state
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
