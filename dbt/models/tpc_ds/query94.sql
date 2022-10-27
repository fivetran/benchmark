-- query94
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         Count(DISTINCT ws_order_number) AS order_count,
         Sum(ws_ext_ship_cost)           AS total_shipping_cost ,
         Sum(ws_net_profit)              AS total_net_profit
FROM     {{source('src__tpc_ds', 'web_sales')}} ws1 ,
         {{source('src__tpc_ds', 'date_dim')}} ,
         {{source('src__tpc_ds', 'customer_address')}} ,
         {{source('src__tpc_ds', 'web_site')}}
WHERE    Cast(d_date AS DATE) BETWEEN Cast('2000-3-01' AS DATE) AND      (
                  Cast('2000-5-01' AS DATE))
AND      ws1.ws_ship_date_sk = d_date_sk
AND      ws1.ws_ship_addr_sk = ca_address_sk
AND      ca_state = 'MT'
AND      ws1.ws_web_site_sk = web_site_sk
AND      web_company_name = 'pri'
AND      EXISTS
         (
                SELECT *
                FROM   {{source('src__tpc_ds', 'web_sales')}} ws2
                WHERE  ws1.ws_order_number = ws2.ws_order_number
                AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   {{source('src__tpc_ds', 'web_returns')}} wr1
                WHERE  ws1.ws_order_number = wr1.wr_order_number)
ORDER BY count(DISTINCT ws_order_number)
{{ 'LIMIT 100' if target.type != 'synapse' }}

