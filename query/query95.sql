-- query95
WITH ws_wh AS 
( 
       SELECT ws1.ws_order_number, 
              ws1.ws_warehouse_sk wh1, 
              ws2.ws_warehouse_sk wh2 
       FROM   web_sales ws1, 
              web_sales ws2 
       WHERE  ws1.ws_order_number = ws2.ws_order_number 
       AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) 
SELECT 
         Count(DISTINCT ws_order_number) AS order_count, 
         Sum(ws_ext_ship_cost)           AS total_shipping_cost, 
         Sum(ws_net_profit)              AS total_net_profit
FROM     web_sales ws1 , 
         date_dim , 
         customer_address , 
         web_site 
WHERE    Cast(d_date AS DATE) BETWEEN Cast('2000-4-01' AS DATE) AND      ( 
                  Cast('2000-6-01' AS DATE)) 
AND      ws1.ws_ship_date_sk = d_date_sk 
AND      ws1.ws_ship_addr_sk = ca_address_sk 
AND      ca_state = 'IN' 
AND      ws1.ws_web_site_sk = web_site_sk 
AND      web_company_name = 'pri' 
AND      ws1.ws_order_number IN 
         ( 
                SELECT ws_order_number 
                FROM   ws_wh) 
AND      ws1.ws_order_number IN 
         ( 
                SELECT wr_order_number 
                FROM   web_returns, 
                       ws_wh 
                WHERE  wr_order_number = ws_wh.ws_order_number) 
ORDER BY count(DISTINCT ws_order_number) 
LIMIT 100; 

