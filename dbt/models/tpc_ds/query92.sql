-- query92
SELECT 
         Sum(ws_ext_discount_amt) AS excess_discount_amount
FROM     web_sales , 
         item , 
         date_dim 
WHERE    i_manufact_id = 718 
AND      i_item_sk = ws_item_sk 
AND      Cast(d_date AS DATE) BETWEEN Cast('2002-03-29' AS DATE) AND      ( 
                  Cast('2002-06-28' AS DATE)) 
AND      d_date_sk = ws_sold_date_sk 
AND      ws_ext_discount_amt > 
         ( 
                SELECT 1.3 * avg(ws_ext_discount_amt) 
                FROM   web_sales , 
                       date_dim 
                WHERE  ws_item_sk = i_item_sk 
                AND    Cast(d_date AS DATE) BETWEEN Cast('2002-03-29' AS DATE) AND    ( 
                              cast('2002-06-28' AS date)) 
                AND    d_date_sk = ws_sold_date_sk ) 
ORDER BY sum(ws_ext_discount_amt) 
LIMIT 100; 

