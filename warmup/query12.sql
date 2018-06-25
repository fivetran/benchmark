-- query12
-- query12
SELECT
         i_item_id , 
         i_item_desc , 
         i_category ,
         i_current_price , 
         Sum(ws_ext_sales_price)                                                              AS itemrevenue ,
         Sum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     web_sales , 
         item , 
         date_dim 
WHERE    ws_item_sk = i_item_sk 
GROUP BY i_item_id , 
         i_item_desc , 
         i_category , 
         i_class , 
         i_current_price 

LIMIT 100; 