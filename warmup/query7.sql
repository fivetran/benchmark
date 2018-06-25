-- query7
SELECT i_item_id, 
               Avg(ss_quantity)    agg1, 
               Avg(ss_list_price)  agg2, 
               Avg(ss_coupon_amt)  agg3, 
               Avg(ss_sales_price) agg4 
FROM   store_sales, 
       customer_demographics, 
       date_dim, 
       item, 
       promotion 
WHERE  ss_sold_date_sk = d_date_sk 
GROUP  BY i_item_id 

LIMIT 100; 
