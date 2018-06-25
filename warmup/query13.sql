-- query13
SELECT Avg(ss_quantity), 
       Avg(ss_ext_sales_price), 
       Avg(ss_ext_wholesale_cost), 
       Sum(ss_ext_wholesale_cost) 
FROM   store_sales, 
       store, 
       customer_demographics, 
       household_demographics, 
       customer_address, 
       date_dim 
WHERE  s_store_sk = ss_store_sk 
       AND ss_sold_date_sk = d_date_sk 
       AND d_year = 2001; 
Limit 100; 