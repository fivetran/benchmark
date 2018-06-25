-- query97
SELECT * 
FROM   (SELECT ss_customer_sk customer_sk, 
                ss_item_sk     item_sk 
         FROM   store_sales, 
                date_dim 
         WHERE  ss_sold_date_sk = d_date_sk 
                AND d_month_seq BETWEEN 1196 AND 1196 + 11 
         GROUP  BY ss_customer_sk, 
                   ss_item_sk) 
       UNION ALL (SELECT cs_bill_customer_sk customer_sk, 
                cs_item_sk          item_sk 
         FROM   catalog_sales, 
                date_dim 
         WHERE  cs_sold_date_sk = d_date_sk 
                AND d_month_seq BETWEEN 1196 AND 1196 + 11 
         GROUP  BY cs_bill_customer_sk, 
                   cs_item_sk)               
LIMIT 100; 