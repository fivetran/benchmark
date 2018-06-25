-- query38

SELECT DISTINCT c_last_name, 
                c_first_name, 
                d_date 
FROM   store_sales, 
       date_dim, 
       customer,
       catalog_sales,
       web_sales       
WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
       AND store_sales.ss_customer_sk = customer.c_customer_sk 
       AND d_month_seq BETWEEN 1188 AND 1188 + 11 
       AND catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
       AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
       AND d_month_seq BETWEEN 1188 AND 1188 + 11 
       AND web_sales.ws_sold_date_sk = date_dim.d_date_sk 
       AND web_sales.ws_bill_customer_sk = customer.c_customer_sk 
       AND d_month_seq BETWEEN 1188 AND 1188 + 11
LIMIT 100; 
