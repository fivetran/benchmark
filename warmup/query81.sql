-- query81
SELECT *          
FROM   (SELECT cr_returning_customer_sk   AS ctr_customer_sk, 
                ca_state                   AS ctr_state, 
                Sum(cr_return_amt_inc_tax) AS ctr_total_return
                
         FROM   catalog_returns, 
                date_dim, 
                customer_address 
         WHERE  cr_returned_date_sk = d_date_sk 
                AND d_year = 1999 
                AND cr_returning_addr_sk = ca_address_sk 
         GROUP  BY cr_returning_customer_sk, 
                   ca_state)

LIMIT 100; 