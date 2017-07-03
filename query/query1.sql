-- query1
WITH customer_total_return 
     AS (SELECT sr_customer_sk     AS ctr_customer_sk, 
                sr_store_sk        AS ctr_store_sk, 
                Sum(sr_return_amt) AS ctr_total_return 
         FROM   store_returns, 
                date_dim 
         WHERE  sr_returned_date_sk = d_date_sk 
                AND d_year = 2001 
         GROUP  BY sr_customer_sk, 
                   sr_store_sk),
high_return AS (
    SELECT ctr_store_sk, Avg(ctr_total_return) * 1.2 AS return_limit
    FROM   customer_total_return ctr2 
    GROUP BY ctr_store_sk
)
SELECT c_customer_id 
FROM   customer_total_return ctr1, 
       store, 
       customer,
       high_return 
WHERE  ctr1.ctr_total_return > high_return.return_limit
       AND s_store_sk = ctr1.ctr_store_sk 
       AND s_state = 'TN' 
       AND ctr1.ctr_customer_sk = c_customer_sk 
       AND ctr1.ctr_store_sk = high_return.ctr_store_sk
ORDER  BY c_customer_id
LIMIT 100;
