-- query1
SELECT c_customer_id, sr_return_amt
FROM   store_returns, 
       date_dim,
       store, 
       customer
WHERE  sr_returned_date_sk = d_date_sk 
       AND s_store_sk = sr_store_sk 
       AND sr_customer_sk = c_customer_sk
ORDER  BY sr_return_amt DESC
LIMIT 100;
