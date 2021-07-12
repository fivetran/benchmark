
-- query81
WITH customer_total_return 
     AS (SELECT cr_returning_customer_sk   AS ctr_customer_sk, 
                ca_state                   AS ctr_state, 
                Sum(cr_return_amt_inc_tax) AS ctr_total_return 
         FROM   catalog_returns, 
                date_dim, 
                customer_address 
         WHERE  cr_returned_date_sk = d_date_sk 
                AND d_year = 1999 
                AND cr_returning_addr_sk = ca_address_sk 
         GROUP  BY cr_returning_customer_sk, 
                   ca_state),
high_return AS (
    SELECT ctr_state AS hr_state, Avg(ctr_total_return) * 1.2 AS hr_limit
    FROM   customer_total_return 
    GROUP BY ctr_state
)
SELECT c_customer_id, 
               c_salutation, 
               c_first_name, 
               c_last_name, 
               ca_street_number, 
               ca_street_name, 
               ca_street_type, 
               ca_suite_number, 
               ca_city, 
               ca_county, 
               ca_state, 
               ca_zip, 
               ca_country, 
               ca_gmt_offset, 
               ca_location_type, 
               ctr_total_return 
FROM   customer_total_return, 
       high_return,
       customer_address, 
       customer
WHERE  ctr_state = hr_state 
       AND ctr_customer_sk = c_customer_sk 
       AND ca_address_sk = c_current_addr_sk 
       AND ca_state = 'TX' 
       AND ctr_total_return > hr_limit
ORDER  BY c_customer_id, 
          c_salutation, 
          c_first_name, 
          c_last_name, 
          ca_street_number, 
          ca_street_name, 
          ca_street_type, 
          ca_suite_number, 
          ca_city, 
          ca_county, 
          ca_state, 
          ca_zip, 
          ca_country, 
          ca_gmt_offset, 
          ca_location_type, 
          ctr_total_return
LIMIT 100; 
