-- query30
SELECT         c_customer_id, 
               c_salutation, 
               c_first_name, 
               c_last_name, 
               c_preferred_cust_flag, 
               c_birth_day, 
               c_birth_month, 
               c_birth_year, 
               c_birth_country, 
               c_login, 
               c_email_address, 
               c_last_review_date
               
FROM  
      tpcds2.web_returns, 
      tpcds2.date_dim, 
      tpcds2.customer_address,
       tpcds2.customer 
WHERE  
       ca_address_sk = c_current_addr_sk 
       AND ca_state = 'IN' 
       
LIMIT 100; 