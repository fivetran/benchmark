-- query11
SELECT c_customer_id customer_id, 
                c_first_name customer_first_name 
                , 
                c_last_name customer_last_name, 
               
                c_birth_country 
                    customer_birth_country
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year 
       
Limit 100; 
