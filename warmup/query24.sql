-- query24
SELECT c_last_name, 
       c_first_name, 
       s_store_name 
FROM   (SELECT *
FROM    store_sales, 
        store_returns, 
        store, 
        item, 
        customer, 
        customer_address 
WHERE  ss_ticket_number = sr_ticket_number 
        AND ss_item_sk = sr_item_sk 
        AND ss_customer_sk = c_customer_sk 
        AND ss_item_sk = i_item_sk 
        AND ss_store_sk = s_store_sk 
        AND c_birth_country = Upper(ca_country) 
        AND s_zip = ca_zip 
        AND s_market_id = 6)
WHERE  i_color = 'papaya' 
GROUP  BY c_last_name, 
          c_first_name, 
          s_store_name 