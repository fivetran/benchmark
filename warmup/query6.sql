-- query6

SELECT a.ca_state state, 
               Count(*)   cnt 
FROM   
       customer_address a, 
       customer c, 
       store_sales s, 
       date_dim d, 
       item i 
WHERE  a.ca_address_sk = c.c_current_addr_sk 
       AND c.c_customer_sk = s.ss_customer_sk 
       AND s.ss_sold_date_sk = d.d_date_sk 
       AND s.ss_item_sk = i.i_item_sk 
       
GROUP  BY a.ca_state 
HAVING Count(*) >= 10 

LIMIT 100; 
