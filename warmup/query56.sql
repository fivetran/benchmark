 -- query56
SELECT  *
FROM  store_sales, 
      date_dim, 
      customer_address, 
      item,
      web_sales 
      
WHERE  i_item_id IN (SELECT i_item_id 
                  FROM   item 
                  WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                 ) 
    AND ss_item_sk = i_item_sk 
    AND ss_sold_date_sk = d_date_sk 
    AND d_year = 1998 
    AND d_moy = 3 
    AND ss_addr_sk = ca_address_sk 
    AND ca_gmt_offset = -6 
LIMIT 100; 