-- query33
SELECT  * 
FROM   
      (SELECT  
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' ))) 
        UNION ALL 
        (SELECT 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )))
        UNION ALL (SELECT 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
               item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )))

LIMIT 100; 
