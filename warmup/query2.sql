-- query2
SELECT *    
FROM   web_sales,
        date_dim, 
       catalog_sales,
       (SELECT sold_date_sk, 
                sales_price 
         FROM   (SELECT ws_sold_date_sk    sold_date_sk, 
                        ws_ext_sales_price sales_price 
         FROM   web_sales) 
         UNION ALL 
         (SELECT cs_sold_date_sk    sold_date_sk, 
                 cs_ext_sales_price sales_price 
          FROM   catalog_sales))
Limit 100; 
