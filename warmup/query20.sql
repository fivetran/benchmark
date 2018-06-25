 -- query20
SELECT *
FROM     catalog_sales , 
         item , 
         date_dim 
WHERE    cs_item_sk = i_item_sk 
AND      i_category IN ('Children', 
                        'Women', 
                        'Electronics') 
AND      cs_sold_date_sk = d_date_sk 
AND      Cast(d_date AS DATE) BETWEEN Cast('2001-02-03' AS DATE) AND      ( 
                  Cast('2001-03-03' AS DATE)) 
LIMIT 100; 

