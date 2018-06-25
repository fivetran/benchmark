-- query58
SELECT * 
FROM   store_sales, 
       item, 
       date_dim 
WHERE  ss_item_sk = i_item_sk 
       AND d_date IN (SELECT d_date 
                     FROM   date_dim 
                     WHERE  d_week_seq = (SELECT d_week_seq 
                                          FROM   date_dim 
                                          WHERE  d_date = '2002-02-25' 
                                         )) 
        AND ss_sold_date_sk = d_date_sk 
        
LIMIT 100; 