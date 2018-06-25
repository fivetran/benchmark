-- query21
SELECT w_warehouse_name , 
       i_item_id,
        Sum( 
       CASE 
                WHEN ( 
                    Cast(d_date AS DATE) < Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand 
                ELSE 0 
       END) AS inv_before , 
       Sum( 
       CASE 
                WHEN ( 
                  Cast(d_date AS DATE) >= Cast ('2000-05-13' AS DATE)) THEN inv_quantity_on_hand 
                ELSE 0 
       END) AS inv_after 
       
FROM     
              inventory , 
              warehouse , 
              item , 
              date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49 
      AND i_item_sk = inv_item_sk 
      AND inv_warehouse_sk = w_warehouse_sk 
      AND inv_date_sk = d_date_sk 
      AND Cast(d_date AS DATE) BETWEEN (Cast ('2000-04-13' AS DATE)) AND     
      (cast ('2000-06-13' AS date)) 

GROUP BY w_warehouse_name, 
         i_item_id  


LIMIT 100; 

