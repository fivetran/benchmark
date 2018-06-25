-- query3
SELECT         dt.d_year, 
               item.i_brand_id          brand_id, 
               item.i_brand             brand 
               
FROM             date_dim dt, 
                 store_sales, 
                 item 
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
       AND store_sales.ss_item_sk = item.i_item_sk 
       AND item.i_manufact_id = 427 
       AND dt.d_moy = 11 
LIMIT 100;
