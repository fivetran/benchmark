SELECT * 
               
FROM   store_sales, 
       date_dim, 
       store, 
       household_demographics 
WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
       AND store_sales.ss_store_sk = store.s_store_sk 
       AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
       AND ( household_demographics.hd_dep_count = 8 
              OR household_demographics.hd_vehicle_count > 4 ) 
       AND date_dim.d_dow = 1 
       AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 ) 
       AND store.s_number_employees BETWEEN 200 AND 295 
LIMIT 100; 