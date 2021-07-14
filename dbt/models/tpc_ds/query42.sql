-- query42
SELECT dt.d_year, 
               item.i_category_id, 
               item.i_category, 
               Sum(ss_ext_sales_price) 
FROM   {{source('src__tpc_ds', 'date_dim')}} dt,
       {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'item')}}
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
       AND store_sales.ss_item_sk = item.i_item_sk 
       AND item.i_manager_id = 1 
       AND dt.d_moy = 12 
       AND dt.d_year = 2000 
GROUP  BY dt.d_year, 
          item.i_category_id, 
          item.i_category 
ORDER  BY Sum(ss_ext_sales_price) DESC, 
          dt.d_year, 
          item.i_category_id, 
          item.i_category
LIMIT 100; 
