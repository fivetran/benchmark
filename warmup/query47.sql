-- query47
SELECT i_category, 
       i_brand, 
       s_store_name, 
       s_company_name, 
       d_year, 
        d_moy, 
        Sum(ss_sales_price)         sum_sales, 
        Avg(Sum(ss_sales_price)) 
        OVER ( 
        partition BY i_category, i_brand, s_store_name, 
        s_company_name, 
        d_year) 
        avg_monthly_sales 

FROM  item, 
      store_sales, 
      date_dim, 
      store 
WHERE  ss_item_sk = i_item_sk 
      AND ss_sold_date_sk = d_date_sk
      AND d_year = 1999 
      AND ss_store_sk = s_store_sk 
      AND ( d_year = 1999 
             OR ( d_year = 1999 - 1 
                  AND d_moy = 12 ) 
             OR ( d_year = 1999 + 1 
                  AND d_moy = 1 ) ) 
         GROUP  BY i_category, 
                   i_brand, 
                   s_store_name, 
                   s_company_name, 
                   d_year, 
                   d_moy      
ORDER  BY sum_sales - avg_monthly_sales, 
          3
LIMIT 100; 