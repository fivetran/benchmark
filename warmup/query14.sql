SELECT DISTINCT 
        iss.i_brand_id, 
        ics.i_class_id, 
        ics.i_category_id, 
        ss_quantity   quantity, 
        ss_list_price list_price     
   FROM store_sales, 
        item iss, 
        date_dim d1,
        catalog_sales, 
        item ics, 
        date_dim d2, 
        web_sales, 
        item iws, 
        date_dim d3,
        item   
        date_dim 
  WHERE ss_item_sk = iss.i_item_sk 
        AND ss_sold_date_sk = d1.d_date_sk 
        AND d1.d_year BETWEEN 1999 AND 1999 + 2
        AND cs_item_sk = ics.i_item_sk 
        AND cs_sold_date_sk = d2.d_date_sk 
        AND d2.d_year BETWEEN 1999 AND 1999 + 2 
        AND ws_item_sk = iws.i_item_sk 
        AND ws_sold_date_sk = d3.d_date_sk 
        AND d3.d_year BETWEEN 1999 AND 1999 + 2
Limit 100; 