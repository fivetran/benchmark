-- query64
SELECT  i_product_name         product_name, 
                i_item_sk              item_sk, 
                s_store_name           store_name, 
                s_zip                  store_zip,
                Count(*)               cnt, 
                Sum(ss_wholesale_cost) s1, 
                Sum(ss_list_price)     s2, 
                Sum(ss_coupon_amt)     s3 
FROM   store_sales, 
    store_returns, 

            date_dim d1, 
            date_dim d2, 
            date_dim d3, 
            store, 
            customer, 
            customer_demographics cd1, 
            customer_demographics cd2, 
            promotion, 
            household_demographics hd1, 
            household_demographics hd2, 
            customer_address ad1, 
            customer_address ad2, 
            income_band ib1, 
            income_band ib2, 
            item 
WHERE  ss_store_sk = s_store_sk 
    AND ss_sold_date_sk = d1.d_date_sk 
    AND ss_customer_sk = c_customer_sk 
    AND ss_cdemo_sk = cd1.cd_demo_sk 
    AND ss_hdemo_sk = hd1.hd_demo_sk 
    AND ss_addr_sk = ad1.ca_address_sk 
    AND ss_item_sk = i_item_sk 
    AND ss_item_sk = sr_item_sk 
    AND ss_ticket_number = sr_ticket_number 

    AND c_current_cdemo_sk = cd2.cd_demo_sk 
    AND c_current_hdemo_sk = hd2.hd_demo_sk 
    AND c_current_addr_sk = ad2.ca_address_sk 
    AND c_first_sales_date_sk = d2.d_date_sk 
    AND c_first_shipto_date_sk = d3.d_date_sk 
    AND ss_promo_sk = p_promo_sk 
    AND hd1.hd_income_band_sk = ib1.ib_income_band_sk 
    AND hd2.hd_income_band_sk = ib2.ib_income_band_sk 
    AND cd1.cd_marital_status <> cd2.cd_marital_status 
    AND i_color IN ( 'cyan', 'peach', 'blush', 'frosted', 
                     'powder', 'orange' ) 
    AND i_current_price BETWEEN 58 AND 58 + 10 
    AND i_current_price BETWEEN 58 + 1 AND 58 + 15 
GROUP  BY i_product_name, 
       i_item_sk, 
       s_store_name, 
       s_zip
Limit 100; 