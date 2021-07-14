-- query35
SELECT ca_state, 
               cd_gender, 
               cd_marital_status, 
               cd_dep_count, 
               Count(*) cnt1, 
               Stddev_samp(cd_dep_count), 
               Avg(cd_dep_count), 
               Max(cd_dep_count), 
               cd_dep_employed_count, 
               Count(*) cnt2, 
               Stddev_samp(cd_dep_employed_count), 
               Avg(cd_dep_employed_count), 
               Max(cd_dep_employed_count), 
               cd_dep_college_count, 
               Count(*) cnt3, 
               Stddev_samp(cd_dep_college_count), 
               Avg(cd_dep_college_count), 
               Max(cd_dep_college_count) 
FROM   {{source('src__tpc_ds', 'customer')}} c,
       {{source('src__tpc_ds', 'customer_address')}} ca,
       {{source('src__tpc_ds', 'customer_demographics')}}
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   {{source('src__tpc_ds', 'store_sales')}},
                          {{source('src__tpc_ds', 'date_dim')}}
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 2001 
                          AND d_qoy < 4) 
       AND ( EXISTS (SELECT * 
                     FROM   {{source('src__tpc_ds', 'web_sales')}},
                            {{source('src__tpc_ds', 'date_dim')}}
                     WHERE  c.c_customer_sk = ws_bill_customer_sk 
                            AND ws_sold_date_sk = d_date_sk 
                            AND d_year = 2001 
                            AND d_qoy < 4) 
              OR EXISTS (SELECT * 
                         FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                                {{source('src__tpc_ds', 'date_dim')}}
                         WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                AND cs_sold_date_sk = d_date_sk 
                                AND d_year = 2001 
                                AND d_qoy < 4) ) 
GROUP  BY ca_state, 
          cd_gender, 
          cd_marital_status, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count 
ORDER  BY ca_state, 
          cd_gender, 
          cd_marital_status, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count
LIMIT 100; 
