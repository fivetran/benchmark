-- query10
SELECT cd_gender, 
               cd_marital_status, 
               cd_education_status, 
               1 cnt1, 
               cd_purchase_estimate, 
               1 cnt2, 
               cd_credit_rating, 
               1 cnt3, 
               cd_dep_count, 
               1 cnt4, 
               cd_dep_employed_count, 
               1 cnt5, 
               cd_dep_college_count, 
               1 cnt6 
FROM   customer c, 
       customer_address ca, 
       tpcds2.customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_county IN ( 'Lycoming County', 'Sheridan County', 
                          'Kandiyohi County', 
                          'Pike County', 
                                       'Greene County' )       
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count 
LIMIT 100; 
