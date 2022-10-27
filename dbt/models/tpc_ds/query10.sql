-- query10
SELECT {{ 'TOP 100' if target.name == 'synapse' }} cd_gender,
               cd_marital_status,
               cd_education_status,
               Count(*) cnt1,
               cd_purchase_estimate,
               Count(*) cnt2,
               cd_credit_rating,
               Count(*) cnt3,
               cd_dep_count,
               Count(*) cnt4,
               cd_dep_employed_count,
               Count(*) cnt5,
               cd_dep_college_count,
               Count(*) cnt6
FROM   {{source('src__tpc_ds', 'customer')}} c,
       {{source('src__tpc_ds', 'customer_address')}} ca,
       {{source('src__tpc_ds', 'customer_demographics')}}
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_county IN ( 'Lycoming County', 'Sheridan County',
                          'Kandiyohi County',
                          'Pike County',
                                           'Greene County' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   {{source('src__tpc_ds', 'store_sales')}},
                          {{source('src__tpc_ds', 'date_dim')}}
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2002
                          AND d_moy BETWEEN 4 AND 4 + 3)
       AND ( EXISTS (SELECT *
                     FROM   {{source('src__tpc_ds', 'web_sales')}},
                            {{source('src__tpc_ds', 'date_dim')}}
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 2002
                            AND d_moy BETWEEN 4 AND 4 + 3)
              OR EXISTS (SELECT *
                         FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                                {{source('src__tpc_ds', 'date_dim')}}
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 2002
                                AND d_moy BETWEEN 4 AND 4 + 3) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
