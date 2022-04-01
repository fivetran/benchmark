-- query69
SELECT {{ 'TOP 100' if target.name == 'synapse' }} cd_gender,
               cd_marital_status,
               cd_education_status,
               Count(*) cnt1,
               cd_purchase_estimate,
               Count(*) cnt2,
               cd_credit_rating,
               Count(*) cnt3
FROM   {{source('src__tpc_ds', 'customer')}} c,
       {{source('src__tpc_ds', 'customer_address')}} ca,
       {{source('src__tpc_ds', 'customer_demographics')}}
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'KS', 'AZ', 'NE' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   {{source('src__tpc_ds', 'store_sales')}},
                          {{source('src__tpc_ds', 'date_dim')}}
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2004
                          AND d_moy BETWEEN 3 AND 3 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   {{source('src__tpc_ds', 'web_sales')}},
                                {{source('src__tpc_ds', 'date_dim')}}
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 2004
                                AND d_moy BETWEEN 3 AND 3 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                                    {{source('src__tpc_ds', 'date_dim')}}
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 2004
                                    AND d_moy BETWEEN 3 AND 3 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
