-- query50
SELECT {{ 'TOP 100' if target.name == 'synapse' }} s_store_name,
               s_company_id,
               s_street_number,
               s_street_name,
               s_street_type,
               s_suite_number,
               s_city,
               s_county,
               s_state,
               s_zip,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk <= 30 ) THEN 1
                     ELSE 0
                   END) AS days_30,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 30 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 60 )
                   THEN 1
                     ELSE 0
                   END) AS days_31_60,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 60 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 90 )
                   THEN 1
                     ELSE 0
                   END) AS days_61_90,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 90 )
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 120 )
                   THEN 1
                     ELSE 0
                   END) AS days_91_120,
               Sum(CASE
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 120 ) THEN 1
                     ELSE 0
                   END) AS days_over_120
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'store_returns')}},
       {{source('src__tpc_ds', 'store')}},
       {{source('src__tpc_ds', 'date_dim')}} d1,
       {{source('src__tpc_ds', 'date_dim')}} d2
WHERE  d2.d_year = 2002
       AND d2.d_moy = 9
       AND ss_ticket_number = sr_ticket_number
       AND ss_item_sk = sr_item_sk
       AND ss_sold_date_sk = d1.d_date_sk
       AND sr_returned_date_sk = d2.d_date_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_store_sk = s_store_sk
GROUP  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
