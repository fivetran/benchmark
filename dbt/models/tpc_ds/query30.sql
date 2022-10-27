-- query30
WITH customer_total_return
     AS (SELECT wr_returning_customer_sk AS ctr_customer_sk,
                ca_state                 AS ctr_state,
                Sum(wr_return_amt)       AS ctr_total_return
         FROM   {{source('src__tpc_ds', 'web_returns')}},
                {{source('src__tpc_ds', 'date_dim')}},
                {{source('src__tpc_ds', 'customer_address')}}
         WHERE  wr_returned_date_sk = d_date_sk
                AND d_year = 2000
                AND wr_returning_addr_sk = ca_address_sk
         GROUP  BY wr_returning_customer_sk,
                   ca_state)
SELECT {{ 'TOP 100' if target.name == 'synapse' }} c_customer_id,
               c_salutation,
               c_first_name,
               c_last_name,
               c_preferred_cust_flag,
               c_birth_day,
               c_birth_month,
               c_birth_year,
               c_birth_country,
               c_login,
               c_email_address,
               c_last_review_date,
               ctr_total_return
FROM   customer_total_return ctr1,
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'customer')}}
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_state = ctr2.ctr_state)
       AND ca_address_sk = c_current_addr_sk
       AND ca_state = 'IN'
       AND ctr1.ctr_customer_sk = c_customer_sk
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY c_customer_id,
          c_salutation,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_day,
          c_birth_month,
          c_birth_year,
          c_birth_country,
          c_login,
          c_email_address,
          c_last_review_date,
          ctr_total_return
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
