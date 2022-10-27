-- query54
WITH my_customers
     AS (SELECT DISTINCT c_customer_sk,
                         c_current_addr_sk
         FROM   (SELECT cs_sold_date_sk     sold_date_sk,
                        cs_bill_customer_sk customer_sk,
                        cs_item_sk          item_sk
                 FROM   {{source('src__tpc_ds', 'catalog_sales')}}
                 UNION ALL
                 SELECT ws_sold_date_sk     sold_date_sk,
                        ws_bill_customer_sk customer_sk,
                        ws_item_sk          item_sk
                 FROM   {{source('src__tpc_ds', 'web_sales')}}) cs_or_ws_sales,
                {{source('src__tpc_ds', 'item')}},
                {{source('src__tpc_ds', 'date_dim')}},
                {{source('src__tpc_ds', 'customer')}}
         WHERE  sold_date_sk = d_date_sk
                AND item_sk = i_item_sk
                AND i_category = 'Sports'
                AND i_class = 'fitness'
                AND c_customer_sk = cs_or_ws_sales.customer_sk
                AND d_moy = 5
                AND d_year = 2000),
     my_revenue
     AS (SELECT c_customer_sk,
                Sum(ss_ext_sales_price) AS revenue
         FROM   my_customers,
                {{source('src__tpc_ds', 'store_sales')}},
                {{source('src__tpc_ds', 'customer_address')}},
                {{source('src__tpc_ds', 'store')}},
                {{source('src__tpc_ds', 'date_dim')}}
         WHERE  c_current_addr_sk = ca_address_sk
                AND ca_county = s_county
                AND ca_state = s_state
                AND ss_sold_date_sk = d_date_sk
                AND c_customer_sk = ss_customer_sk
                AND d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1
                                         FROM   {{source('src__tpc_ds', 'date_dim')}}
                                         WHERE  d_year = 2000
                                                AND d_moy = 5) AND
                                        (SELECT DISTINCT
                                        d_month_seq + 3
                                         FROM   {{source('src__tpc_ds', 'date_dim')}}
                                         WHERE  d_year = 2000
                                                AND d_moy = 5)
         GROUP  BY c_customer_sk),
     segments
     AS (SELECT Floor(revenue / 50) AS segment
         FROM   my_revenue)
SELECT {{ 'TOP 100' if target.name == 'synapse' }} segment,
               Count(*)     AS num_customers,
               segment * 50 AS segment_base
FROM   segments
GROUP  BY segment
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY segment,
          num_customers
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
