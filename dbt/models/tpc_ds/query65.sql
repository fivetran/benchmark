-- query65
SELECT {{ 'TOP 100' if target.name == 'synapse' }} s_store_name,
               i_item_desc,
               sc.revenue,
               i_current_price,
               i_wholesale_cost,
               i_brand
FROM   {{source('src__tpc_ds', 'store')}},
       {{source('src__tpc_ds', 'item')}},
       (SELECT ss_store_sk,
               Avg(revenue) AS ave
        FROM   (SELECT ss_store_sk,
                       ss_item_sk,
                       Sum(ss_sales_price) AS revenue
                FROM   {{source('src__tpc_ds', 'store_sales')}},
                       {{source('src__tpc_ds', 'date_dim')}}
                WHERE  ss_sold_date_sk = d_date_sk
                       AND d_month_seq BETWEEN 1199 AND 1199 + 11
                GROUP  BY ss_store_sk,
                          ss_item_sk) sa
        GROUP  BY ss_store_sk) sb,
       (SELECT ss_store_sk,
               ss_item_sk,
               Sum(ss_sales_price) AS revenue
        FROM   {{source('src__tpc_ds', 'store_sales')}},
               {{source('src__tpc_ds', 'date_dim')}}
        WHERE  ss_sold_date_sk = d_date_sk
               AND d_month_seq BETWEEN 1199 AND 1199 + 11
        GROUP  BY ss_store_sk,
                  ss_item_sk) sc
WHERE  sb.ss_store_sk = sc.ss_store_sk
       AND sc.revenue <= 0.1 * sb.ave
       AND s_store_sk = sc.ss_store_sk
       AND i_item_sk = sc.ss_item_sk
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY s_store_name,
          i_item_desc
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
