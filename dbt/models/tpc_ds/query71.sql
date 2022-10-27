-- query71
SELECT i_brand_id     brand_id,
       i_brand        brand,
       t_hour,
       t_minute,
       Sum(ext_price) ext_price
FROM   {{source('src__tpc_ds', 'item')}},
       (SELECT ws_ext_sales_price AS ext_price,
               ws_sold_date_sk    AS sold_date_sk,
               ws_item_sk         AS sold_item_sk,
               ws_sold_time_sk    AS time_sk
        FROM   {{source('src__tpc_ds', 'web_sales')}},
               {{source('src__tpc_ds', 'date_dim')}}
        WHERE  d_date_sk = ws_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001
        UNION ALL
        SELECT cs_ext_sales_price AS ext_price,
               cs_sold_date_sk    AS sold_date_sk,
               cs_item_sk         AS sold_item_sk,
               cs_sold_time_sk    AS time_sk
        FROM   {{source('src__tpc_ds', 'catalog_sales')}},
               {{source('src__tpc_ds', 'date_dim')}}
        WHERE  d_date_sk = cs_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001
        UNION ALL
        SELECT ss_ext_sales_price AS ext_price,
               ss_sold_date_sk    AS sold_date_sk,
               ss_item_sk         AS sold_item_sk,
               ss_sold_time_sk    AS time_sk
        FROM   {{source('src__tpc_ds', 'store_sales')}},
               {{source('src__tpc_ds', 'date_dim')}}
        WHERE  d_date_sk = ss_sold_date_sk
               AND d_moy = 11
               AND d_year = 2001) AS tmp,
       {{source('src__tpc_ds', 'time_dim')}}
WHERE  sold_item_sk = i_item_sk
       AND i_manager_id = 1
       AND time_sk = t_time_sk
       AND ( t_meal_time = 'breakfast'
              OR t_meal_time = 'dinner' )
GROUP  BY i_brand,
          i_brand_id,
          t_hour,
          t_minute
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ext_price DESC,
          i_brand_id
{% endif %}

