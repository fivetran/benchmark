-- query14
WITH item_ss AS (
    SELECT DISTINCT
        iss.i_brand_id,
        iss.i_class_id,
        iss.i_category_id
    FROM   {{source('src__tpc_ds', 'store_sales')}},
        {{source('src__tpc_ds', 'item')}} iss,
        {{source('src__tpc_ds', 'date_dim')}} d1
    WHERE  ss_item_sk = iss.i_item_sk
        AND ss_sold_date_sk = d1.d_date_sk
        AND d1.d_year BETWEEN 1999 AND 1999 + 2
), item_cs AS (
    SELECT DISTINCT
        ics.i_brand_id,
        ics.i_class_id,
        ics.i_category_id
    FROM   {{source('src__tpc_ds', 'catalog_sales')}},
        {{source('src__tpc_ds', 'item')}} ics,
        {{source('src__tpc_ds', 'date_dim')}} d2
    WHERE  cs_item_sk = ics.i_item_sk
        AND cs_sold_date_sk = d2.d_date_sk
        AND d2.d_year BETWEEN 1999 AND 1999 + 2
), item_ws AS (
    SELECT DISTINCT
        iws.i_brand_id,
        iws.i_class_id,
        iws.i_category_id
    FROM   {{source('src__tpc_ds', 'web_sales')}},
        {{source('src__tpc_ds', 'item')}} iws,
        {{source('src__tpc_ds', 'date_dim')}} d3
    WHERE  ws_item_sk = iws.i_item_sk
        AND ws_sold_date_sk = d3.d_date_sk
        AND d3.d_year BETWEEN 1999 AND 1999 + 2
), item_intersect AS (
    SELECT
        item_ss.i_brand_id    brand_id,
        item_ss.i_class_id    class_id,
        item_ss.i_category_id category_id
    FROM item_ss
    JOIN item_ws ON item_ss.i_brand_id = item_ws.i_brand_id
        AND item_ss.i_class_id = item_ws.i_class_id
        AND item_ss.i_category_id = item_ws.i_category_id
    JOIN item_cs ON item_ss.i_brand_id = item_cs.i_brand_id
        AND item_ss.i_class_id = item_cs.i_class_id
        AND item_ss.i_category_id = item_cs.i_category_id
), cross_items AS (
         SELECT i_item_sk ss_item_sk
         FROM   {{source('src__tpc_ds', 'item')}},
                item_intersect
         WHERE  i_brand_id = brand_id
                AND i_class_id = class_id
                AND i_category_id = category_id),
     avg_sales
     AS (SELECT Avg(quantity * list_price) average_sales
         FROM   (SELECT ss_quantity   quantity,
                        ss_list_price list_price
                 FROM   {{source('src__tpc_ds', 'store_sales')}},
                        {{source('src__tpc_ds', 'date_dim')}}
                 WHERE  ss_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT cs_quantity   quantity,
                        cs_list_price list_price
                 FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                        {{source('src__tpc_ds', 'date_dim')}}
                 WHERE  cs_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2
                 UNION ALL
                 SELECT ws_quantity   quantity,
                        ws_list_price list_price
                 FROM   {{source('src__tpc_ds', 'web_sales')}},
                        {{source('src__tpc_ds', 'date_dim')}}
                 WHERE  ws_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 1999 + 2) x)
SELECT {{ 'TOP 100' if target.name == 'synapse' }} channel,
               i_brand_id,
               i_class_id,
               i_category_id,
               Sum(sales) as sum_sales,
               Sum(number_sales) as sum_number_sales
FROM  (SELECT 'store'                          channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(ss_quantity * ss_list_price) sales,
              Count(*)                         number_sales
       FROM   {{source('src__tpc_ds', 'store_sales')}},
              {{source('src__tpc_ds', 'item')}},
              {{source('src__tpc_ds', 'date_dim')}}
       WHERE  ss_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ss_item_sk = i_item_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(ss_quantity * ss_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'catalog'                        channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(cs_quantity * cs_list_price) sales,
              Count(*)                         number_sales
       FROM   {{source('src__tpc_ds', 'catalog_sales')}},
              {{source('src__tpc_ds', 'item')}},
              {{source('src__tpc_ds', 'date_dim')}}
       WHERE  cs_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND cs_item_sk = i_item_sk
              AND cs_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(cs_quantity * cs_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'web'                            channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              Sum(ws_quantity * ws_list_price) sales,
              Count(*)                         number_sales
       FROM   {{source('src__tpc_ds', 'web_sales')}},
              {{source('src__tpc_ds', 'item')}},
              {{source('src__tpc_ds', 'date_dim')}}
       WHERE  ws_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ws_item_sk = i_item_sk
              AND ws_sold_date_sk = d_date_sk
              AND d_year = 1999 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING Sum(ws_quantity * ws_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)) y
GROUP  BY channel, i_brand_id, i_class_id, i_category_id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY channel,
          i_brand_id,
          i_class_id,
          i_category_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
