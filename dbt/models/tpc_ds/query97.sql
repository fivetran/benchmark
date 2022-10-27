
-- query97
WITH ssci
     AS (SELECT ss_customer_sk customer_sk,
                ss_item_sk     item_sk
         FROM   {{source('src__tpc_ds', 'store_sales')}},
                {{source('src__tpc_ds', 'date_dim')}}
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_month_seq BETWEEN 1196 AND 1196 + 11
         GROUP  BY ss_customer_sk,
                   ss_item_sk),
     csci
     AS (SELECT cs_bill_customer_sk customer_sk,
                cs_item_sk          item_sk
         FROM   {{source('src__tpc_ds', 'catalog_sales')}},
                {{source('src__tpc_ds', 'date_dim')}}
         WHERE  cs_sold_date_sk = d_date_sk
                AND d_month_seq BETWEEN 1196 AND 1196 + 11
         GROUP  BY cs_bill_customer_sk,
                   cs_item_sk)
SELECT {{ 'TOP 100' if target.name == 'synapse' }} Sum(CASE
                     WHEN ssci.customer_sk IS NOT NULL
                          AND csci.customer_sk IS NULL THEN 1
                     ELSE 0
                   END) store_only,
               Sum(CASE
                     WHEN ssci.customer_sk IS NULL
                          AND csci.customer_sk IS NOT NULL THEN 1
                     ELSE 0
                   END) catalog_only,
               Sum(CASE
                     WHEN ssci.customer_sk IS NOT NULL
                          AND csci.customer_sk IS NOT NULL THEN 1
                     ELSE 0
                   END) store_and_catalog
FROM   ssci
       FULL OUTER JOIN csci
                    ON ( ssci.customer_sk = csci.customer_sk
                         AND ssci.item_sk = csci.item_sk )
{{ 'LIMIT 100' if target.type != 'synapse' }}
