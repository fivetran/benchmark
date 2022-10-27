-- query5
WITH ssr AS
(
         SELECT   s_store_id,
                  Sum(sales_price) AS sales,
                  Sum(profit)      AS profit,
                  Sum(return_amt)  AS returns1,
                  Sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT ss_store_sk             AS store_sk,
                                ss_sold_date_sk         AS date_sk,
                                ss_ext_sales_price      AS sales_price,
                                ss_net_profit           AS profit,
                                0 AS return_amt,
                                0 AS net_loss
                         FROM   {{source('src__tpc_ds', 'store_sales')}}
                         UNION ALL
                         SELECT sr_store_sk             AS store_sk,
                                sr_returned_date_sk     AS date_sk,
                                0 AS sales_price,
                                0 AS profit,
                                sr_return_amt           AS return_amt,
                                sr_net_loss             AS net_loss
                         FROM   {{source('src__tpc_ds', 'store_returns')}} ) salesreturns,
                  {{source('src__tpc_ds', 'date_dim')}},
                  {{source('src__tpc_ds', 'store')}}
         WHERE    date_sk = d_date_sk
         AND      Cast(d_date AS DATE) BETWEEN Cast('2002-08-22' AS DATE) AND      (
                           Cast('2002-09-05' AS DATE))
         AND      store_sk = s_store_sk
         GROUP BY s_store_id) , csr AS
(
         SELECT   cp_catalog_page_id,
                  sum(sales_price) AS sales,
                  sum(profit)      AS profit,
                  sum(return_amt)  AS returns1,
                  sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT cs_catalog_page_sk      AS page_sk,
                                cs_sold_date_sk         AS date_sk,
                                cs_ext_sales_price      AS sales_price,
                                cs_net_profit           AS profit,
                                0 AS return_amt,
                                0 AS net_loss
                         FROM   {{source('src__tpc_ds', 'catalog_sales')}}
                         UNION ALL
                         SELECT cr_catalog_page_sk      AS page_sk,
                                cr_returned_date_sk     AS date_sk,
                                0 AS sales_price,
                                0 AS profit,
                                cr_return_amount        AS return_amt,
                                cr_net_loss             AS net_loss
                         FROM   {{source('src__tpc_ds', 'catalog_returns')}} ) salesreturns,
                  {{source('src__tpc_ds', 'date_dim')}},
                  {{source('src__tpc_ds', 'catalog_page')}}
         WHERE    date_sk = d_date_sk
         AND      Cast(d_date AS DATE) BETWEEN cast('2002-08-22' AS date) AND      (
                           Cast('2002-09-05' AS DATE))
         AND      page_sk = cp_catalog_page_sk
         GROUP BY cp_catalog_page_id) , wsr AS
(
         SELECT   web_site_id,
                  sum(sales_price) AS sales,
                  sum(profit)      AS profit,
                  sum(return_amt)  AS returns1,
                  sum(net_loss)    AS profit_loss
         FROM     (
                         SELECT ws_web_site_sk          AS wsr_web_site_sk,
                                ws_sold_date_sk         AS date_sk,
                                ws_ext_sales_price      AS sales_price,
                                ws_net_profit           AS profit,
                                0 AS return_amt,
                                0 AS net_loss
                         FROM   {{source('src__tpc_ds', 'web_sales')}}
                         UNION ALL
                         SELECT          ws_web_site_sk          AS wsr_web_site_sk,
                                         wr_returned_date_sk     AS date_sk,
                                         0 AS sales_price,
                                         0 AS profit,
                                         wr_return_amt           AS return_amt,
                                         wr_net_loss             AS net_loss
                         FROM            {{source('src__tpc_ds', 'web_returns')}}
                         LEFT OUTER JOIN {{source('src__tpc_ds', 'web_sales')}}
                         ON              (
                                                         wr_item_sk = ws_item_sk
                                         AND             wr_order_number = ws_order_number) ) salesreturns,
                  {{source('src__tpc_ds', 'date_dim')}},
                  {{source('src__tpc_ds', 'web_site')}}
         WHERE    date_sk = d_date_sk
         AND      Cast(d_date AS DATE) BETWEEN cast('2002-08-22' AS date) AND      (
                           Cast('2002-09-05' AS DATE))
         AND      wsr_web_site_sk = web_site_sk
         GROUP BY web_site_id)
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         channel ,
         id ,
         sum(sales)   AS sales ,
         sum(returns1) AS returns1 ,
         sum(profit)  AS profit
FROM     (
                SELECT '{{source('src__tpc_ds', 'store')}} channel' AS channel ,
                       Concat('store', s_store_id) AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   ssr
                UNION ALL
                SELECT 'catalog channel' AS channel ,
                       Concat('catalog_page', cp_catalog_page_id) AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   csr
                UNION ALL
                SELECT 'web channel' AS channel ,
                       Concat('web_site', web_site_id) AS id ,
                       sales ,
                       returns1 ,
                       (profit - profit_loss) AS profit
                FROM   wsr ) x
GROUP BY channel, id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY channel ,
         id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

