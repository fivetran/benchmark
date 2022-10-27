-- query80
WITH ssr AS
(
                SELECT          s_store_id                                    AS store_id,
                                Sum(ss_ext_sales_price)                       AS sales,
                                Sum(COALESCE(sr_return_amt, 0))               AS returns1,
                                Sum(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
                FROM            {{source('src__tpc_ds', 'store_sales')}}
                LEFT OUTER JOIN {{source('src__tpc_ds', 'store_returns')}}
                ON              (
                                                ss_item_sk = sr_item_sk
                                AND             ss_ticket_number = sr_ticket_number),
                                {{source('src__tpc_ds', 'date_dim')}},
                                {{source('src__tpc_ds', 'store')}},
                                {{source('src__tpc_ds', 'item')}},
                                {{source('src__tpc_ds', 'promotion')}}
                WHERE           ss_sold_date_sk = d_date_sk
                AND             Cast(d_date AS DATE) BETWEEN Cast('2000-08-26' AS DATE) AND             (
                                                Cast('2001-09-25' AS DATE))
                AND             ss_store_sk = s_store_sk
                AND             ss_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             ss_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        s_store_id) , csr AS
(
                SELECT          cp_catalog_page_id                            AS catalog_page_id,
                                sum(cs_ext_sales_price)                       AS sales,
                                sum(COALESCE(cr_return_amount, 0))            AS returns1,
                                sum(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
                FROM            {{source('src__tpc_ds', 'catalog_sales')}}
                LEFT OUTER JOIN {{source('src__tpc_ds', 'catalog_returns')}}
                ON              (
                                                cs_item_sk = cr_item_sk
                                AND             cs_order_number = cr_order_number),
                                {{source('src__tpc_ds', 'date_dim')}},
                                {{source('src__tpc_ds', 'catalog_page')}},
                                {{source('src__tpc_ds', 'item')}},
                                {{source('src__tpc_ds', 'promotion')}}
                WHERE           cs_sold_date_sk = d_date_sk
                AND             Cast(d_date AS DATE) BETWEEN cast('2000-08-26' AS date) AND             (
                                                Cast('2001-09-25' AS DATE))
                AND             cs_catalog_page_sk = cp_catalog_page_sk
                AND             cs_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             cs_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        cp_catalog_page_id) , wsr AS
(
                SELECT          web_site_id,
                                sum(ws_ext_sales_price)                       AS sales,
                                sum(COALESCE(wr_return_amt, 0))               AS returns1,
                                sum(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
                FROM            {{source('src__tpc_ds', 'web_sales')}}
                LEFT OUTER JOIN {{source('src__tpc_ds', 'web_returns')}}
                ON              (
                                                ws_item_sk = wr_item_sk
                                AND             ws_order_number = wr_order_number),
                                {{source('src__tpc_ds', 'date_dim')}},
                                {{source('src__tpc_ds', 'web_site')}},
                                {{source('src__tpc_ds', 'item')}},
                                {{source('src__tpc_ds', 'promotion')}}
                WHERE           ws_sold_date_sk = d_date_sk
                AND             Cast(d_date AS DATE) BETWEEN cast('2000-08-26' AS date) AND             (
                                                Cast('2001-09-25' AS DATE))
                AND             ws_web_site_sk = web_site_sk
                AND             ws_item_sk = i_item_sk
                AND             i_current_price > 50
                AND             ws_promo_sk = p_promo_sk
                AND             p_channel_tv = 'N'
                GROUP BY        web_site_id)
SELECT {{ 'TOP 100' if target.name == 'synapse' }}
         channel ,
         id ,
         sum(sales)   AS sales ,
         sum(returns1) AS returns1 ,
         sum(profit)  AS profit
FROM     (
                SELECT '{{source('src__tpc_ds', 'store')}} channel' AS channel ,
                       Concat('store', store_id) AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   ssr
                UNION ALL
                SELECT 'catalog channel' AS channel ,
                       Concat('catalog_page', catalog_page_id) AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   csr
                UNION ALL
                SELECT 'web channel' AS channel ,
                       Concat('web_site', web_site_id) AS id ,
                       sales ,
                       returns1 ,
                       profit
                FROM   wsr ) x
GROUP BY channel, id
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER BY channel ,
         id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}

