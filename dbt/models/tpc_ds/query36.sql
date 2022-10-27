-- query36
SELECT {{ 'TOP 100' if target.name == 'synapse' }} Sum(ss_net_profit) / Sum(ss_ext_sales_price)                 AS
               gross_margin,
               i_category,
               i_class,
               Rank()
                 OVER (
                   PARTITION BY i_category, i_class
                   ORDER BY Sum(ss_net_profit)/Sum(ss_ext_sales_price) ASC) AS
               rank_within_parent
FROM   {{source('src__tpc_ds', 'store_sales')}},
       {{source('src__tpc_ds', 'date_dim')}} d1,
       {{source('src__tpc_ds', 'item')}},
       {{source('src__tpc_ds', 'store')}}
WHERE  d1.d_year = 2000
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN',
                        'TN', 'TN', 'TN', 'TN' )
GROUP  BY i_category, i_class
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY i_category,
          rank_within_parent
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
