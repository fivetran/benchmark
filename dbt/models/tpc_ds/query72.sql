-- query72
WITH top_items AS (
  SELECT cs_item_sk,
                cs_warehouse_sk,
                d1.d_week_seq,
                Sum(CASE
                      WHEN p_promo_sk IS NULL THEN 1
                      ELSE 0
                    END) no_promo,
                Sum(CASE
                      WHEN p_promo_sk IS NOT NULL THEN 1
                      ELSE 0
                    END) promo,
                Count(*) total_cnt
  FROM   {{source('src__tpc_ds', 'catalog_sales')}}
        JOIN {{source('src__tpc_ds', 'inventory')}}
          ON ( cs_item_sk = inv_item_sk and cs_sold_date_sk = inv_date_sk and cs_warehouse_sk = inv_warehouse_sk )
        JOIN {{source('src__tpc_ds', 'customer_demographics')}}
          ON ( cs_bill_cdemo_sk = cd_demo_sk )
        JOIN {{source('src__tpc_ds', 'household_demographics')}}
          ON ( cs_bill_hdemo_sk = hd_demo_sk )
        JOIN {{source('src__tpc_ds', 'date_dim')}} d1
          ON ( cs_sold_date_sk = d1.d_date_sk )
        LEFT OUTER JOIN {{source('src__tpc_ds', 'promotion')}}
                      ON ( cs_promo_sk = p_promo_sk )
        LEFT OUTER JOIN {{source('src__tpc_ds', 'catalog_returns')}}
                      ON ( cr_item_sk = cs_item_sk
                          AND cr_order_number = cs_order_number )
  WHERE inv_quantity_on_hand < cs_quantity
        AND hd_buy_potential = '501-1000'
        AND d1.d_year = 2002
        AND cd_marital_status = 'M'
  GROUP  BY cs_item_sk, cs_warehouse_sk, d_week_seq
)
SELECT {{ 'TOP 100' if target.name == 'synapse' }} i_item_desc,
       w_warehouse_name,
       d_week_seq,
       no_promo,
       promo,
       total_cnt
FROM top_items
JOIN {{source('src__tpc_ds', 'warehouse')}}
  ON ( w_warehouse_sk = cs_warehouse_sk )
JOIN {{source('src__tpc_ds', 'item')}}
  ON ( i_item_sk = cs_item_sk )
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY total_cnt DESC,
          1, 2, 3
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
