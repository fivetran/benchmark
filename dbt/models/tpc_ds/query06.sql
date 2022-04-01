-- query6
SELECT {{ 'TOP 100' if target.name == 'synapse' }} a.ca_state state,
               Count(*)   cnt
FROM   {{source('src__tpc_ds', 'customer_address')}} a,
       {{source('src__tpc_ds', 'customer')}} c,
       {{source('src__tpc_ds', 'store_sales')}} s,
       {{source('src__tpc_ds', 'date_dim')}} d,
       {{source('src__tpc_ds', 'item')}} i
WHERE  a.ca_address_sk = c.c_current_addr_sk
       AND c.c_customer_sk = s.ss_customer_sk
       AND s.ss_sold_date_sk = d.d_date_sk
       AND s.ss_item_sk = i.i_item_sk
       AND d.d_month_seq = (SELECT DISTINCT ( d_month_seq )
                            FROM   {{source('src__tpc_ds', 'date_dim')}}
                            WHERE  d_year = 1998
                                   AND d_moy = 7)
       AND i.i_current_price > 1.2 * (SELECT Avg(j.i_current_price)
                                      FROM   {{source('src__tpc_ds', 'item')}} j
                                      WHERE  j.i_category = i.i_category)
GROUP  BY a.ca_state
HAVING Count(*) >= 10
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY cnt
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
