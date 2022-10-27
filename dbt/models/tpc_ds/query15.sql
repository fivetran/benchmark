-- query15
SELECT {{ 'TOP 100' if target.name == 'synapse' }} ca_zip,
               Sum(cs_sales_price) as sum_cs_sales_price
FROM   {{source('src__tpc_ds', 'catalog_sales')}},
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'date_dim')}}
WHERE  cs_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ( {{ substr('ca_zip', 1, 5) }} IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
              OR ca_state IN ( 'CA', 'WA', 'GA' )
              OR cs_sales_price > 500 )
       AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
GROUP  BY ca_zip
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY ca_zip
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
