-- query84
SELECT {{ 'TOP 100' if target.name == 'synapse' }} c_customer_id   AS customer_id,
       Concat(c_last_name, Concat(', ', c_first_name))  AS customername
FROM   {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'customer_demographics')}},
       {{source('src__tpc_ds', 'household_demographics')}},
       {{source('src__tpc_ds', 'income_band')}},
       {{source('src__tpc_ds', 'store_returns')}}
WHERE  ca_city = 'Green Acres'
       AND c_current_addr_sk = ca_address_sk
       AND ib_lower_bound >= 54986
       AND ib_upper_bound <= 54986 + 50000
       AND ib_income_band_sk = hd_income_band_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND sr_cdemo_sk = cd_demo_sk
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY c_customer_id
{% endif %}
{{ 'LIMIT 100' if target.type != 'synapse' }}
