-- query91
SELECT cc_call_center_id Call_Center,
       cc_name           Call_Center_Name,
       cc_manager        Manager,
       Sum(cr_net_loss)  Returns_Loss
FROM   {{source('src__tpc_ds', 'call_center')}},
       {{source('src__tpc_ds', 'catalog_returns')}},
       {{source('src__tpc_ds', 'date_dim')}},
       {{source('src__tpc_ds', 'customer')}},
       {{source('src__tpc_ds', 'customer_address')}},
       {{source('src__tpc_ds', 'customer_demographics')}},
       {{source('src__tpc_ds', 'household_demographics')}}
WHERE  cr_call_center_sk = cc_call_center_sk
       AND cr_returned_date_sk = d_date_sk
       AND cr_returning_customer_sk = c_customer_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND ca_address_sk = c_current_addr_sk
       AND d_year = 1999
       AND d_moy = 12
       AND ( ( cd_marital_status = 'M'
               AND cd_education_status = 'Unknown' )
              OR ( cd_marital_status = 'W'
                   AND cd_education_status = 'Advanced Degree' ) )
       AND hd_buy_potential LIKE 'Unknown%'
       AND ca_gmt_offset = -7
GROUP  BY cc_call_center_id,
          cc_name,
          cc_manager,
          cd_marital_status,
          cd_education_status
{% if not (target.type == 'synapse' and model.config.materialized == 'table') %}
ORDER  BY Sum(cr_net_loss) DESC
{% endif %}
