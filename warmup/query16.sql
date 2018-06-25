-- query16
SELECT
         Count(DISTINCT cs_order_number) AS order_count ,
         Sum(cs_ext_ship_cost)           AS total_shipping_cost ,
         Sum(cs_net_profit)              AS total_net_profit
         
FROM     
         catalog_sales cs1 ,
         date_dim ,
         customer_address ,
         call_center
WHERE    Cast(d_date AS DATE) BETWEEN Cast('2002-3-01' AS DATE) AND (
                  Cast('2002-5-01' AS DATE))
AND      cs1.cs_ship_date_sk = d_date_sk
AND      cs1.cs_ship_addr_sk = ca_address_sk
AND      ca_state = 'IA'
AND      cs1.cs_call_center_sk = cc_call_center_sk
AND      cc_county IN ('Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County')

