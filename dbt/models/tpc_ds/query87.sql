-- query87
with store_customers as (
  select distinct c_last_name as sc_last, c_first_name as sc_first, d_date as sc_date
  from {{source('src__tpc_ds', 'store_sales')}}, {{source('src__tpc_ds', 'date_dim')}}, {{source('src__tpc_ds', 'customer')}}
  where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_customer_sk = customer.c_customer_sk
    and d_month_seq between 1188 and 1188+11
),
catalog_customers as (
  select distinct c_last_name as cc_last, c_first_name as cc_first, d_date as cc_date
  from {{source('src__tpc_ds', 'catalog_sales')}}, {{source('src__tpc_ds', 'date_dim')}}, {{source('src__tpc_ds', 'customer')}}
  where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
    and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
    and d_month_seq between 1188 and 1188+11
),
web_customers as (
  select distinct c_last_name as wc_last, c_first_name as wc_first, d_date as wc_date
  from {{source('src__tpc_ds', 'web_sales')}}, {{source('src__tpc_ds', 'date_dim')}}, {{source('src__tpc_ds', 'customer')}}
  where web_sales.ws_sold_date_sk = date_dim.d_date_sk
    and web_sales.ws_bill_customer_sk = customer.c_customer_sk
    and d_month_seq between 1188 and 1188+11
)
select count(1) as cnt
from store_customers
where not exists (
  select 1 from catalog_customers
  where sc_last = cc_last and sc_first = cc_first and sc_date = cc_date
)
and not exists (
  select 1 from web_customers
  where sc_last = wc_last and sc_first = wc_first and sc_date = wc_date
)

