-- query67
SELECT    *
from store_sales
    ,date_dim
    ,store
    ,item
where  ss_sold_date_sk=d_date_sk
and ss_item_sk=i_item_sk
and ss_store_sk = s_store_sk
and d_month_seq between 1181 and 1181+11
limit 100;