--query39
SELECT w_warehouse_name, 
                w_warehouse_sk, 
                i_item_sk, 
                d_moy, 
                stdev, 
                mean, 
                CASE mean 
                  WHEN 0 THEN NULL 
                  ELSE stdev / mean 
                END cov 
FROM  (SELECT w_warehouse_name, 
     w_warehouse_sk, 
     i_item_sk, 
     d_moy, 
     Stddev_samp(inv_quantity_on_hand) stdev, 
     Avg(inv_quantity_on_hand)         mean 
FROM    tpcds2.inventory, 
      tpcds2.item, 
      tpcds2.warehouse, 
      tpcds2.date_dim 
WHERE  inv_item_sk = i_item_sk 
     AND inv_warehouse_sk = w_warehouse_sk 
     AND inv_date_sk = d_date_sk 
     AND d_year = 2002 
GROUP  BY w_warehouse_name, 
        w_warehouse_sk, 
        i_item_sk, 
        d_moy) foo 
WHERE  CASE mean 
WHEN 0 THEN 0 
ELSE stdev / mean 
END > 1 