-- query31
SELECT ca_county, 
      d_qoy, 
      d_year, 
      Sum(ss_ext_sales_price) AS store_sales
FROM   store_sales, 
       date_dim, 
       customer_address,
       web_sales
WHERE  ss_sold_date_sk = d_date_sk 
        AND ss_addr_sk = ca_address_sk 
GROUP  BY ca_county, 
       d_qoy, 
       d_year 