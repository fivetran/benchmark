-- Merge small files 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=500000000;
set hive.merge.smallfiles.avgsize=500000000;

use tpcds_csv;

insert into tpcds_parquet.call_center
select * from call_center;

insert into tpcds_parquet.catalog_page
select * from catalog_page;

insert into tpcds_parquet.catalog_returns
select * from catalog_returns;

insert into tpcds_parquet.catalog_sales
select * from catalog_sales;

insert into tpcds_parquet.customer_address
select * from customer_address;

insert into tpcds_parquet.customer_demographics
select * from customer_demographics;

insert into tpcds_parquet.customer
select * from customer;

insert into tpcds_parquet.date_dim
select * from date_dim;

insert into tpcds_parquet.household_demographics 
select * from household_demographics;

insert into tpcds_parquet.income_band
select * from income_band;

insert into tpcds_parquet.inventory
select * from inventory;

insert into tpcds_parquet.item
select * from item;

insert into tpcds_parquet.promotion
select * from promotion;

insert into tpcds_parquet.reason
select * from reason;

insert into tpcds_parquet.ship_mode
select * from ship_mode;

insert into tpcds_parquet.store_returns
select * from store_returns;

insert into tpcds_parquet.store_sales
select * from store_sales;

insert into tpcds_parquet.store
select * from store;

insert into tpcds_parquet.time_dim
select * from time_dim;

insert into tpcds_parquet.warehouse
select * from warehouse;

insert into tpcds_parquet.web_page
select * from web_page;

insert into tpcds_parquet.web_returns
select * from web_returns;

insert into tpcds_parquet.web_sales
select * from web_sales;

insert into tpcds_parquet.web_site
select * from web_site;

