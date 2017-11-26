-- Control files size
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=50000000;
set hive.merge.smallfiles.avgsize=50000000;
set mapred.max.split.size=50000000;

use tpcds_csv;

insert into tpcds_parquet_50mb.call_center
select * from call_center;

insert into tpcds_parquet_50mb.catalog_page
select * from catalog_page;

insert into tpcds_parquet_50mb.catalog_returns
select * from catalog_returns;

insert into tpcds_parquet_50mb.catalog_sales
select * from catalog_sales;

insert into tpcds_parquet_50mb.customer_address
select * from customer_address;

insert into tpcds_parquet_50mb.customer_demographics
select * from customer_demographics;

insert into tpcds_parquet_50mb.customer
select * from customer;

insert into tpcds_parquet_50mb.date_dim
select * from date_dim;

insert into tpcds_parquet_50mb.household_demographics 
select * from household_demographics;

insert into tpcds_parquet_50mb.income_band
select * from income_band;

insert into tpcds_parquet_50mb.inventory
select * from inventory;

insert into tpcds_parquet_50mb.item
select * from item;

insert into tpcds_parquet_50mb.promotion
select * from promotion;

insert into tpcds_parquet_50mb.reason
select * from reason;

insert into tpcds_parquet_50mb.ship_mode
select * from ship_mode;

insert into tpcds_parquet_50mb.store_returns
select * from store_returns;

insert into tpcds_parquet_50mb.store_sales
select * from store_sales;

insert into tpcds_parquet_50mb.store
select * from store;

insert into tpcds_parquet_50mb.time_dim
select * from time_dim;

insert into tpcds_parquet_50mb.warehouse
select * from warehouse;

insert into tpcds_parquet_50mb.web_page
select * from web_page;

insert into tpcds_parquet_50mb.web_returns
select * from web_returns;

insert into tpcds_parquet_50mb.web_sales
select * from web_sales;

insert into tpcds_parquet_50mb.web_site
select * from web_site;

