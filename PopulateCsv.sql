-- Generate CSV files in gs
-- Run this script in Hive

use tpcds_text_100;

insert into tpcds_csv.call_center
select * from call_center;

insert into tpcds_csv.catalog_page
select * from catalog_page;

insert into tpcds_csv.catalog_returns
select * from catalog_returns;

insert into tpcds_csv.catalog_sales
select * from catalog_sales;

insert into tpcds_csv.customer_address
select * from customer_address;

insert into tpcds_csv.customer_demographics
select * from customer_demographics;

insert into tpcds_csv.customer
select * from customer;

insert into tpcds_csv.date_dim
select * from date_dim;

insert into tpcds_csv.household_demographics 
select * from household_demographics;

insert into tpcds_csv.income_band
select * from income_band;

insert into tpcds_csv.inventory
select * from inventory;

insert into tpcds_csv.item
select * from item;

insert into tpcds_csv.promotion
select * from promotion;

insert into tpcds_csv.reason
select * from reason;

insert into tpcds_csv.ship_mode
select * from ship_mode;

insert into tpcds_csv.store_returns
select * from store_returns;

insert into tpcds_csv.store_sales
select * from store_sales;

insert into tpcds_csv.store
select * from store;

insert into tpcds_csv.time_dim
select * from time_dim;

insert into tpcds_csv.warehouse
select * from warehouse;

insert into tpcds_csv.web_page
select * from web_page;

insert into tpcds_csv.web_returns
select * from web_returns;

insert into tpcds_csv.web_sales
select * from web_sales;

insert into tpcds_csv.web_site
select * from web_site;

