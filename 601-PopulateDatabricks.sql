-- Configure a Databricks cluster with:
--   4 i3.8xlarge workers
--   1 i3.xlarge driver
--   Spark config: 'spark.sql.crossJoin.enable true'
drop database if exists benchmark cascade;
create database benchmark;
use benchmark;


create table call_center_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string, _c28 string, _c29 string, _c30 string, _c31 string) using delta;
create table catalog_page_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string) using delta;
create table catalog_returns_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string) using delta;
create table catalog_sales_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string, _c28 string, _c29 string, _c30 string, _c31 string, _c32 string, _c33 string, _c34 string) using delta;
create table customer_address_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string) using delta;
create table customer_demographics_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string) using delta;
create table customer_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string) using delta;
create table date_dim_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string, _c28 string) using delta;
create table household_demographics_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string) using delta;
create table income_band_staging (_c0 string, _c1 string, _c2 string, _c3 string) using delta;
create table inventory_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string) using delta;
create table item_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string) using delta;
create table promotion_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string) using delta;
create table reason_staging (_c0 string, _c1 string, _c2 string, _c3 string) using delta;
create table ship_mode_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string) using delta;
create table store_returns_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string) using delta;
create table store_sales_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string) using delta;
create table store_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string, _c28 string, _c29 string) using delta;
create table time_dim_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string) using delta;
create table warehouse_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string) using delta;
create table web_page_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string) using delta;
create table web_returns_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string) using delta;
create table web_sales_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string, _c27 string, _c28 string, _c29 string, _c30 string, _c31 string, _c32 string, _c33 string, _c34 string) using delta;
create table web_site_staging (_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string, _c10 string, _c11 string, _c12 string, _c13 string, _c14 string, _c15 string, _c16 string, _c17 string, _c18 string, _c19 string, _c20 string, _c21 string, _c22 string, _c23 string, _c24 string, _c25 string, _c26 string) using delta;


create table call_center (
	cc_call_center_sk long,
	cc_call_center_id string,
	cc_rec_start_date string,
	cc_rec_end_date string,
	cc_closed_date_sk long,
	cc_open_date_sk long,
	cc_name string,
	cc_class string,
	cc_employees int,
	cc_sq_ft int,
	cc_hours string,
	cc_manager string,
	cc_mkt_id int,
	cc_mkt_class string,
	cc_mkt_desc string,
	cc_market_manager string,
	cc_division int,
	cc_division_name string,
	cc_company int,
	cc_company_name string,
	cc_street_number string,
	cc_street_name string,
	cc_street_type string,
	cc_suite_number string,
	cc_city string,
	cc_county string,
	cc_state string,
	cc_zip string,
	cc_country string,
	cc_gmt_offset double,
	cc_tax_percentage double) using delta;
create table catalog_page (
	cp_catalog_page_sk long,
	cp_catalog_page_id string,
	cp_start_date_sk long,
	cp_end_date_sk long,
	cp_department string,
	cp_catalog_number int,
	cp_catalog_page_number int,
	cp_description string,
	cp_type string) using delta;
create table catalog_returns (
	cr_returned_date_sk long,
	cr_returned_time_sk long,
	cr_item_sk long,
	cr_refunded_customer_sk long,
	cr_refunded_cdemo_sk long,
	cr_refunded_hdemo_sk long,
	cr_refunded_addr_sk long,
	cr_returning_customer_sk long,
	cr_returning_cdemo_sk long,
	cr_returning_hdemo_sk long,
	cr_returning_addr_sk long,
	cr_call_center_sk long,
	cr_catalog_page_sk long,
	cr_ship_mode_sk long,
	cr_warehouse_sk long,
	cr_reason_sk long,
	cr_order_number long,
	cr_return_quantity int,
	cr_return_amount double,
	cr_return_tax double,
	cr_return_amt_inc_tax double,
	cr_fee double,
	cr_return_ship_cost double,
	cr_refunded_cash double,
	cr_reversed_charge double,
	cr_store_credit double,
	cr_net_loss double) using delta;
create table catalog_sales (
	cs_sold_date_sk long,
	cs_sold_time_sk long,
	cs_ship_date_sk long,
	cs_bill_customer_sk long,
	cs_bill_cdemo_sk long,
	cs_bill_hdemo_sk long,
	cs_bill_addr_sk long,
	cs_ship_customer_sk long,
	cs_ship_cdemo_sk long,
	cs_ship_hdemo_sk long,
	cs_ship_addr_sk long,
	cs_call_center_sk long,
	cs_catalog_page_sk long,
	cs_ship_mode_sk long,
	cs_warehouse_sk long,
	cs_item_sk long,
	cs_promo_sk long,
	cs_order_number long,
	cs_quantity int,
	cs_wholesale_cost double,
	cs_list_price double,
	cs_sales_price double,
	cs_ext_discount_amt double,
	cs_ext_sales_price double,
	cs_ext_wholesale_cost double,
	cs_ext_list_price double,
	cs_ext_tax double,
	cs_coupon_amt double,
	cs_ext_ship_cost double,
	cs_net_paid double,
	cs_net_paid_inc_tax double,
	cs_net_paid_inc_ship double,
	cs_net_paid_inc_ship_tax double,
	cs_net_profit double) using delta;
create table customer_address (
	ca_address_sk long,
	ca_address_id string,
	ca_street_number string,
	ca_street_name string,
	ca_street_type string,
	ca_suite_number string,
	ca_city string,
	ca_county string,
	ca_state string,
	ca_zip string,
	ca_country string,
	ca_gmt_offset double,
	ca_location_type string) using delta;
create table customer_demographics (
	cd_demo_sk long,
	cd_gender string,
	cd_marital_status string,
	cd_education_status string,
	cd_purchase_estimate int,
	cd_credit_rating string,
	cd_dep_count int,
	cd_dep_employed_count int,
	cd_dep_college_count int) using delta;
create table customer (
	c_customer_sk long,
	c_customer_id string,
	c_current_cdemo_sk long,
	c_current_hdemo_sk long,
	c_current_addr_sk long,
	c_first_shipto_date_sk long,
	c_first_sales_date_sk long,
	c_salutation string,
	c_first_name string,
	c_last_name string,
	c_preferred_cust_flag string,
	c_birth_day int,
	c_birth_month int,
	c_birth_year int,
	c_birth_country string,
	c_login string,
	c_email_address string,
	c_last_review_date string) using delta;
create table date_dim (
	d_date_sk long,
	d_date_id string,
	d_date string,
	d_month_seq int,
	d_week_seq int,
	d_quarter_seq int,
	d_year int,
	d_dow int,
	d_moy int,
	d_dom int,
	d_qoy int,
	d_fy_year int,
	d_fy_quarter_seq int,
	d_fy_week_seq int,
	d_day_name string,
	d_quarter_name string,
	d_holiday string,
	d_weekend string,
	d_following_holiday string,
	d_first_dom int,
	d_last_dom int,
	d_same_day_ly int,
	d_same_day_lq int,
	d_current_day string,
	d_current_week string,
	d_current_month string,
	d_current_quarter string,
	d_current_year string) using delta;
create table household_demographics (
	hd_demo_sk long,
	hd_income_band_sk long,
	hd_buy_potential string,
	hd_dep_count int,
	hd_vehicle_count int) using delta;
create table income_band (
	ib_income_band_sk long,
	ib_lower_bound int,
	ib_upper_bound int) using delta;
create table inventory (
	inv_date_sk long,
	inv_item_sk long,
	inv_warehouse_sk long,
	inv_quantity_on_hand int) using delta;
create table item (
	i_item_sk long,
	i_item_id string,
	i_rec_start_date string,
	i_rec_end_date string,
	i_item_desc string,
	i_current_price double,
	i_wholesale_cost double,
	i_brand_id int,
	i_brand string,
	i_class_id int,
	i_class string,
	i_category_id int,
	i_category string,
	i_manufact_id int,
	i_manufact string,
	i_size string,
	i_formulation string,
	i_color string,
	i_units string,
	i_container string,
	i_manager_id int,
	i_product_name string) using delta;
create table promotion (
	p_promo_sk long,
	p_promo_id string,
	p_start_date_sk long,
	p_end_date_sk long,
	p_item_sk long,
	p_cost double,
	p_response_target int,
	p_promo_name string,
	p_channel_dmail string,
	p_channel_email string,
	p_channel_catalog string,
	p_channel_tv string,
	p_channel_radio string,
	p_channel_press string,
	p_channel_event string,
	p_channel_demo string,
	p_channel_details string,
	p_purpose string,
	p_discount_active string) using delta;
create table reason (
	r_reason_sk long,
	r_reason_id string,
	r_reason_desc string) using delta;
create table ship_mode (
	sm_ship_mode_sk long,
	sm_ship_mode_id string,
	sm_type string,
	sm_code string,
	sm_carrier string,
	sm_contract string) using delta;
create table store_returns (
	sr_returned_date_sk long,
	sr_return_time_sk long,
	sr_item_sk long,
	sr_customer_sk long,
	sr_cdemo_sk long,
	sr_hdemo_sk long,
	sr_addr_sk long,
	sr_store_sk long,
	sr_reason_sk long,
	sr_ticket_number long,
	sr_return_quantity int,
	sr_return_amt double,
	sr_return_tax double,
	sr_return_amt_inc_tax double,
	sr_fee double,
	sr_return_ship_cost double,
	sr_refunded_cash double,
	sr_reversed_charge double,
	sr_store_credit double,
	sr_net_loss double) using delta;
create table store_sales (
	ss_sold_date_sk long,
	ss_sold_time_sk long,
	ss_item_sk long,
	ss_customer_sk long,
	ss_cdemo_sk long,
	ss_hdemo_sk long,
	ss_addr_sk long,
	ss_store_sk long,
	ss_promo_sk long,
	ss_ticket_number long,
	ss_quantity int,
	ss_wholesale_cost double,
	ss_list_price double,
	ss_sales_price double,
	ss_ext_discount_amt double,
	ss_ext_sales_price double,
	ss_ext_wholesale_cost double,
	ss_ext_list_price double,
	ss_ext_tax double,
	ss_coupon_amt double,
	ss_net_paid double,
	ss_net_paid_inc_tax double,
	ss_net_profit double) using delta;
create table store (
	s_store_sk long,
	s_store_id string,
	s_rec_start_date string,
	s_rec_end_date string,
	s_closed_date_sk long,
	s_store_name string,
	s_number_employees int,
	s_floor_space int,
	s_hours string,
	s_manager string,
	s_market_id int,
	s_geography_class string,
	s_market_desc string,
	s_market_manager string,
	s_division_id int,
	s_division_name string,
	s_company_id int,
	s_company_name string,
	s_street_number string,
	s_street_name string,
	s_street_type string,
	s_suite_number string,
	s_city string,
	s_county string,
	s_state string,
	s_zip string,
	s_country string,
	s_gmt_offset double,
	s_tax_precentage double) using delta;
create table time_dim (
	t_time_sk long,
	t_time_id string,
	t_time int,
	t_hour int,
	t_minute int,
	t_second int,
	t_am_pm string,
	t_shift string,
	t_sub_shift string,
	t_meal_time string) using delta;
create table warehouse (
	w_warehouse_sk long,
	w_warehouse_id string,
	w_warehouse_name string,
	w_warehouse_sq_ft int,
	w_street_number string,
	w_street_name string,
	w_street_type string,
	w_suite_number string,
	w_city string,
	w_county string,
	w_state string,
	w_zip string,
	w_country string,
	w_gmt_offset double) using delta;
create table web_page (
	wp_web_page_sk long,
	wp_web_page_id string,
	wp_rec_start_date string,
	wp_rec_end_date string,
	wp_creation_date_sk long,
	wp_access_date_sk long,
	wp_autogen_flag string,
	wp_customer_sk long,
	wp_url string,
	wp_type string,
	wp_char_count int,
	wp_link_count int,
	wp_image_count int,
	wp_max_ad_count int) using delta;
create table web_returns (
	wr_returned_date_sk long,
	wr_returned_time_sk long,
	wr_item_sk long,
	wr_refunded_customer_sk long,
	wr_refunded_cdemo_sk long,
	wr_refunded_hdemo_sk long,
	wr_refunded_addr_sk long,
	wr_returning_customer_sk long,
	wr_returning_cdemo_sk long,
	wr_returning_hdemo_sk long,
	wr_returning_addr_sk long,
	wr_web_page_sk long,
	wr_reason_sk long,
	wr_order_number long,
	wr_return_quantity int,
	wr_return_amt double,
	wr_return_tax double,
	wr_return_amt_inc_tax double,
	wr_fee double,
	wr_return_ship_cost double,
	wr_refunded_cash double,
	wr_reversed_charge double,
	wr_account_credit double,
	wr_net_loss double) using delta;
create table web_sales (
	ws_sold_date_sk long,
	ws_sold_time_sk long,
	ws_ship_date_sk long,
	ws_item_sk long,
	ws_bill_customer_sk long,
	ws_bill_cdemo_sk long,
	ws_bill_hdemo_sk long,
	ws_bill_addr_sk long,
	ws_ship_customer_sk long,
	ws_ship_cdemo_sk long,
	ws_ship_hdemo_sk long,
	ws_ship_addr_sk long,
	ws_web_page_sk long,
	ws_web_site_sk long,
	ws_ship_mode_sk long,
	ws_warehouse_sk long,
	ws_promo_sk long,
	ws_order_number long,
	ws_quantity int,
	ws_wholesale_cost double,
	ws_list_price double,
	ws_sales_price double,
	ws_ext_discount_amt double,
	ws_ext_sales_price double,
	ws_ext_wholesale_cost double,
	ws_ext_list_price double,
	ws_ext_tax double,
	ws_coupon_amt double,
	ws_ext_ship_cost double,
	ws_net_paid double,
	ws_net_paid_inc_tax double,
	ws_net_paid_inc_ship double,
	ws_net_paid_inc_ship_tax double,
	ws_net_profit double) using delta;
create table web_site (
	web_site_sk long,
	web_site_id string,
	web_rec_start_date string,
	web_rec_end_date string,
	web_name string,
	web_open_date_sk long,
	web_close_date_sk long,
	web_class string,
	web_manager string,
	web_mkt_id int,
	web_mkt_class string,
	web_mkt_desc string,
	web_market_manager string,
	web_company_id int,
	web_company_name string,
	web_street_number string,
	web_street_name string,
	web_street_type string,
	web_suite_number string,
	web_city string,
	web_county string,
	web_state string,
	web_zip string,
	web_country string,
	web_gmt_offset double,
	web_tax_percentage double) using delta;


copy into call_center_staging from 's3://fivetran-tpcds/tpcds_1000_dat/call_center/' fileformat = csv format_options ('sep' = '|');
copy into catalog_page_staging from 's3://fivetran-tpcds/tpcds_1000_dat/catalog_page/' fileformat = csv format_options ('sep' = '|');
copy into catalog_returns_staging from 's3://fivetran-tpcds/tpcds_1000_dat/catalog_returns/' fileformat = csv format_options ('sep' = '|');
copy into catalog_sales_staging from 's3://fivetran-tpcds/tpcds_1000_dat/catalog_sales/' fileformat = csv format_options ('sep' = '|');
copy into customer_address_staging from 's3://fivetran-tpcds/tpcds_1000_dat/customer_address/' fileformat = csv format_options ('sep' = '|');
copy into customer_demographics_staging from 's3://fivetran-tpcds/tpcds_1000_dat/customer_demographics/' fileformat = csv format_options ('sep' = '|');
copy into customer_staging from 's3://fivetran-tpcds/tpcds_1000_dat/customer/' fileformat = csv format_options ('sep' = '|');
copy into date_dim_staging from 's3://fivetran-tpcds/tpcds_1000_dat/date_dim/' fileformat = csv format_options ('sep' = '|');
copy into household_demographics_staging from 's3://fivetran-tpcds/tpcds_1000_dat/household_demographics/' fileformat = csv format_options ('sep' = '|');
copy into income_band_staging from 's3://fivetran-tpcds/tpcds_1000_dat/income_band/' fileformat = csv format_options ('sep' = '|');
copy into inventory_staging from 's3://fivetran-tpcds/tpcds_1000_dat/inventory/' fileformat = csv format_options ('sep' = '|');
copy into item_staging from 's3://fivetran-tpcds/tpcds_1000_dat/item/' fileformat = csv format_options ('sep' = '|');
copy into promotion_staging from 's3://fivetran-tpcds/tpcds_1000_dat/promotion/' fileformat = csv format_options ('sep' = '|');
copy into reason_staging from 's3://fivetran-tpcds/tpcds_1000_dat/reason/' fileformat = csv format_options ('sep' = '|');
copy into ship_mode_staging from 's3://fivetran-tpcds/tpcds_1000_dat/ship_mode/' fileformat = csv format_options ('sep' = '|');
copy into store_returns_staging from 's3://fivetran-tpcds/tpcds_1000_dat/store_returns/' fileformat = csv format_options ('sep' = '|');
copy into store_sales_staging from 's3://fivetran-tpcds/tpcds_1000_dat/store_sales/' fileformat = csv format_options ('sep' = '|');
copy into store_staging from 's3://fivetran-tpcds/tpcds_1000_dat/store/' fileformat = csv format_options ('sep' = '|');
copy into time_dim_staging from 's3://fivetran-tpcds/tpcds_1000_dat/time_dim/' fileformat = csv format_options ('sep' = '|');
copy into warehouse_staging from 's3://fivetran-tpcds/tpcds_1000_dat/warehouse/' fileformat = csv format_options ('sep' = '|');
copy into web_page_staging from 's3://fivetran-tpcds/tpcds_1000_dat/web_page/' fileformat = csv format_options ('sep' = '|');
copy into web_returns_staging from 's3://fivetran-tpcds/tpcds_1000_dat/web_returns/' fileformat = csv format_options ('sep' = '|');
copy into web_sales_staging from 's3://fivetran-tpcds/tpcds_1000_dat/web_sales/' fileformat = csv format_options ('sep' = '|');
copy into web_site_staging from 's3://fivetran-tpcds/tpcds_1000_dat/web_site/' fileformat = csv format_options ('sep' = '|');


insert into call_center
select
	cast(_c0 as long) as cc_call_center_sk,
	cast(_c1 as string) as cc_call_center_id,
	cast(_c2 as string) as cc_rec_start_date,
	cast(_c3 as string) as cc_rec_end_date,
	cast(_c4 as long) as cc_closed_date_sk,
	cast(_c5 as long) as cc_open_date_sk,
	cast(_c6 as string) as cc_name,
	cast(_c7 as string) as cc_class,
	cast(_c8 as int) as cc_employees,
	cast(_c9 as int) as cc_sq_ft,
	cast(_c10 as string) as cc_hours,
	cast(_c11 as string) as cc_manager,
	cast(_c12 as int) as cc_mkt_id,
	cast(_c13 as string) as cc_mkt_class,
	cast(_c14 as string) as cc_mkt_desc,
	cast(_c15 as string) as cc_market_manager,
	cast(_c16 as int) as cc_division,
	cast(_c17 as string) as cc_division_name,
	cast(_c18 as int) as cc_company,
	cast(_c19 as string) as cc_company_name,
	cast(_c20 as string) as cc_street_number,
	cast(_c21 as string) as cc_street_name,
	cast(_c22 as string) as cc_street_type,
	cast(_c23 as string) as cc_suite_number,
	cast(_c24 as string) as cc_city,
	cast(_c25 as string) as cc_county,
	cast(_c26 as string) as cc_state,
	cast(_c27 as string) as cc_zip,
	cast(_c28 as string) as cc_country,
	cast(_c29 as double) as cc_gmt_offset,
	cast(_c30 as double) as cc_tax_percentage
from call_center_staging;
insert into catalog_page
select
	cast(_c0 as long) as cp_catalog_page_sk,
	cast(_c1 as string) as cp_catalog_page_id,
	cast(_c2 as long) as cp_start_date_sk,
	cast(_c3 as long) as cp_end_date_sk,
	cast(_c4 as string) as cp_department,
	cast(_c5 as int) as cp_catalog_number,
	cast(_c6 as int) as cp_catalog_page_number,
	cast(_c7 as string) as cp_description,
	cast(_c8 as string) as cp_type
from catalog_page_staging;
insert into catalog_returns
select
	cast(_c0 as long) as cr_returned_date_sk,
	cast(_c1 as long) as cr_returned_time_sk,
	cast(_c2 as long) as cr_item_sk,
	cast(_c3 as long) as cr_refunded_customer_sk,
	cast(_c4 as long) as cr_refunded_cdemo_sk,
	cast(_c5 as long) as cr_refunded_hdemo_sk,
	cast(_c6 as long) as cr_refunded_addr_sk,
	cast(_c7 as long) as cr_returning_customer_sk,
	cast(_c8 as long) as cr_returning_cdemo_sk,
	cast(_c9 as long) as cr_returning_hdemo_sk,
	cast(_c10 as long) as cr_returning_addr_sk,
	cast(_c11 as long) as cr_call_center_sk,
	cast(_c12 as long) as cr_catalog_page_sk,
	cast(_c13 as long) as cr_ship_mode_sk,
	cast(_c14 as long) as cr_warehouse_sk,
	cast(_c15 as long) as cr_reason_sk,
	cast(_c16 as long) as cr_order_number,
	cast(_c17 as int) as cr_return_quantity,
	cast(_c18 as double) as cr_return_amount,
	cast(_c19 as double) as cr_return_tax,
	cast(_c20 as double) as cr_return_amt_inc_tax,
	cast(_c21 as double) as cr_fee,
	cast(_c22 as double) as cr_return_ship_cost,
	cast(_c23 as double) as cr_refunded_cash,
	cast(_c24 as double) as cr_reversed_charge,
	cast(_c25 as double) as cr_store_credit,
	cast(_c26 as double) as cr_net_loss
from catalog_returns_staging;
insert into catalog_sales
select
	cast(_c0 as long) as cs_sold_date_sk,
	cast(_c1 as long) as cs_sold_time_sk,
	cast(_c2 as long) as cs_ship_date_sk,
	cast(_c3 as long) as cs_bill_customer_sk,
	cast(_c4 as long) as cs_bill_cdemo_sk,
	cast(_c5 as long) as cs_bill_hdemo_sk,
	cast(_c6 as long) as cs_bill_addr_sk,
	cast(_c7 as long) as cs_ship_customer_sk,
	cast(_c8 as long) as cs_ship_cdemo_sk,
	cast(_c9 as long) as cs_ship_hdemo_sk,
	cast(_c10 as long) as cs_ship_addr_sk,
	cast(_c11 as long) as cs_call_center_sk,
	cast(_c12 as long) as cs_catalog_page_sk,
	cast(_c13 as long) as cs_ship_mode_sk,
	cast(_c14 as long) as cs_warehouse_sk,
	cast(_c15 as long) as cs_item_sk,
	cast(_c16 as long) as cs_promo_sk,
	cast(_c17 as long) as cs_order_number,
	cast(_c18 as int) as cs_quantity,
	cast(_c19 as double) as cs_wholesale_cost,
	cast(_c20 as double) as cs_list_price,
	cast(_c21 as double) as cs_sales_price,
	cast(_c22 as double) as cs_ext_discount_amt,
	cast(_c23 as double) as cs_ext_sales_price,
	cast(_c24 as double) as cs_ext_wholesale_cost,
	cast(_c25 as double) as cs_ext_list_price,
	cast(_c26 as double) as cs_ext_tax,
	cast(_c27 as double) as cs_coupon_amt,
	cast(_c28 as double) as cs_ext_ship_cost,
	cast(_c29 as double) as cs_net_paid,
	cast(_c30 as double) as cs_net_paid_inc_tax,
	cast(_c31 as double) as cs_net_paid_inc_ship,
	cast(_c32 as double) as cs_net_paid_inc_ship_tax,
	cast(_c33 as double) as cs_net_profit
from catalog_sales_staging;
insert into customer_address
select
	cast(_c0 as long) as ca_address_sk,
	cast(_c1 as string) as ca_address_id,
	cast(_c2 as string) as ca_street_number,
	cast(_c3 as string) as ca_street_name,
	cast(_c4 as string) as ca_street_type,
	cast(_c5 as string) as ca_suite_number,
	cast(_c6 as string) as ca_city,
	cast(_c7 as string) as ca_county,
	cast(_c8 as string) as ca_state,
	cast(_c9 as string) as ca_zip,
	cast(_c10 as string) as ca_country,
	cast(_c11 as double) as ca_gmt_offset,
	cast(_c12 as string) as ca_location_type
from customer_address_staging;
insert into customer_demographics
select
	cast(_c0 as long) as cd_demo_sk,
	cast(_c1 as string) as cd_gender,
	cast(_c2 as string) as cd_marital_status,
	cast(_c3 as string) as cd_education_status,
	cast(_c4 as int) as cd_purchase_estimate,
	cast(_c5 as string) as cd_credit_rating,
	cast(_c6 as int) as cd_dep_count,
	cast(_c7 as int) as cd_dep_employed_count,
	cast(_c8 as int) as cd_dep_college_count
from customer_demographics_staging;
insert into customer
select
	cast(_c0 as long) as c_customer_sk,
	cast(_c1 as string) as c_customer_id,
	cast(_c2 as long) as c_current_cdemo_sk,
	cast(_c3 as long) as c_current_hdemo_sk,
	cast(_c4 as long) as c_current_addr_sk,
	cast(_c5 as long) as c_first_shipto_date_sk,
	cast(_c6 as long) as c_first_sales_date_sk,
	cast(_c7 as string) as c_salutation,
	cast(_c8 as string) as c_first_name,
	cast(_c9 as string) as c_last_name,
	cast(_c10 as string) as c_preferred_cust_flag,
	cast(_c11 as int) as c_birth_day,
	cast(_c12 as int) as c_birth_month,
	cast(_c13 as int) as c_birth_year,
	cast(_c14 as string) as c_birth_country,
	cast(_c15 as string) as c_login,
	cast(_c16 as string) as c_email_address,
	cast(_c17 as string) as c_last_review_date
from customer_staging;
insert into date_dim
select
	cast(_c0 as long) as d_date_sk,
	cast(_c1 as string) as d_date_id,
	cast(_c2 as string) as d_date,
	cast(_c3 as int) as d_month_seq,
	cast(_c4 as int) as d_week_seq,
	cast(_c5 as int) as d_quarter_seq,
	cast(_c6 as int) as d_year,
	cast(_c7 as int) as d_dow,
	cast(_c8 as int) as d_moy,
	cast(_c9 as int) as d_dom,
	cast(_c10 as int) as d_qoy,
	cast(_c11 as int) as d_fy_year,
	cast(_c12 as int) as d_fy_quarter_seq,
	cast(_c13 as int) as d_fy_week_seq,
	cast(_c14 as string) as d_day_name,
	cast(_c15 as string) as d_quarter_name,
	cast(_c16 as string) as d_holiday,
	cast(_c17 as string) as d_weekend,
	cast(_c18 as string) as d_following_holiday,
	cast(_c19 as int) as d_first_dom,
	cast(_c20 as int) as d_last_dom,
	cast(_c21 as int) as d_same_day_ly,
	cast(_c22 as int) as d_same_day_lq,
	cast(_c23 as string) as d_current_day,
	cast(_c24 as string) as d_current_week,
	cast(_c25 as string) as d_current_month,
	cast(_c26 as string) as d_current_quarter,
	cast(_c27 as string) as d_current_year
from date_dim_staging;
insert into household_demographics
select
	cast(_c0 as long) as hd_demo_sk,
	cast(_c1 as long) as hd_income_band_sk,
	cast(_c2 as string) as hd_buy_potential,
	cast(_c3 as int) as hd_dep_count,
	cast(_c4 as int) as hd_vehicle_count
from household_demographics_staging;
insert into income_band
select
	cast(_c0 as long) as ib_income_band_sk,
	cast(_c1 as int) as ib_lower_bound,
	cast(_c2 as int) as ib_upper_bound
from income_band_staging;
insert into inventory
select
	cast(_c0 as long) as inv_date_sk,
	cast(_c1 as long) as inv_item_sk,
	cast(_c2 as long) as inv_warehouse_sk,
	cast(_c3 as int) as inv_quantity_on_hand
from inventory_staging;
insert into item
select
	cast(_c0 as long) as i_item_sk,
	cast(_c1 as string) as i_item_id,
	cast(_c2 as string) as i_rec_start_date,
	cast(_c3 as string) as i_rec_end_date,
	cast(_c4 as string) as i_item_desc,
	cast(_c5 as double) as i_current_price,
	cast(_c6 as double) as i_wholesale_cost,
	cast(_c7 as int) as i_brand_id,
	cast(_c8 as string) as i_brand,
	cast(_c9 as int) as i_class_id,
	cast(_c10 as string) as i_class,
	cast(_c11 as int) as i_category_id,
	cast(_c12 as string) as i_category,
	cast(_c13 as int) as i_manufact_id,
	cast(_c14 as string) as i_manufact,
	cast(_c15 as string) as i_size,
	cast(_c16 as string) as i_formulation,
	cast(_c17 as string) as i_color,
	cast(_c18 as string) as i_units,
	cast(_c19 as string) as i_container,
	cast(_c20 as int) as i_manager_id,
	cast(_c21 as string) as i_product_name
from item_staging;
insert into promotion
select
	cast(_c0 as long) as p_promo_sk,
	cast(_c1 as string) as p_promo_id,
	cast(_c2 as long) as p_start_date_sk,
	cast(_c3 as long) as p_end_date_sk,
	cast(_c4 as long) as p_item_sk,
	cast(_c5 as double) as p_cost,
	cast(_c6 as int) as p_response_target,
	cast(_c7 as string) as p_promo_name,
	cast(_c8 as string) as p_channel_dmail,
	cast(_c9 as string) as p_channel_email,
	cast(_c10 as string) as p_channel_catalog,
	cast(_c11 as string) as p_channel_tv,
	cast(_c12 as string) as p_channel_radio,
	cast(_c13 as string) as p_channel_press,
	cast(_c14 as string) as p_channel_event,
	cast(_c15 as string) as p_channel_demo,
	cast(_c16 as string) as p_channel_details,
	cast(_c17 as string) as p_purpose,
	cast(_c18 as string) as p_discount_active
from promotion_staging;
insert into reason
select
	cast(_c0 as long) as r_reason_sk,
	cast(_c1 as string) as r_reason_id,
	cast(_c2 as string) as r_reason_desc
from reason_staging;
insert into ship_mode
select
	cast(_c0 as long) as sm_ship_mode_sk,
	cast(_c1 as string) as sm_ship_mode_id,
	cast(_c2 as string) as sm_type,
	cast(_c3 as string) as sm_code,
	cast(_c4 as string) as sm_carrier,
	cast(_c5 as string) as sm_contract
from ship_mode_staging;
insert into store_returns
select
	cast(_c0 as long) as sr_returned_date_sk,
	cast(_c1 as long) as sr_return_time_sk,
	cast(_c2 as long) as sr_item_sk,
	cast(_c3 as long) as sr_customer_sk,
	cast(_c4 as long) as sr_cdemo_sk,
	cast(_c5 as long) as sr_hdemo_sk,
	cast(_c6 as long) as sr_addr_sk,
	cast(_c7 as long) as sr_store_sk,
	cast(_c8 as long) as sr_reason_sk,
	cast(_c9 as long) as sr_ticket_number,
	cast(_c10 as int) as sr_return_quantity,
	cast(_c11 as double) as sr_return_amt,
	cast(_c12 as double) as sr_return_tax,
	cast(_c13 as double) as sr_return_amt_inc_tax,
	cast(_c14 as double) as sr_fee,
	cast(_c15 as double) as sr_return_ship_cost,
	cast(_c16 as double) as sr_refunded_cash,
	cast(_c17 as double) as sr_reversed_charge,
	cast(_c18 as double) as sr_store_credit,
	cast(_c19 as double) as sr_net_loss
from store_returns_staging;
insert into store_sales
select
	cast(_c0 as long) as ss_sold_date_sk,
	cast(_c1 as long) as ss_sold_time_sk,
	cast(_c2 as long) as ss_item_sk,
	cast(_c3 as long) as ss_customer_sk,
	cast(_c4 as long) as ss_cdemo_sk,
	cast(_c5 as long) as ss_hdemo_sk,
	cast(_c6 as long) as ss_addr_sk,
	cast(_c7 as long) as ss_store_sk,
	cast(_c8 as long) as ss_promo_sk,
	cast(_c9 as long) as ss_ticket_number,
	cast(_c10 as int) as ss_quantity,
	cast(_c11 as double) as ss_wholesale_cost,
	cast(_c12 as double) as ss_list_price,
	cast(_c13 as double) as ss_sales_price,
	cast(_c14 as double) as ss_ext_discount_amt,
	cast(_c15 as double) as ss_ext_sales_price,
	cast(_c16 as double) as ss_ext_wholesale_cost,
	cast(_c17 as double) as ss_ext_list_price,
	cast(_c18 as double) as ss_ext_tax,
	cast(_c19 as double) as ss_coupon_amt,
	cast(_c20 as double) as ss_net_paid,
	cast(_c21 as double) as ss_net_paid_inc_tax,
	cast(_c22 as double) as ss_net_profit
from store_sales_staging;
insert into store
select
	cast(_c0 as long) as s_store_sk,
	cast(_c1 as string) as s_store_id,
	cast(_c2 as string) as s_rec_start_date,
	cast(_c3 as string) as s_rec_end_date,
	cast(_c4 as long) as s_closed_date_sk,
	cast(_c5 as string) as s_store_name,
	cast(_c6 as int) as s_number_employees,
	cast(_c7 as int) as s_floor_space,
	cast(_c8 as string) as s_hours,
	cast(_c9 as string) as s_manager,
	cast(_c10 as int) as s_market_id,
	cast(_c11 as string) as s_geography_class,
	cast(_c12 as string) as s_market_desc,
	cast(_c13 as string) as s_market_manager,
	cast(_c14 as int) as s_division_id,
	cast(_c15 as string) as s_division_name,
	cast(_c16 as int) as s_company_id,
	cast(_c17 as string) as s_company_name,
	cast(_c18 as string) as s_street_number,
	cast(_c19 as string) as s_street_name,
	cast(_c20 as string) as s_street_type,
	cast(_c21 as string) as s_suite_number,
	cast(_c22 as string) as s_city,
	cast(_c23 as string) as s_county,
	cast(_c24 as string) as s_state,
	cast(_c25 as string) as s_zip,
	cast(_c26 as string) as s_country,
	cast(_c27 as double) as s_gmt_offset,
	cast(_c28 as double) as s_tax_precentage
from store_staging;
insert into time_dim
select
	cast(_c0 as long) as t_time_sk,
	cast(_c1 as string) as t_time_id,
	cast(_c2 as int) as t_time,
	cast(_c3 as int) as t_hour,
	cast(_c4 as int) as t_minute,
	cast(_c5 as int) as t_second,
	cast(_c6 as string) as t_am_pm,
	cast(_c7 as string) as t_shift,
	cast(_c8 as string) as t_sub_shift,
	cast(_c9 as string) as t_meal_time
from time_dim_staging;
insert into warehouse
select
	cast(_c0 as long) as w_warehouse_sk,
	cast(_c1 as string) as w_warehouse_id,
	cast(_c2 as string) as w_warehouse_name,
	cast(_c3 as int) as w_warehouse_sq_ft,
	cast(_c4 as string) as w_street_number,
	cast(_c5 as string) as w_street_name,
	cast(_c6 as string) as w_street_type,
	cast(_c7 as string) as w_suite_number,
	cast(_c8 as string) as w_city,
	cast(_c9 as string) as w_county,
	cast(_c10 as string) as w_state,
	cast(_c11 as string) as w_zip,
	cast(_c12 as string) as w_country,
	cast(_c13 as double) as w_gmt_offset
from warehouse_staging;
insert into web_page
select
	cast(_c0 as long) as wp_web_page_sk,
	cast(_c1 as string) as wp_web_page_id,
	cast(_c2 as string) as wp_rec_start_date,
	cast(_c3 as string) as wp_rec_end_date,
	cast(_c4 as long) as wp_creation_date_sk,
	cast(_c5 as long) as wp_access_date_sk,
	cast(_c6 as string) as wp_autogen_flag,
	cast(_c7 as long) as wp_customer_sk,
	cast(_c8 as string) as wp_url,
	cast(_c9 as string) as wp_type,
	cast(_c10 as int) as wp_char_count,
	cast(_c11 as int) as wp_link_count,
	cast(_c12 as int) as wp_image_count,
	cast(_c13 as int) as wp_max_ad_count
from web_page_staging;
insert into web_returns
select
	cast(_c0 as long) as wr_returned_date_sk,
	cast(_c1 as long) as wr_returned_time_sk,
	cast(_c2 as long) as wr_item_sk,
	cast(_c3 as long) as wr_refunded_customer_sk,
	cast(_c4 as long) as wr_refunded_cdemo_sk,
	cast(_c5 as long) as wr_refunded_hdemo_sk,
	cast(_c6 as long) as wr_refunded_addr_sk,
	cast(_c7 as long) as wr_returning_customer_sk,
	cast(_c8 as long) as wr_returning_cdemo_sk,
	cast(_c9 as long) as wr_returning_hdemo_sk,
	cast(_c10 as long) as wr_returning_addr_sk,
	cast(_c11 as long) as wr_web_page_sk,
	cast(_c12 as long) as wr_reason_sk,
	cast(_c13 as long) as wr_order_number,
	cast(_c14 as int) as wr_return_quantity,
	cast(_c15 as double) as wr_return_amt,
	cast(_c16 as double) as wr_return_tax,
	cast(_c17 as double) as wr_return_amt_inc_tax,
	cast(_c18 as double) as wr_fee,
	cast(_c19 as double) as wr_return_ship_cost,
	cast(_c20 as double) as wr_refunded_cash,
	cast(_c21 as double) as wr_reversed_charge,
	cast(_c22 as double) as wr_account_credit,
	cast(_c23 as double) as wr_net_loss
from web_returns_staging;
insert into web_sales
select
	cast(_c0 as long) as ws_sold_date_sk,
	cast(_c1 as long) as ws_sold_time_sk,
	cast(_c2 as long) as ws_ship_date_sk,
	cast(_c3 as long) as ws_item_sk,
	cast(_c4 as long) as ws_bill_customer_sk,
	cast(_c5 as long) as ws_bill_cdemo_sk,
	cast(_c6 as long) as ws_bill_hdemo_sk,
	cast(_c7 as long) as ws_bill_addr_sk,
	cast(_c8 as long) as ws_ship_customer_sk,
	cast(_c9 as long) as ws_ship_cdemo_sk,
	cast(_c10 as long) as ws_ship_hdemo_sk,
	cast(_c11 as long) as ws_ship_addr_sk,
	cast(_c12 as long) as ws_web_page_sk,
	cast(_c13 as long) as ws_web_site_sk,
	cast(_c14 as long) as ws_ship_mode_sk,
	cast(_c15 as long) as ws_warehouse_sk,
	cast(_c16 as long) as ws_promo_sk,
	cast(_c17 as long) as ws_order_number,
	cast(_c18 as int) as ws_quantity,
	cast(_c19 as double) as ws_wholesale_cost,
	cast(_c20 as double) as ws_list_price,
	cast(_c21 as double) as ws_sales_price,
	cast(_c22 as double) as ws_ext_discount_amt,
	cast(_c23 as double) as ws_ext_sales_price,
	cast(_c24 as double) as ws_ext_wholesale_cost,
	cast(_c25 as double) as ws_ext_list_price,
	cast(_c26 as double) as ws_ext_tax,
	cast(_c27 as double) as ws_coupon_amt,
	cast(_c28 as double) as ws_ext_ship_cost,
	cast(_c29 as double) as ws_net_paid,
	cast(_c30 as double) as ws_net_paid_inc_tax,
	cast(_c31 as double) as ws_net_paid_inc_ship,
	cast(_c32 as double) as ws_net_paid_inc_ship_tax,
	cast(_c33 as double) as ws_net_profit
from web_sales_staging;
insert into web_site
select
	cast(_c0 as long) as web_site_sk,
	cast(_c1 as string) as web_site_id,
	cast(_c2 as string) as web_rec_start_date,
	cast(_c3 as string) as web_rec_end_date,
	cast(_c4 as string) as web_name,
	cast(_c5 as long) as web_open_date_sk,
	cast(_c6 as long) as web_close_date_sk,
	cast(_c7 as string) as web_class,
	cast(_c8 as string) as web_manager,
	cast(_c9 as int) as web_mkt_id,
	cast(_c10 as string) as web_mkt_class,
	cast(_c11 as string) as web_mkt_desc,
	cast(_c12 as string) as web_market_manager,
	cast(_c13 as int) as web_company_id,
	cast(_c14 as string) as web_company_name,
	cast(_c15 as string) as web_street_number,
	cast(_c16 as string) as web_street_name,
	cast(_c17 as string) as web_street_type,
	cast(_c18 as string) as web_suite_number,
	cast(_c19 as string) as web_city,
	cast(_c20 as string) as web_county,
	cast(_c21 as string) as web_state,
	cast(_c22 as string) as web_zip,
	cast(_c23 as string) as web_country,
	cast(_c24 as double) as web_gmt_offset,
	cast(_c25 as double) as web_tax_percentage
from web_site_staging;


