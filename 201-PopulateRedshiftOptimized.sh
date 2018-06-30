# Set up your redshift cluster with WLM configuration:
#
# [ {
#   "query_concurrency" : 5,
#   "memory_percent_to_use" : 100,
#   "query_group" : [ ],
#   "query_group_wild_card" : 0,
#   "user_group" : [ ],
#   "user_group_wild_card" : 0
# }, {
#   "short_query_queue" : false
# } ]
#
# This should be the same as the default configuration, but set it explicitly just to be safe.
# 
# Set KMS encryption to ON.
set -e

# Set HOST, PGPASSWORD, SCALE=100/1000 manually
if [ -z "$HOST" ]; then 
  echo "You must set HOST"
  exit 1
fi 
if [ -z "$PGPASSWORD" ]; then 
  echo "You must set PGPASSWORD"
  exit 1
fi 
if [ -z "$SCALE" ]; then 
  echo "You must set SCALE"
  exit 1
fi 

export DB=dev
export USER=fivetran

echo 'Create tables...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done <<EOF
drop schema public cascade;
create schema public;
create table public.call_center ( cc_call_center_sk integer not null , cc_call_center_id char(16) not null , cc_rec_start_date date , cc_rec_end_date date , cc_closed_date_sk integer , cc_open_date_sk integer , cc_name varchar(50) , cc_class varchar(50) , cc_employees integer , cc_sq_ft integer , cc_hours char(20) , cc_manager varchar(40) , cc_mkt_id integer , cc_mkt_class char(50) , cc_mkt_desc varchar(100) , cc_market_manager varchar(40) , cc_division integer , cc_division_name varchar(50) , cc_company integer , cc_company_name char(50) , cc_street_number char(10) , cc_street_name varchar(60) , cc_street_type char(15) , cc_suite_number char(10) , cc_city varchar(60) , cc_county varchar(30) , cc_state char(2) , cc_zip char(10) , cc_country varchar(20) , cc_gmt_offset decimal(5,2) , cc_tax_percentage decimal(5,2) ) diststyle all sortkey (cc_call_center_sk) ;
create table public.catalog_page ( cp_catalog_page_sk integer not null , cp_catalog_page_id char(16) not null , cp_start_date_sk integer , cp_end_date_sk integer , cp_department varchar(50) , cp_catalog_number integer , cp_catalog_page_number integer , cp_description varchar(100) , cp_type varchar(100) ) diststyle all sortkey (cp_catalog_page_sk) ;
create table public.catalog_returns ( cr_returned_date_sk integer , cr_returned_time_sk integer , cr_item_sk integer not null , cr_refunded_customer_sk integer , cr_refunded_cdemo_sk integer , cr_refunded_hdemo_sk integer , cr_refunded_addr_sk integer , cr_returning_customer_sk integer , cr_returning_cdemo_sk integer , cr_returning_hdemo_sk integer , cr_returning_addr_sk integer , cr_call_center_sk integer , cr_catalog_page_sk integer , cr_ship_mode_sk integer , cr_warehouse_sk integer , cr_reason_sk integer , cr_order_number integer not null , cr_return_quantity integer , cr_return_amount decimal(7,2) , cr_return_tax decimal(7,2) , cr_return_amt_inc_tax decimal(7,2) , cr_fee decimal(7,2) , cr_return_ship_cost decimal(7,2) , cr_refunded_cash decimal(7,2) , cr_reversed_charge decimal(7,2) , cr_store_credit decimal(7,2) , cr_net_loss decimal(7,2) ) distkey (cr_item_sk) sortkey (cr_returned_date_sk) ; 
create table public.catalog_sales ( cs_sold_date_sk integer , cs_sold_time_sk integer , cs_ship_date_sk integer , cs_bill_customer_sk integer , cs_bill_cdemo_sk integer , cs_bill_hdemo_sk integer , cs_bill_addr_sk integer , cs_ship_customer_sk integer , cs_ship_cdemo_sk integer , cs_ship_hdemo_sk integer , cs_ship_addr_sk integer , cs_call_center_sk integer , cs_catalog_page_sk integer , cs_ship_mode_sk integer , cs_warehouse_sk integer , cs_item_sk integer not null , cs_promo_sk integer , cs_order_number integer not null , cs_quantity integer , cs_wholesale_cost decimal(7,2) , cs_list_price decimal(7,2) , cs_sales_price decimal(7,2) , cs_ext_discount_amt decimal(7,2) , cs_ext_sales_price decimal(7,2) , cs_ext_wholesale_cost decimal(7,2) , cs_ext_list_price decimal(7,2) , cs_ext_tax decimal(7,2) , cs_coupon_amt decimal(7,2) , cs_ext_ship_cost decimal(7,2) , cs_net_paid decimal(7,2) , cs_net_paid_inc_tax decimal(7,2) , cs_net_paid_inc_ship decimal(7,2) , cs_net_paid_inc_ship_tax decimal(7,2) , cs_net_profit decimal(7,2) ) distkey (cs_item_sk) sortkey (cs_sold_date_sk) ;  
create table public.customer_address ( ca_address_sk integer not null , ca_address_id char(16) not null , ca_street_number char(10) , ca_street_name varchar(60) , ca_street_type char(15) , ca_suite_number char(10) , ca_city varchar(60) , ca_county varchar(30) , ca_state char(2) , ca_zip char(10) , ca_country varchar(20) , ca_gmt_offset decimal(5,2) , ca_location_type char(20) ) distkey (ca_address_sk) sortkey (ca_country, ca_gmt_offset, ca_state, ca_city) ; 
create table public.customer_demographics ( cd_demo_sk integer not null , cd_gender char(1) , cd_marital_status char(1) , cd_education_status char(20) , cd_purchase_estimate integer , cd_credit_rating char(10) , cd_dep_count integer , cd_dep_employed_count integer , cd_dep_college_count integer ) diststyle all sortkey (cd_demo_sk) ;
create table public.customer ( c_customer_sk integer not null , c_customer_id char(16) not null , c_current_cdemo_sk integer , c_current_hdemo_sk integer , c_current_addr_sk integer , c_first_shipto_date_sk integer , c_first_sales_date_sk integer , c_salutation char(10) , c_first_name char(20) , c_last_name char(30) , c_preferred_cust_flag char(1) , c_birth_day integer , c_birth_month integer , c_birth_year integer , c_birth_country varchar(20) , c_login char(13) , c_email_address char(50) , c_last_review_date char(10) ) distkey (c_customer_sk) sortkey (c_preferred_cust_flag, c_birth_month) ; 
create table public.date_dim ( d_date_sk integer not null , d_date_id char(16) not null , d_date date , d_month_seq integer , d_week_seq integer , d_quarter_seq integer , d_year integer , d_dow integer , d_moy integer , d_dom integer , d_qoy integer , d_fy_year integer , d_fy_quarter_seq integer , d_fy_week_seq integer , d_day_name char(9) , d_quarter_name char(6) , d_holiday char(1) , d_weekend char(1) , d_following_holiday char(1) , d_first_dom integer , d_last_dom integer , d_same_day_ly integer , d_same_day_lq integer , d_current_day char(1) , d_current_week char(1) , d_current_month char(1) , d_current_quarter char(1) , d_current_year char(1) ) diststyle all sortkey (d_date) ; 
create table public.household_demographics ( hd_demo_sk integer not null , hd_income_band_sk integer , hd_buy_potential char(15) , hd_dep_count integer , hd_vehicle_count integer ) diststyle all sortkey (hd_demo_sk) ; 
create table public.income_band ( ib_income_band_sk integer not null , ib_lower_bound integer , ib_upper_bound integer ) diststyle all sortkey (ib_income_band_sk) ;
create table public.inventory ( inv_date_sk integer not null , inv_item_sk integer not null , inv_warehouse_sk integer not null , inv_quantity_on_hand integer ) distkey (inv_item_sk) sortkey (inv_quantity_on_hand) ; 
create table public.item ( i_item_sk integer not null , i_item_id char(16) not null , i_rec_start_date date , i_rec_end_date date , i_item_desc varchar(200) , i_current_price decimal(7,2) , i_wholesale_cost decimal(7,2) , i_brand_id integer , i_brand char(50) , i_class_id integer , i_class char(50) , i_category_id integer , i_category char(50) , i_manufact_id integer , i_manufact char(50) , i_size char(20) , i_formulation char(20) , i_color char(20) , i_units char(10) , i_container char(10) , i_manager_id integer , i_product_name char(50) ) distkey (i_item_sk) sortkey (i_category, i_brand, i_manager_id) ; 
create table public.promotion ( p_promo_sk integer not null , p_promo_id char(16) not null , p_start_date_sk integer , p_end_date_sk integer , p_item_sk integer , p_cost decimal(15,2) , p_response_target integer , p_promo_name char(50) , p_channel_dmail char(1) , p_channel_email char(1) , p_channel_catalog char(1) , p_channel_tv char(1) , p_channel_radio char(1) , p_channel_press char(1) , p_channel_event char(1) , p_channel_demo char(1) , p_channel_details varchar(100) , p_purpose char(15) , p_discount_active char(1) ) diststyle all sortkey (p_promo_sk) ;
create table public.reason ( r_reason_sk integer not null , r_reason_id char(16) not null , r_reason_desc char(100) ) diststyle all sortkey (r_reason_sk) ;
create table public.ship_mode ( sm_ship_mode_sk integer not null , sm_ship_mode_id char(16) not null , sm_type char(30) , sm_code char(10) , sm_carrier char(20) , sm_contract char(20) ) diststyle all sortkey (sm_ship_mode_sk) ;
create table public.store_returns ( sr_returned_date_sk integer , sr_return_time_sk integer , sr_item_sk integer not null , sr_customer_sk integer , sr_cdemo_sk integer , sr_hdemo_sk integer , sr_addr_sk integer , sr_store_sk integer , sr_reason_sk integer , sr_ticket_number integer not null , sr_return_quantity integer , sr_return_amt decimal(7,2) , sr_return_tax decimal(7,2) , sr_return_amt_inc_tax decimal(7,2) , sr_fee decimal(7,2) , sr_return_ship_cost decimal(7,2) , sr_refunded_cash decimal(7,2) , sr_reversed_charge decimal(7,2) , sr_store_credit decimal(7,2) , sr_net_loss decimal(7,2) ) distkey (sr_item_sk) sortkey (sr_returned_date_sk) ; 
create table public.store_sales ( ss_sold_date_sk integer , ss_sold_time_sk integer , ss_item_sk integer not null , ss_customer_sk integer , ss_cdemo_sk integer , ss_hdemo_sk integer , ss_addr_sk integer , ss_store_sk integer , ss_promo_sk integer , ss_ticket_number integer not null , ss_quantity integer , ss_wholesale_cost decimal(7,2) , ss_list_price decimal(7,2) , ss_sales_price decimal(7,2) , ss_ext_discount_amt decimal(7,2) , ss_ext_sales_price decimal(7,2) , ss_ext_wholesale_cost decimal(7,2) , ss_ext_list_price decimal(7,2) , ss_ext_tax decimal(7,2) , ss_coupon_amt decimal(7,2) , ss_net_paid decimal(7,2) , ss_net_paid_inc_tax decimal(7,2) , ss_net_profit decimal(7,2) ) distkey (ss_item_sk) sortkey (ss_sold_date_sk,ss_store_sk,ss_promo_sk,ss_customer_sk,ss_net_profit,ss_sales_price,ss_quantity) ; 
create table public.store ( s_store_sk integer not null , s_store_id char(16) not null , s_rec_start_date date , s_rec_end_date date , s_closed_date_sk integer , s_store_name varchar(50) , s_number_employees integer , s_floor_space integer , s_hours char(20) , s_manager varchar(40) , s_market_id integer , s_geography_class varchar(100) , s_market_desc varchar(100) , s_market_manager varchar(40) , s_division_id integer , s_division_name varchar(50) , s_company_id integer , s_company_name varchar(50) , s_street_number varchar(10) , s_street_name varchar(60) , s_street_type char(15) , s_suite_number char(10) , s_city varchar(60) , s_county varchar(30) , s_state char(2) , s_zip char(10) , s_country varchar(20) , s_gmt_offset decimal(5,2) , s_tax_precentage decimal(5,2) ) diststyle all sortkey (s_state,s_county) ;
create table public.time_dim ( t_time_sk integer not null , t_time_id char(16) not null , t_time integer , t_hour integer , t_minute integer , t_second integer , t_am_pm char(2) , t_shift char(20) , t_sub_shift char(20) , t_meal_time char(20) ) diststyle all sortkey (t_hour,t_minute) ; 
create table public.warehouse ( w_warehouse_sk integer not null , w_warehouse_id char(16) not null , w_warehouse_name varchar(20) , w_warehouse_sq_ft integer , w_street_number char(10) , w_street_name varchar(60) , w_street_type char(15) , w_suite_number char(10) , w_city varchar(60) , w_county varchar(30) , w_state char(2) , w_zip char(10) , w_country varchar(20) , w_gmt_offset decimal(5,2) ) diststyle all sortkey (w_warehouse_sk) ;
create table public.web_page ( wp_web_page_sk integer not null , wp_web_page_id char(16) not null , wp_rec_start_date date , wp_rec_end_date date , wp_creation_date_sk integer , wp_access_date_sk integer , wp_autogen_flag char(1) , wp_customer_sk integer , wp_url varchar(100) , wp_type char(50) , wp_char_count integer , wp_link_count integer , wp_image_count integer , wp_max_ad_count integer ) diststyle all sortkey (wp_web_page_sk) ;
create table public.web_returns ( wr_returned_date_sk integer , wr_returned_time_sk integer , wr_item_sk integer not null , wr_refunded_customer_sk integer , wr_refunded_cdemo_sk integer , wr_refunded_hdemo_sk integer , wr_refunded_addr_sk integer , wr_returning_customer_sk integer , wr_returning_cdemo_sk integer , wr_returning_hdemo_sk integer , wr_returning_addr_sk integer , wr_web_page_sk integer , wr_reason_sk integer , wr_order_number integer not null , wr_return_quantity integer , wr_return_amt decimal(7,2) , wr_return_tax decimal(7,2) , wr_return_amt_inc_tax decimal(7,2) , wr_fee decimal(7,2) , wr_return_ship_cost decimal(7,2) , wr_refunded_cash decimal(7,2) , wr_reversed_charge decimal(7,2) , wr_account_credit decimal(7,2) , wr_net_loss decimal(7,2) ) distkey (wr_item_sk) sortkey (wr_returned_date_sk) ; 
create table public.web_sales ( ws_sold_date_sk integer , ws_sold_time_sk integer , ws_ship_date_sk integer , ws_item_sk integer not null , ws_bill_customer_sk integer , ws_bill_cdemo_sk integer , ws_bill_hdemo_sk integer , ws_bill_addr_sk integer , ws_ship_customer_sk integer , ws_ship_cdemo_sk integer , ws_ship_hdemo_sk integer , ws_ship_addr_sk integer , ws_web_page_sk integer , ws_web_site_sk integer , ws_ship_mode_sk integer , ws_warehouse_sk integer , ws_promo_sk integer , ws_order_number integer not null , ws_quantity integer , ws_wholesale_cost decimal(7,2) , ws_list_price decimal(7,2) , ws_sales_price decimal(7,2) , ws_ext_discount_amt decimal(7,2) , ws_ext_sales_price decimal(7,2) , ws_ext_wholesale_cost decimal(7,2) , ws_ext_list_price decimal(7,2) , ws_ext_tax decimal(7,2) , ws_coupon_amt decimal(7,2) , ws_ext_ship_cost decimal(7,2) , ws_net_paid decimal(7,2) , ws_net_paid_inc_tax decimal(7,2) , ws_net_paid_inc_ship decimal(7,2) , ws_net_paid_inc_ship_tax decimal(7,2) , ws_net_profit decimal(7,2) ) distkey (ws_item_sk) sortkey (ws_sold_date_sk, ws_web_page_sk, ws_ship_cdemo_sk, ws_net_profit) ; 
create table public.web_site ( web_site_sk integer not null , web_site_id char(16) not null , web_rec_start_date date , web_rec_end_date date , web_name varchar(50) , web_open_date_sk integer , web_close_date_sk integer , web_class varchar(50) , web_manager varchar(40) , web_mkt_id integer , web_mkt_class varchar(50) , web_mkt_desc varchar(100) , web_market_manager varchar(40) , web_company_id integer , web_company_name char(50) , web_street_number char(10) , web_street_name varchar(60) , web_street_type char(15) , web_suite_number char(10) , web_city varchar(60) , web_county varchar(30) , web_state char(2) , web_zip char(10) , web_country varchar(20) , web_gmt_offset decimal(5,2) , web_tax_percentage decimal(5,2) ) diststyle all sortkey (web_site_sk) ;
copy public.call_center from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/call_center/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.catalog_page from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/catalog_page/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.catalog_returns from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/catalog_returns/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.catalog_sales from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/catalog_sales/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.customer_address from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/customer_address/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.customer_demographics from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/customer_demographics/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.customer from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/customer/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.date_dim from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/date_dim/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.household_demographics from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/household_demographics/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.income_band from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/income_band/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.inventory from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/inventory/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.item from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/item/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.promotion from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/promotion/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.reason from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/reason/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.ship_mode from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/ship_mode/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.store_returns from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/store_returns/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.store_sales from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/store_sales/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.store from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/store/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.time_dim from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/time_dim/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.warehouse from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/warehouse/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.web_page from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/web_page/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.web_returns from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/web_returns/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.web_sales from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/web_sales/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
copy public.web_site from 's3://fivetran-benchmark/tpcds_${SCALE}_dat/web_site/' region 'us-east-1' format delimiter '|' acceptinvchars compupdate on iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';
EOF

echo 'Creating tpcds_user...'
while read line;
do
  psql --host ${HOST} --port 5439 --user ${USER} ${DB} \
    --echo-queries --output /dev/null \
    --command "$line" 
done <<EOF
create user tpcds_user password 'OaklandOffice1';
grant usage on schema public to tpcds_user;
grant all privileges on all tables in schema public to tpcds_user;
EOF
