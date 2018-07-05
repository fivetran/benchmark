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
create table public.call_center( cc_call_center_sk bigint, cc_call_center_id char(16), cc_rec_start_date varchar(10), cc_rec_end_date varchar(10), cc_closed_date_sk bigint, cc_open_date_sk bigint, cc_name varchar(50), cc_class varchar(50), cc_employees int, cc_sq_ft int, cc_hours char(20), cc_manager varchar(40), cc_mkt_id int, cc_mkt_class char(50), cc_mkt_desc varchar(100), cc_market_manager varchar(40), cc_division int, cc_division_name varchar(50), cc_company int, cc_company_name char(50), cc_street_number char(10), cc_street_name varchar(60), cc_street_type char(15), cc_suite_number char(10), cc_city varchar(60), cc_county varchar(30), cc_state char(2), cc_zip char(10), cc_country varchar(20), cc_gmt_offset double precision, cc_tax_percentage double precision ) diststyle all sortkey (cc_call_center_sk) ;
create table public.catalog_page( cp_catalog_page_sk bigint, cp_catalog_page_id char(16), cp_start_date_sk bigint, cp_end_date_sk bigint, cp_department varchar(50), cp_catalog_number int, cp_catalog_page_number int, cp_description varchar(100), cp_type varchar(100) ) diststyle all sortkey (cp_catalog_page_sk) ;
create table public.catalog_returns( cr_returned_date_sk bigint, cr_returned_time_sk bigint, cr_item_sk bigint, cr_refunded_customer_sk bigint, cr_refunded_cdemo_sk bigint, cr_refunded_hdemo_sk bigint, cr_refunded_addr_sk bigint, cr_returning_customer_sk bigint, cr_returning_cdemo_sk bigint, cr_returning_hdemo_sk bigint, cr_returning_addr_sk bigint, cr_call_center_sk bigint, cr_catalog_page_sk bigint, cr_ship_mode_sk bigint, cr_warehouse_sk bigint, cr_reason_sk bigint, cr_order_number bigint, cr_return_quantity int, cr_return_amount double precision, cr_return_tax double precision, cr_return_amt_inc_tax double precision, cr_fee double precision, cr_return_ship_cost double precision, cr_refunded_cash double precision, cr_reversed_charge double precision, cr_store_credit double precision, cr_net_loss double precision ) distkey(cr_item_sk) sortkey (cr_returned_date_sk) ; 
create table public.catalog_sales( cs_sold_date_sk bigint, cs_sold_time_sk bigint, cs_ship_date_sk bigint, cs_bill_customer_sk bigint, cs_bill_cdemo_sk bigint, cs_bill_hdemo_sk bigint, cs_bill_addr_sk bigint, cs_ship_customer_sk bigint, cs_ship_cdemo_sk bigint, cs_ship_hdemo_sk bigint, cs_ship_addr_sk bigint, cs_call_center_sk bigint, cs_catalog_page_sk bigint, cs_ship_mode_sk bigint, cs_warehouse_sk bigint, cs_item_sk bigint, cs_promo_sk bigint, cs_order_number bigint, cs_quantity int, cs_wholesale_cost double precision, cs_list_price double precision, cs_sales_price double precision, cs_ext_discount_amt double precision, cs_ext_sales_price double precision, cs_ext_wholesale_cost double precision, cs_ext_list_price double precision, cs_ext_tax double precision, cs_coupon_amt double precision, cs_ext_ship_cost double precision, cs_net_paid double precision, cs_net_paid_inc_tax double precision, cs_net_paid_inc_ship double precision, cs_net_paid_inc_ship_tax double precision, cs_net_profit double precision ) distkey(cs_item_sk) sortkey (cs_sold_date_sk) ;  
create table public.customer_address( ca_address_sk bigint, ca_address_id char(16), ca_street_number char(10), ca_street_name varchar(60), ca_street_type char(15), ca_suite_number char(10), ca_city varchar(60), ca_county varchar(30), ca_state char(2), ca_zip char(10), ca_country varchar(20), ca_gmt_offset double precision, ca_location_type char(20) ) distkey (ca_address_sk)  sortkey (ca_country, ca_gmt_offset, ca_state, ca_city) ; 
create table public.customer_demographics( cd_demo_sk bigint, cd_gender char(1), cd_marital_status char(1), cd_education_status char(20), cd_purchase_estimate int, cd_credit_rating char(10), cd_dep_count int, cd_dep_employed_count int, cd_dep_college_count int ) diststyle all sortkey (cd_demo_sk) ;
create table public.customer( c_customer_sk bigint, c_customer_id char(16), c_current_cdemo_sk bigint, c_current_hdemo_sk bigint, c_current_addr_sk bigint, c_first_shipto_date_sk bigint, c_first_sales_date_sk bigint, c_salutation char(10), c_first_name char(20), c_last_name char(30), c_preferred_cust_flag char(1), c_birth_day int, c_birth_month int, c_birth_year int, c_birth_country varchar(20), c_login char(13), c_email_address char(50), c_last_review_date char(10) ) distkey(c_customer_sk) sortkey (c_preferred_cust_flag, c_birth_month) ; 
create table public.date_dim( d_date_sk bigint, d_date_id char(16), d_date varchar(10), d_month_seq int, d_week_seq int, d_quarter_seq int, d_year int, d_dow int, d_moy int, d_dom int, d_qoy int, d_fy_year int, d_fy_quarter_seq int, d_fy_week_seq int, d_day_name char(9), d_quarter_name char(6), d_holiday char(1), d_weekend char(1), d_following_holiday char(1), d_first_dom int, d_last_dom int, d_same_day_ly int, d_same_day_lq int, d_current_day char(1), d_current_week char(1), d_current_month char(1), d_current_quarter char(1), d_current_year char(1) ) diststyle all sortkey (d_date) ; 
create table public.household_demographics( hd_demo_sk bigint, hd_income_band_sk bigint, hd_buy_potential char(15), hd_dep_count int, hd_vehicle_count int ) diststyle all sortkey (hd_demo_sk) ; 
create table public.income_band( ib_income_band_sk bigint, ib_lower_bound int, ib_upper_bound int ) diststyle all sortkey (ib_income_band_sk) ;
create table public.inventory( inv_date_sk bigint, inv_item_sk bigint, inv_warehouse_sk bigint, inv_quantity_on_hand int ) distkey(inv_item_sk) sortkey (inv_quantity_on_hand) ; 
create table public.item( i_item_sk bigint, i_item_id char(16), i_rec_start_date varchar(10), i_rec_end_date varchar(10), i_item_desc varchar(200), i_current_price double precision, i_wholesale_cost double precision, i_brand_id int, i_brand char(50), i_class_id int, i_class char(50), i_category_id int, i_category char(50), i_manufact_id int, i_manufact char(50), i_size char(20), i_formulation char(20), i_color char(20), i_units char(10), i_container char(10), i_manager_id int, i_product_name char(50) ) distkey(i_item_sk) sortkey (i_category, i_brand, i_manager_id) ; 
create table public.promotion( p_promo_sk bigint, p_promo_id char(16), p_start_date_sk bigint, p_end_date_sk bigint, p_item_sk bigint, p_cost double precision, p_response_target int, p_promo_name char(50), p_channel_dmail char(1), p_channel_email char(1), p_channel_catalog char(1), p_channel_tv char(1), p_channel_radio char(1), p_channel_press char(1), p_channel_event char(1), p_channel_demo char(1), p_channel_details varchar(100), p_purpose char(15), p_discount_active char(1) ) diststyle all sortkey (p_promo_sk) ;
create table public.reason( r_reason_sk bigint, r_reason_id char(16), r_reason_desc char(100) ) diststyle all sortkey (r_reason_sk) ;
create table public.ship_mode( sm_ship_mode_sk bigint, sm_ship_mode_id char(16), sm_type char(30), sm_code char(10), sm_carrier char(20), sm_contract char(20) ) diststyle all sortkey (sm_ship_mode_sk) ;
create table public.store_returns( sr_returned_date_sk bigint, sr_return_time_sk bigint, sr_item_sk bigint, sr_customer_sk bigint, sr_cdemo_sk bigint, sr_hdemo_sk bigint, sr_addr_sk bigint, sr_store_sk bigint, sr_reason_sk bigint, sr_ticket_number bigint, sr_return_quantity int, sr_return_amt double precision, sr_return_tax double precision, sr_return_amt_inc_tax double precision, sr_fee double precision, sr_return_ship_cost double precision, sr_refunded_cash double precision, sr_reversed_charge double precision, sr_store_credit double precision, sr_net_loss double precision ) distkey(sr_item_sk) sortkey (sr_returned_date_sk) ; 
create table public.store_sales( ss_sold_date_sk bigint, ss_sold_time_sk bigint, ss_item_sk bigint, ss_customer_sk bigint, ss_cdemo_sk bigint, ss_hdemo_sk bigint, ss_addr_sk bigint, ss_store_sk bigint, ss_promo_sk bigint, ss_ticket_number bigint, ss_quantity int, ss_wholesale_cost double precision, ss_list_price double precision, ss_sales_price double precision, ss_ext_discount_amt double precision, ss_ext_sales_price double precision, ss_ext_wholesale_cost double precision, ss_ext_list_price double precision, ss_ext_tax double precision, ss_coupon_amt double precision, ss_net_paid double precision, ss_net_paid_inc_tax double precision, ss_net_profit double precision ) distkey(ss_item_sk) sortkey (ss_sold_date_sk,ss_store_sk,ss_promo_sk,ss_customer_sk,ss_net_profit,ss_sales_price,ss_quantity) ; 
create table public.store( s_store_sk bigint, s_store_id char(16), s_rec_start_date varchar(10), s_rec_end_date varchar(10), s_closed_date_sk bigint, s_store_name varchar(50), s_number_employees int, s_floor_space int, s_hours char(20), s_manager varchar(40), s_market_id int, s_geography_class varchar(100), s_market_desc varchar(100), s_market_manager varchar(40), s_division_id int, s_division_name varchar(50), s_company_id int, s_company_name varchar(50), s_street_number varchar(10), s_street_name varchar(60), s_street_type char(15), s_suite_number char(10), s_city varchar(60), s_county varchar(30), s_state char(2), s_zip char(10), s_country varchar(20), s_gmt_offset double precision, s_tax_precentage double precision ) diststyle all sortkey (s_state,s_county) ;
create table public.time_dim( t_time_sk bigint, t_time_id char(16), t_time int, t_hour int, t_minute int, t_second int, t_am_pm char(2), t_shift char(20), t_sub_shift char(20), t_meal_time char(20) ) diststyle all sortkey (t_hour,t_minute) ; 
create table public.warehouse( w_warehouse_sk bigint, w_warehouse_id char(16), w_warehouse_name varchar(20), w_warehouse_sq_ft int, w_street_number char(10), w_street_name varchar(60), w_street_type char(15), w_suite_number char(10), w_city varchar(60), w_county varchar(30), w_state char(2), w_zip char(10), w_country varchar(20), w_gmt_offset double precision ) diststyle all sortkey (w_warehouse_sk) ;
create table public.web_page( wp_web_page_sk bigint, wp_web_page_id char(16), wp_rec_start_date varchar(10), wp_rec_end_date varchar(10), wp_creation_date_sk bigint, wp_access_date_sk bigint, wp_autogen_flag char(1), wp_customer_sk bigint, wp_url varchar(100), wp_type char(50), wp_char_count int, wp_link_count int, wp_image_count int, wp_max_ad_count int ) diststyle all sortkey (wp_web_page_sk) ;
create table public.web_returns( wr_returned_date_sk bigint, wr_returned_time_sk bigint, wr_item_sk bigint, wr_refunded_customer_sk bigint, wr_refunded_cdemo_sk bigint, wr_refunded_hdemo_sk bigint, wr_refunded_addr_sk bigint, wr_returning_customer_sk bigint, wr_returning_cdemo_sk bigint, wr_returning_hdemo_sk bigint, wr_returning_addr_sk bigint, wr_web_page_sk bigint, wr_reason_sk bigint, wr_order_number bigint, wr_return_quantity int, wr_return_amt double precision, wr_return_tax double precision, wr_return_amt_inc_tax double precision, wr_fee double precision, wr_return_ship_cost double precision, wr_refunded_cash double precision, wr_reversed_charge double precision, wr_account_credit double precision, wr_net_loss double precision ) distkey(wr_item_sk) sortkey (wr_returned_date_sk) ; 
create table public.web_sales( ws_sold_date_sk bigint, ws_sold_time_sk bigint, ws_ship_date_sk bigint, ws_item_sk bigint, ws_bill_customer_sk bigint, ws_bill_cdemo_sk bigint, ws_bill_hdemo_sk bigint, ws_bill_addr_sk bigint, ws_ship_customer_sk bigint, ws_ship_cdemo_sk bigint, ws_ship_hdemo_sk bigint, ws_ship_addr_sk bigint, ws_web_page_sk bigint, ws_web_site_sk bigint, ws_ship_mode_sk bigint, ws_warehouse_sk bigint, ws_promo_sk bigint, ws_order_number bigint, ws_quantity int, ws_wholesale_cost double precision, ws_list_price double precision, ws_sales_price double precision, ws_ext_discount_amt double precision, ws_ext_sales_price double precision, ws_ext_wholesale_cost double precision, ws_ext_list_price double precision, ws_ext_tax double precision, ws_coupon_amt double precision, ws_ext_ship_cost double precision, ws_net_paid double precision, ws_net_paid_inc_tax double precision, ws_net_paid_inc_ship double precision, ws_net_paid_inc_ship_tax double precision, ws_net_profit double precision ) distkey(ws_item_sk) sortkey (ws_sold_date_sk, ws_web_page_sk, ws_ship_cdemo_sk, ws_net_profit) ; 
create table public.web_site( web_site_sk bigint, web_site_id char(16), web_rec_start_date varchar(10), web_rec_end_date varchar(10), web_name varchar(50), web_open_date_sk bigint, web_close_date_sk bigint, web_class varchar(50), web_manager varchar(40), web_mkt_id int, web_mkt_class varchar(50), web_mkt_desc varchar(100), web_market_manager varchar(40), web_company_id bigint, web_company_name char(50), web_street_number char(10), web_street_name varchar(60), web_street_type char(15), web_suite_number char(10), web_city varchar(60), web_county varchar(30), web_state char(2), web_zip char(10), web_country varchar(20), web_gmt_offset double precision, web_tax_percentage double precision ) diststyle all sortkey (web_site_sk) ;
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
