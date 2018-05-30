
create table raptor.tpcds.call_center(
      cc_call_center_sk         bigint               
,     cc_call_center_id         varchar              
,     cc_rec_start_date        varchar                         
,     cc_rec_end_date          varchar                         
,     cc_closed_date_sk         bigint                       
,     cc_open_date_sk           bigint                       
,     cc_name                   varchar                   
,     cc_class                  varchar                   
,     cc_employees              int                       
,     cc_sq_ft                  int                       
,     cc_hours                  varchar                      
,     cc_manager                varchar                   
,     cc_mkt_id                 int                       
,     cc_mkt_class              varchar                      
,     cc_mkt_desc               varchar                  
,     cc_market_manager         varchar                   
,     cc_division               int                       
,     cc_division_name          varchar                   
,     cc_company                int                       
,     cc_company_name           varchar                      
,     cc_street_number          varchar                      
,     cc_street_name            varchar                   
,     cc_street_type            varchar                      
,     cc_suite_number           varchar                      
,     cc_city                   varchar                   
,     cc_county                 varchar                   
,     cc_state                  varchar                       
,     cc_zip                    varchar                      
,     cc_country                varchar                   
,     cc_gmt_offset             double                  
,     cc_tax_percentage         double
);

create table raptor.tpcds.catalog_page(
      cp_catalog_page_sk        bigint               
,     cp_catalog_page_id        varchar              
,     cp_start_date_sk          bigint                       
,     cp_end_date_sk            bigint                       
,     cp_department             varchar                   
,     cp_catalog_number         int                       
,     cp_catalog_page_number    int                       
,     cp_description            varchar                  
,     cp_type                   varchar
);

create table raptor.tpcds.catalog_returns
(
    cr_returned_date_sk       bigint,
    cr_returned_time_sk       bigint,
    cr_item_sk                bigint,
    cr_refunded_customer_sk   bigint,
    cr_refunded_cdemo_sk      bigint,
    cr_refunded_hdemo_sk      bigint,
    cr_refunded_addr_sk       bigint,
    cr_returning_customer_sk  bigint,
    cr_returning_cdemo_sk     bigint,
    cr_returning_hdemo_sk     bigint,
    cr_returning_addr_sk      bigint,
    cr_call_center_sk         bigint,
    cr_catalog_page_sk        bigint,
    cr_ship_mode_sk           bigint,
    cr_warehouse_sk           bigint,
    cr_reason_sk              bigint,
    cr_order_number           bigint,
    cr_return_quantity        int,
    cr_return_amount          double,
    cr_return_tax             double,
    cr_return_amt_inc_tax     double,
    cr_fee                    double,
    cr_return_ship_cost       double,
    cr_refunded_cash          double,
    cr_reversed_charge        double,
    cr_store_credit           double,
    cr_net_loss               double
);

create table raptor.tpcds.catalog_sales
(
    cs_sold_date_sk           bigint,
    cs_sold_time_sk           bigint,
    cs_ship_date_sk           bigint,
    cs_bill_customer_sk       bigint,
    cs_bill_cdemo_sk          bigint,
    cs_bill_hdemo_sk          bigint,
    cs_bill_addr_sk           bigint,
    cs_ship_customer_sk       bigint,
    cs_ship_cdemo_sk          bigint,
    cs_ship_hdemo_sk          bigint,
    cs_ship_addr_sk           bigint,
    cs_call_center_sk         bigint,
    cs_catalog_page_sk        bigint,
    cs_ship_mode_sk           bigint,
    cs_warehouse_sk           bigint,
    cs_item_sk                bigint,
    cs_promo_sk               bigint,
    cs_order_number           bigint,
    cs_quantity               int,
    cs_wholesale_cost         double,
    cs_list_price             double,
    cs_sales_price            double,
    cs_ext_discount_amt       double,
    cs_ext_sales_price        double,
    cs_ext_wholesale_cost     double,
    cs_ext_list_price         double,
    cs_ext_tax                double,
    cs_coupon_amt             double,
    cs_ext_ship_cost          double,
    cs_net_paid               double,
    cs_net_paid_inc_tax       double,
    cs_net_paid_inc_ship      double,
    cs_net_paid_inc_ship_tax  double,
    cs_net_profit             double
);

create table raptor.tpcds.customer_address
(
    ca_address_sk             bigint,
    ca_address_id             varchar,
    ca_street_number          varchar,
    ca_street_name            varchar,
    ca_street_type            varchar,
    ca_suite_number           varchar,
    ca_city                   varchar,
    ca_county                 varchar,
    ca_state                  varchar,
    ca_zip                    varchar,
    ca_country                varchar,
    ca_gmt_offset             double,
    ca_location_type          varchar
);

create table raptor.tpcds.customer_demographics
(
    cd_demo_sk                bigint,
    cd_gender                 varchar,
    cd_marital_status         varchar,
    cd_education_status       varchar,
    cd_purchase_estimate      int,
    cd_credit_rating          varchar,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
);

create table raptor.tpcds.customer
(
    c_customer_sk             bigint,
    c_customer_id             varchar,
    c_current_cdemo_sk        bigint,
    c_current_hdemo_sk        bigint,
    c_current_addr_sk         bigint,
    c_first_shipto_date_sk    bigint,
    c_first_sales_date_sk     bigint,
    c_salutation              varchar,
    c_first_name              varchar,
    c_last_name               varchar,
    c_preferred_cust_flag     varchar,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           varchar,
    c_login                   varchar,
    c_email_address           varchar,
    c_last_review_date        varchar
);

create table raptor.tpcds.date_dim
(
    d_date_sk                 bigint,
    d_date_id                 varchar,
    d_date                    varchar,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                varchar,
    d_quarter_name            varchar,
    d_holiday                 varchar,
    d_weekend                 varchar,
    d_following_holiday       varchar,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             varchar,
    d_current_week            varchar,
    d_current_month           varchar,
    d_current_quarter         varchar,
    d_current_year            varchar 
);

create table raptor.tpcds.household_demographics
(
    hd_demo_sk                bigint,
    hd_income_band_sk         bigint,
    hd_buy_potential          varchar,
    hd_dep_count              int,
    hd_vehicle_count          int
);

create table raptor.tpcds.income_band(
      ib_income_band_sk         bigint               
,     ib_lower_bound            int                       
,     ib_upper_bound            int
);

create table raptor.tpcds.inventory
(
    inv_date_sk            bigint,
    inv_item_sk            bigint,
    inv_warehouse_sk        bigint,
    inv_quantity_on_hand    int
);

create table raptor.tpcds.item
(
    i_item_sk                 bigint,
    i_item_id                 varchar,
    i_rec_start_date          varchar,
    i_rec_end_date            varchar,
    i_item_desc               varchar,
    i_current_price           double,
    i_wholesale_cost          double,
    i_brand_id                int,
    i_brand                   varchar,
    i_class_id                int,
    i_class                   varchar,
    i_category_id             int,
    i_category                varchar,
    i_manufact_id             int,
    i_manufact                varchar,
    i_size                    varchar,
    i_formulation             varchar,
    i_color                   varchar,
    i_units                   varchar,
    i_container               varchar,
    i_manager_id              int,
    i_product_name            varchar
);

create table raptor.tpcds.promotion
(
    p_promo_sk                bigint,
    p_promo_id                varchar,
    p_start_date_sk           bigint,
    p_end_date_sk             bigint,
    p_item_sk                 bigint,
    p_cost                    double,
    p_response_target         int,
    p_promo_name              varchar,
    p_channel_dmail           varchar,
    p_channel_email           varchar,
    p_channel_catalog         varchar,
    p_channel_tv              varchar,
    p_channel_radio           varchar,
    p_channel_press           varchar,
    p_channel_event           varchar,
    p_channel_demo            varchar,
    p_channel_details         varchar,
    p_purpose                 varchar,
    p_discount_active         varchar 
);

create table raptor.tpcds.reason(
      r_reason_sk               bigint               
,     r_reason_id               varchar              
,     r_reason_desc             varchar                
);

create table raptor.tpcds.ship_mode(
      sm_ship_mode_sk           bigint               
,     sm_ship_mode_id           varchar              
,     sm_type                   varchar                      
,     sm_code                   varchar                      
,     sm_carrier                varchar                      
,     sm_contract               varchar                      
);

create table raptor.tpcds.store_returns
(
    sr_returned_date_sk       bigint,
    sr_return_time_sk         bigint,
    sr_item_sk                bigint,
    sr_customer_sk            bigint,
    sr_cdemo_sk               bigint,
    sr_hdemo_sk               bigint,
    sr_addr_sk                bigint,
    sr_store_sk               bigint,
    sr_reason_sk              bigint,
    sr_ticket_number          bigint,
    sr_return_quantity        int,
    sr_return_amt             double,
    sr_return_tax             double,
    sr_return_amt_inc_tax     double,
    sr_fee                    double,
    sr_return_ship_cost       double,
    sr_refunded_cash          double,
    sr_reversed_charge        double,
    sr_store_credit           double,
    sr_net_loss               double             
);

create table raptor.tpcds.store_sales
(
    ss_sold_date_sk           bigint,
    ss_sold_time_sk           bigint,
    ss_item_sk                bigint,
    ss_customer_sk            bigint,
    ss_cdemo_sk               bigint,
    ss_hdemo_sk               bigint,
    ss_addr_sk                bigint,
    ss_store_sk               bigint,
    ss_promo_sk               bigint,
    ss_ticket_number          bigint,
    ss_quantity               int,
    ss_wholesale_cost         double,
    ss_list_price             double,
    ss_sales_price            double,
    ss_ext_discount_amt       double,
    ss_ext_sales_price        double,
    ss_ext_wholesale_cost     double,
    ss_ext_list_price         double,
    ss_ext_tax                double,
    ss_coupon_amt             double,
    ss_net_paid               double,
    ss_net_paid_inc_tax       double,
    ss_net_profit             double                  
);

create table raptor.tpcds.store
(
    s_store_sk                bigint,
    s_store_id                varchar,
    s_rec_start_date          varchar,
    s_rec_end_date            varchar,
    s_closed_date_sk          bigint,
    s_store_name              varchar,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   varchar,
    s_manager                 varchar,
    s_market_id               int,
    s_geography_class         varchar,
    s_market_desc             varchar,
    s_market_manager          varchar,
    s_division_id             int,
    s_division_name           varchar,
    s_company_id              int,
    s_company_name            varchar,
    s_street_number           varchar,
    s_street_name             varchar,
    s_street_type             varchar,
    s_suite_number            varchar,
    s_city                    varchar,
    s_county                  varchar,
    s_state                   varchar,
    s_zip                     varchar,
    s_country                 varchar,
    s_gmt_offset              double,
    s_tax_precentage          double                  
);

create table raptor.tpcds.time_dim
(
    t_time_sk                 bigint,
    t_time_id                 varchar,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   varchar,
    t_shift                   varchar,
    t_sub_shift               varchar,
    t_meal_time               varchar
);

create table raptor.tpcds.warehouse(
      w_warehouse_sk            bigint               
,     w_warehouse_id            varchar              
,     w_warehouse_name          varchar                   
,     w_warehouse_sq_ft         int                       
,     w_street_number           varchar                      
,     w_street_name             varchar                   
,     w_street_type             varchar                      
,     w_suite_number            varchar                      
,     w_city                    varchar                   
,     w_county                  varchar                   
,     w_state                   varchar                       
,     w_zip                     varchar                      
,     w_country                 varchar                   
,     w_gmt_offset              double                  
);

create table raptor.tpcds.web_page(
      wp_web_page_sk            bigint               
,     wp_web_page_id            varchar              
,     wp_rec_start_date        varchar                         
,     wp_rec_end_date          varchar                         
,     wp_creation_date_sk       bigint                       
,     wp_access_date_sk         bigint                       
,     wp_autogen_flag           varchar                       
,     wp_customer_sk            bigint                       
,     wp_url                    varchar                  
,     wp_type                   varchar                      
,     wp_char_count             int                       
,     wp_link_count             int                       
,     wp_image_count            int                       
,     wp_max_ad_count           int
);

create table raptor.tpcds.web_returns
(
    wr_returned_date_sk       bigint,
    wr_returned_time_sk       bigint,
    wr_item_sk                bigint,
    wr_refunded_customer_sk   bigint,
    wr_refunded_cdemo_sk      bigint,
    wr_refunded_hdemo_sk      bigint,
    wr_refunded_addr_sk       bigint,
    wr_returning_customer_sk  bigint,
    wr_returning_cdemo_sk     bigint,
    wr_returning_hdemo_sk     bigint,
    wr_returning_addr_sk      bigint,
    wr_web_page_sk            bigint,
    wr_reason_sk              bigint,
    wr_order_number           bigint,
    wr_return_quantity        int,
    wr_return_amt             double,
    wr_return_tax             double,
    wr_return_amt_inc_tax     double,
    wr_fee                    double,
    wr_return_ship_cost       double,
    wr_refunded_cash          double,
    wr_reversed_charge        double,
    wr_account_credit         double,
    wr_net_loss               double
);

create table raptor.tpcds.web_sales
(
    ws_sold_date_sk           bigint,
    ws_sold_time_sk           bigint,
    ws_ship_date_sk           bigint,
    ws_item_sk                bigint,
    ws_bill_customer_sk       bigint,
    ws_bill_cdemo_sk          bigint,
    ws_bill_hdemo_sk          bigint,
    ws_bill_addr_sk           bigint,
    ws_ship_customer_sk       bigint,
    ws_ship_cdemo_sk          bigint,
    ws_ship_hdemo_sk          bigint,
    ws_ship_addr_sk           bigint,
    ws_web_page_sk            bigint,
    ws_web_site_sk            bigint,
    ws_ship_mode_sk           bigint,
    ws_warehouse_sk           bigint,
    ws_promo_sk               bigint,
    ws_order_number           bigint,
    ws_quantity               int,
    ws_wholesale_cost         double,
    ws_list_price             double,
    ws_sales_price            double,
    ws_ext_discount_amt       double,
    ws_ext_sales_price        double,
    ws_ext_wholesale_cost     double,
    ws_ext_list_price         double,
    ws_ext_tax                double,
    ws_coupon_amt             double,
    ws_ext_ship_cost          double,
    ws_net_paid               double,
    ws_net_paid_inc_tax       double,
    ws_net_paid_inc_ship      double,
    ws_net_paid_inc_ship_tax  double,
    ws_net_profit             double
);

create table raptor.tpcds.web_site
(
    web_site_sk           bigint,
    web_site_id           varchar,
    web_rec_start_date    varchar,
    web_rec_end_date      varchar,
    web_name              varchar,
    web_open_date_sk      bigint,
    web_close_date_sk     bigint,
    web_class             varchar,
    web_manager           varchar,
    web_mkt_id            int,
    web_mkt_class         varchar,
    web_mkt_desc          varchar,
    web_market_manager    varchar,
    web_company_id        int,
    web_company_name      varchar,
    web_street_number     varchar,
    web_street_name       varchar,
    web_street_type       varchar,
    web_suite_number      varchar,
    web_city              varchar,
    web_county            varchar,
    web_state             varchar,
    web_zip               varchar,
    web_country           varchar,
    web_gmt_offset        double,
    web_tax_percentage    double
);

insert into raptor.tpcds.call_center
select * from hive.tpcds_parquet.call_center;

insert into raptor.tpcds.catalog_page
select * from hive.tpcds_parquet.catalog_page;

insert into raptor.tpcds.catalog_returns
select * from hive.tpcds_parquet.catalog_returns;

insert into raptor.tpcds.catalog_sales
select * from hive.tpcds_parquet.catalog_sales;

insert into raptor.tpcds.customer_address
select * from hive.tpcds_parquet.customer_address;

insert into raptor.tpcds.customer_demographics
select * from hive.tpcds_parquet.customer_demographics;

insert into raptor.tpcds.customer
select * from hive.tpcds_parquet.customer;

insert into raptor.tpcds.date_dim
select * from hive.tpcds_parquet.date_dim;

insert into raptor.tpcds.household_demographics 
select * from hive.tpcds_parquet.household_demographics;

insert into raptor.tpcds.income_band
select * from hive.tpcds_parquet.income_band;

insert into raptor.tpcds.inventory
select * from hive.tpcds_parquet.inventory;

insert into raptor.tpcds.item
select * from hive.tpcds_parquet.item;

insert into raptor.tpcds.promotion
select * from hive.tpcds_parquet.promotion;

insert into raptor.tpcds.reason
select * from hive.tpcds_parquet.reason;

insert into raptor.tpcds.ship_mode
select * from hive.tpcds_parquet.ship_mode;

insert into raptor.tpcds.store_returns
select * from hive.tpcds_parquet.store_returns;

insert into raptor.tpcds.store_sales
select * from hive.tpcds_parquet.store_sales;

insert into raptor.tpcds.store
select * from hive.tpcds_parquet.store;

insert into raptor.tpcds.time_dim
select * from hive.tpcds_parquet.time_dim;

insert into raptor.tpcds.warehouse
select * from hive.tpcds_parquet.warehouse;

insert into raptor.tpcds.web_page
select * from hive.tpcds_parquet.web_page;

insert into raptor.tpcds.web_returns
select * from hive.tpcds_parquet.web_returns;

insert into raptor.tpcds.web_sales
select * from hive.tpcds_parquet.web_sales;

insert into raptor.tpcds.web_site
select * from hive.tpcds_parquet.web_site;
