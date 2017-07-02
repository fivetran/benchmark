
drop schema public cascade;

create schema public;

create table public.call_center(
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
,     cc_gmt_offset             double precision                  
,     cc_tax_percentage         double precision
);

copy public.call_center
from 's3://fivetran-benchmark/tpcds/csv/call_center/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.catalog_page(
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

copy public.catalog_page
from 's3://fivetran-benchmark/tpcds/csv/catalog_page/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.catalog_returns
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
    cr_return_amount          double precision,
    cr_return_tax             double precision,
    cr_return_amt_inc_tax     double precision,
    cr_fee                    double precision,
    cr_return_ship_cost       double precision,
    cr_refunded_cash          double precision,
    cr_reversed_charge        double precision,
    cr_store_credit           double precision,
    cr_net_loss               double precision
);

copy public.catalog_returns
from 's3://fivetran-benchmark/tpcds/csv/catalog_returns/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.catalog_sales
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
    cs_wholesale_cost         double precision,
    cs_list_price             double precision,
    cs_sales_price            double precision,
    cs_ext_discount_amt       double precision,
    cs_ext_sales_price        double precision,
    cs_ext_wholesale_cost     double precision,
    cs_ext_list_price         double precision,
    cs_ext_tax                double precision,
    cs_coupon_amt             double precision,
    cs_ext_ship_cost          double precision,
    cs_net_paid               double precision,
    cs_net_paid_inc_tax       double precision,
    cs_net_paid_inc_ship      double precision,
    cs_net_paid_inc_ship_tax  double precision,
    cs_net_profit             double precision
);

copy public.catalog_sales
from 's3://fivetran-benchmark/tpcds/csv/catalog_sales/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.customer_address
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
    ca_gmt_offset             double precision,
    ca_location_type          varchar
);

copy public.customer_address
from 's3://fivetran-benchmark/tpcds/csv/customer_address/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.customer_demographics
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

copy public.customer_demographics
from 's3://fivetran-benchmark/tpcds/csv/customer_demographics/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.customer
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

copy public.customer
from 's3://fivetran-benchmark/tpcds/csv/customer/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.date_dim
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

copy public.date_dim
from 's3://fivetran-benchmark/tpcds/csv/date_dim/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.household_demographics
(
    hd_demo_sk                bigint,
    hd_income_band_sk         bigint,
    hd_buy_potential          varchar,
    hd_dep_count              int,
    hd_vehicle_count          int
);

copy public.household_demographics
from 's3://fivetran-benchmark/tpcds/csv/household_demographics/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.income_band(
      ib_income_band_sk         bigint               
,     ib_lower_bound            int                       
,     ib_upper_bound            int
);

copy public.income_band
from 's3://fivetran-benchmark/tpcds/csv/income_band/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.inventory
(
    inv_date_sk            bigint,
    inv_item_sk            bigint,
    inv_warehouse_sk        bigint,
    inv_quantity_on_hand    int
);

copy public.inventory
from 's3://fivetran-benchmark/tpcds/csv/inventory/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.item
(
    i_item_sk                 bigint,
    i_item_id                 varchar,
    i_rec_start_date          varchar,
    i_rec_end_date            varchar,
    i_item_desc               varchar,
    i_current_price           double precision,
    i_wholesale_cost          double precision,
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

copy public.item
from 's3://fivetran-benchmark/tpcds/csv/item/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.promotion
(
    p_promo_sk                bigint,
    p_promo_id                varchar,
    p_start_date_sk           bigint,
    p_end_date_sk             bigint,
    p_item_sk                 bigint,
    p_cost                    double precision,
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

copy public.promotion
from 's3://fivetran-benchmark/tpcds/csv/promotion/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.reason(
      r_reason_sk               bigint               
,     r_reason_id               varchar              
,     r_reason_desc             varchar                
);

copy public.reason
from 's3://fivetran-benchmark/tpcds/csv/reason/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.ship_mode(
      sm_ship_mode_sk           bigint               
,     sm_ship_mode_id           varchar              
,     sm_type                   varchar                      
,     sm_code                   varchar                      
,     sm_carrier                varchar                      
,     sm_contract               varchar                      
);

copy public.ship_mode
from 's3://fivetran-benchmark/tpcds/csv/ship_mode/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.store_returns
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
    sr_return_amt             double precision,
    sr_return_tax             double precision,
    sr_return_amt_inc_tax     double precision,
    sr_fee                    double precision,
    sr_return_ship_cost       double precision,
    sr_refunded_cash          double precision,
    sr_reversed_charge        double precision,
    sr_store_credit           double precision,
    sr_net_loss               double precision             
);

copy public.store_returns
from 's3://fivetran-benchmark/tpcds/csv/store_returns/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.store_sales
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
    ss_wholesale_cost         double precision,
    ss_list_price             double precision,
    ss_sales_price            double precision,
    ss_ext_discount_amt       double precision,
    ss_ext_sales_price        double precision,
    ss_ext_wholesale_cost     double precision,
    ss_ext_list_price         double precision,
    ss_ext_tax                double precision,
    ss_coupon_amt             double precision,
    ss_net_paid               double precision,
    ss_net_paid_inc_tax       double precision,
    ss_net_profit             double precision                  
);

copy public.store_sales
from 's3://fivetran-benchmark/tpcds/csv/store_sales/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.store
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
    s_gmt_offset              double precision,
    s_tax_precentage          double precision                  
);

copy public.store
from 's3://fivetran-benchmark/tpcds/csv/store/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.time_dim
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

copy public.time_dim
from 's3://fivetran-benchmark/tpcds/csv/time_dim/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.warehouse(
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
,     w_gmt_offset              double precision                  
);

copy public.warehouse
from 's3://fivetran-benchmark/tpcds/csv/warehouse/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.web_page(
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

copy public.web_page
from 's3://fivetran-benchmark/tpcds/csv/web_page/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.web_returns
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
    wr_return_amt             double precision,
    wr_return_tax             double precision,
    wr_return_amt_inc_tax     double precision,
    wr_fee                    double precision,
    wr_return_ship_cost       double precision,
    wr_refunded_cash          double precision,
    wr_reversed_charge        double precision,
    wr_account_credit         double precision,
    wr_net_loss               double precision
);

copy public.web_returns
from 's3://fivetran-benchmark/tpcds/csv/web_returns/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.web_sales
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
    ws_wholesale_cost         double precision,
    ws_list_price             double precision,
    ws_sales_price            double precision,
    ws_ext_discount_amt       double precision,
    ws_ext_sales_price        double precision,
    ws_ext_wholesale_cost     double precision,
    ws_ext_list_price         double precision,
    ws_ext_tax                double precision,
    ws_coupon_amt             double precision,
    ws_ext_ship_cost          double precision,
    ws_net_paid               double precision,
    ws_net_paid_inc_tax       double precision,
    ws_net_paid_inc_ship      double precision,
    ws_net_paid_inc_ship_tax  double precision,
    ws_net_profit             double precision
);

copy public.web_sales
from 's3://fivetran-benchmark/tpcds/csv/web_sales/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';

create table public.web_site
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
    web_gmt_offset        double precision,
    web_tax_percentage    double precision
);

copy public.web_site
from 's3://fivetran-benchmark/tpcds/csv/web_site/'
format delimiter '|' compupdate on
iam_role 'arn:aws:iam::254359228911:role/RedshiftReadS3';