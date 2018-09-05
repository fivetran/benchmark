create table call_center(
      cc_call_center_sk         bigint               
,     cc_call_center_id         nvarchar(16)              
,     cc_rec_start_date        nvarchar(10)                         
,     cc_rec_end_date          nvarchar(10)                         
,     cc_closed_date_sk         bigint                       
,     cc_open_date_sk           bigint                       
,     cc_name                   nvarchar(50)                   
,     cc_class                  nvarchar(50)                   
,     cc_employees              int                       
,     cc_sq_ft                  int                       
,     cc_hours                  nvarchar(20)                      
,     cc_manager                nvarchar(40)                   
,     cc_mkt_id                 int                       
,     cc_mkt_class              nvarchar(50)                      
,     cc_mkt_desc               nvarchar(100)                  
,     cc_market_manager         nvarchar(40)                   
,     cc_division               int                       
,     cc_division_name          nvarchar(50)                   
,     cc_company                int                       
,     cc_company_name           nvarchar(50)                      
,     cc_street_number          nvarchar(10)                      
,     cc_street_name            nvarchar(60)                   
,     cc_street_type            nvarchar(15)                      
,     cc_suite_number           nvarchar(10)                      
,     cc_city                   nvarchar(60)                   
,     cc_county                 nvarchar(30)                   
,     cc_state                  nvarchar(2)                       
,     cc_zip                    nvarchar(10)                      
,     cc_country                nvarchar(20)                   
,     cc_gmt_offset             float                  
,     cc_tax_percentage         float
);

create table catalog_page(
      cp_catalog_page_sk        bigint               
,     cp_catalog_page_id        nvarchar(16)              
,     cp_start_date_sk          bigint                       
,     cp_end_date_sk            bigint                       
,     cp_department             nvarchar(50)                   
,     cp_catalog_number         int                       
,     cp_catalog_page_number    int                       
,     cp_description            nvarchar(100)                  
,     cp_type                   nvarchar(100)
);

create table catalog_returns(
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
    cr_return_amount          float,
    cr_return_tax             float,
    cr_return_amt_inc_tax     float,
    cr_fee                    float,
    cr_return_ship_cost       float,
    cr_refunded_cash          float,
    cr_reversed_charge        float,
    cr_store_credit           float,
    cr_net_loss               float
);

create table catalog_sales (
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
    cs_wholesale_cost         float,
    cs_list_price             float,
    cs_sales_price            float,
    cs_ext_discount_amt       float,
    cs_ext_sales_price        float,
    cs_ext_wholesale_cost     float,
    cs_ext_list_price         float,
    cs_ext_tax                float,
    cs_coupon_amt             float,
    cs_ext_ship_cost          float,
    cs_net_paid               float,
    cs_net_paid_inc_tax       float,
    cs_net_paid_inc_ship      float,
    cs_net_paid_inc_ship_tax  float,
    cs_net_profit             float
);

create table customer_address (
    ca_address_sk             bigint,
    ca_address_id             nvarchar(16),
    ca_street_number          nvarchar(10),
    ca_street_name            nvarchar(60),
    ca_street_type            nvarchar(15),
    ca_suite_number           nvarchar(10),
    ca_city                   nvarchar(60),
    ca_county                 nvarchar(30),
    ca_state                  nvarchar(2),
    ca_zip                    nvarchar(10),
    ca_country                nvarchar(20),
    ca_gmt_offset             float,
    ca_location_type          nvarchar(20)
);

create table customer_demographics (
    cd_demo_sk                bigint,
    cd_gender                 nvarchar(1),
    cd_marital_status         nvarchar(1),
    cd_education_status       nvarchar(20),
    cd_purchase_estimate      int,
    cd_credit_rating          nvarchar(10),
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
);

create table customer (
    c_customer_sk             bigint,
    c_customer_id             nvarchar(16),
    c_current_cdemo_sk        bigint,
    c_current_hdemo_sk        bigint,
    c_current_addr_sk         bigint,
    c_first_shipto_date_sk    bigint,
    c_first_sales_date_sk     bigint,
    c_salutation              nvarchar(10),
    c_first_name              nvarchar(20),
    c_last_name               nvarchar(30),
    c_preferred_cust_flag     nvarchar(1),
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           nvarchar(20),
    c_login                   nvarchar(13),
    c_email_address           nvarchar(50),
    c_last_review_date        nvarchar(10)
);

create table date_dim (
    d_date_sk                 bigint,
    d_date_id                 nvarchar(16),
    d_date                    nvarchar(10),
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
    d_day_name                nvarchar(9),
    d_quarter_name            nvarchar(6),
    d_holiday                 nvarchar(1),
    d_weekend                 nvarchar(1),
    d_following_holiday       nvarchar(1),
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             nvarchar(1),
    d_current_week            nvarchar(1),
    d_current_month           nvarchar(1),
    d_current_quarter         nvarchar(1),
    d_current_year            nvarchar(1) 
);

create table household_demographics (
    hd_demo_sk                bigint,
    hd_income_band_sk         bigint,
    hd_buy_potential          nvarchar(15),
    hd_dep_count              int,
    hd_vehicle_count          int
);

create table income_band(
      ib_income_band_sk         bigint               
,     ib_lower_bound            int                       
,     ib_upper_bound            int
);

create table inventory (
    inv_date_sk            bigint,
    inv_item_sk            bigint,
    inv_warehouse_sk        bigint,
    inv_quantity_on_hand    int
);

create table item (
    i_item_sk                 bigint,
    i_item_id                 nvarchar(16),
    i_rec_start_date          nvarchar(10),
    i_rec_end_date            nvarchar(10),
    i_item_desc               nvarchar(200),
    i_current_price           float,
    i_wholesale_cost          float,
    i_brand_id                int,
    i_brand                   nvarchar(50),
    i_class_id                int,
    i_class                   nvarchar(50),
    i_category_id             int,
    i_category                nvarchar(50),
    i_manufact_id             int,
    i_manufact                nvarchar(50),
    i_size                    nvarchar(20),
    i_formulation             nvarchar(20),
    i_color                   nvarchar(20),
    i_units                   nvarchar(10),
    i_container               nvarchar(10),
    i_manager_id              int,
    i_product_name            nvarchar(50)
);

create table promotion (
    p_promo_sk                bigint,
    p_promo_id                nvarchar(16),
    p_start_date_sk           bigint,
    p_end_date_sk             bigint,
    p_item_sk                 bigint,
    p_cost                    float,
    p_response_target         int,
    p_promo_name              nvarchar(50),
    p_channel_dmail           nvarchar(1),
    p_channel_email           nvarchar(1),
    p_channel_catalog         nvarchar(1),
    p_channel_tv              nvarchar(1),
    p_channel_radio           nvarchar(1),
    p_channel_press           nvarchar(1),
    p_channel_event           nvarchar(1),
    p_channel_demo            nvarchar(1),
    p_channel_details         nvarchar(100),
    p_purpose                 nvarchar(15),
    p_discount_active         nvarchar(1) 
);

create table reason(
      r_reason_sk               bigint               
,     r_reason_id               nvarchar(16)              
,     r_reason_desc             nvarchar(100)                
);

create table ship_mode(
      sm_ship_mode_sk           bigint               
,     sm_ship_mode_id           nvarchar(16)              
,     sm_type                   nvarchar(30)                      
,     sm_code                   nvarchar(10)                      
,     sm_carrier                nvarchar(20)                      
,     sm_contract               nvarchar(20)                      
);

create table store_returns (
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
    sr_return_amt             float,
    sr_return_tax             float,
    sr_return_amt_inc_tax     float,
    sr_fee                    float,
    sr_return_ship_cost       float,
    sr_refunded_cash          float,
    sr_reversed_charge        float,
    sr_store_credit           float,
    sr_net_loss               float             
);

create table store_sales (
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
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
);

create table store (
    s_store_sk                bigint,
    s_store_id                nvarchar(16),
    s_rec_start_date          nvarchar(10),
    s_rec_end_date            nvarchar(10),
    s_closed_date_sk          bigint,
    s_store_name              nvarchar(50),
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   nvarchar(20),
    s_manager                 nvarchar(40),
    s_market_id               int,
    s_geography_class         nvarchar(100),
    s_market_desc             nvarchar(100),
    s_market_manager          nvarchar(40),
    s_division_id             int,
    s_division_name           nvarchar(50),
    s_company_id              int,
    s_company_name            nvarchar(50),
    s_street_number           nvarchar(10),
    s_street_name             nvarchar(60),
    s_street_type             nvarchar(15),
    s_suite_number            nvarchar(10),
    s_city                    nvarchar(60),
    s_county                  nvarchar(30),
    s_state                   nvarchar(2),
    s_zip                     nvarchar(10),
    s_country                 nvarchar(20),
    s_gmt_offset              float,
    s_tax_precentage          float                  
);

create table time_dim (
    t_time_sk                 bigint,
    t_time_id                 nvarchar(16),
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   nvarchar(2),
    t_shift                   nvarchar(20),
    t_sub_shift               nvarchar(20),
    t_meal_time               nvarchar(20)
);

create table warehouse(
      w_warehouse_sk            bigint               
,     w_warehouse_id            nvarchar(16)              
,     w_warehouse_name          nvarchar(20)                   
,     w_warehouse_sq_ft         int                       
,     w_street_number           nvarchar(10)                      
,     w_street_name             nvarchar(60)                   
,     w_street_type             nvarchar(15)                      
,     w_suite_number            nvarchar(10)                      
,     w_city                    nvarchar(60)                   
,     w_county                  nvarchar(30)                   
,     w_state                   nvarchar(2)                       
,     w_zip                     nvarchar(10)                      
,     w_country                 nvarchar(20)                   
,     w_gmt_offset              float                  
);

create table web_page(
      wp_web_page_sk            bigint               
,     wp_web_page_id            nvarchar(16)              
,     wp_rec_start_date         nvarchar(10)                         
,     wp_rec_end_date           nvarchar(10)                         
,     wp_creation_date_sk       bigint                       
,     wp_access_date_sk         bigint                       
,     wp_autogen_flag           nvarchar(1)                       
,     wp_customer_sk            bigint
,     wp_url                    nvarchar(100)                  
,     wp_type                   nvarchar(50)                      
,     wp_char_count             int                       
,     wp_link_count             int                       
,     wp_image_count            int                       
,     wp_max_ad_count           int
);

create table web_returns (
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
    wr_return_amt             float,
    wr_return_tax             float,
    wr_return_amt_inc_tax     float,
    wr_fee                    float,
    wr_return_ship_cost       float,
    wr_refunded_cash          float,
    wr_reversed_charge        float,
    wr_account_credit         float,
    wr_net_loss               float
);

create table web_sales (
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
    ws_wholesale_cost         float,
    ws_list_price             float,
    ws_sales_price            float,
    ws_ext_discount_amt       float,
    ws_ext_sales_price        float,
    ws_ext_wholesale_cost     float,
    ws_ext_list_price         float,
    ws_ext_tax                float,
    ws_coupon_amt             float,
    ws_ext_ship_cost          float,
    ws_net_paid               float,
    ws_net_paid_inc_tax       float,
    ws_net_paid_inc_ship      float,
    ws_net_paid_inc_ship_tax  float,
    ws_net_profit             float
);

create table web_site (
    web_site_sk           bigint,
    web_site_id           nvarchar(16),
    web_rec_start_date    nvarchar(10),
    web_rec_end_date      nvarchar(10),
    web_name              nvarchar(50),
    web_open_date_sk      bigint,
    web_close_date_sk     bigint,
    web_class             nvarchar(50),
    web_manager           nvarchar(40),
    web_mkt_id            int,
    web_mkt_class         nvarchar(50),
    web_mkt_desc          nvarchar(100),
    web_market_manager    nvarchar(40),
    web_company_id        int,
    web_company_name      nvarchar(50),
    web_street_number     nvarchar(10),
    web_street_name       nvarchar(60),
    web_street_type       nvarchar(15),
    web_suite_number      nvarchar(10),
    web_city              nvarchar(60),
    web_county            nvarchar(30),
    web_state             nvarchar(2),
    web_zip               nvarchar(10),
    web_country           nvarchar(20),
    web_gmt_offset        float,
    web_tax_percentage    float
);

