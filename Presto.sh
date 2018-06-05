#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
set -x -e

# Variables for running this script
ROLE=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoRole)
HOSTNAME=$(hostname)
DNSNAME=$(dnsdomainname)
FQDN=${HOSTNAME}.${DNSNAME}
PRESTO_MASTER=$(curl -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/PrestoMaster)
PRESTO_MASTER_FQDN=${PRESTO_MASTER}.${DNSNAME}
HTTP_PORT="8080"
TASKS_PER_INSTANCE_PER_QUERY=64
INSTANCE_MEMORY=120000
PRESTO_JVM_MB=$(( ${INSTANCE_MEMORY} * 8 / 10 ))
PRESTO_OVERHEAD=500
PRESTO_QUERY_NODE_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 7 / 10 ))
PRESTO_RESERVED_SYSTEM_MB=$(( (${PRESTO_JVM_MB} - ${PRESTO_OVERHEAD}) * 3 / 10 ))

# Prevents "Too many open files"
ulimit -n 30000

# Configure local ssds
# We're going to mount each disk separately, rather than in a raid0 array, 
# because HDFS has intrinsic parallelism that will give better performance that raid performance
for i in {1..4}; do 
  mkfs.ext4 -F /dev/nvme0n$i
  mkdir -p /mnt/disks/ssd$i
  mount /dev/nvme0n$i /mnt/disks/ssd$i
  chmod a+w /mnt/disks/ssd$i
done

# Install Java
apt-get install openjdk-8-jre-headless -y
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/

# Install Hadoop
wget http://mirrors.sonic.net/apache/hadoop/common/hadoop-2.9.1/hadoop-2.9.1.tar.gz
tar -zxf hadoop-2.9.1.tar.gz
mv hadoop-2.9.1 hadoop
export PATH=/hadoop/bin:$PATH

# Tell Hadoop where to store data
cat > /hadoop/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/mnt/disks/ssd1,/mnt/disks/ssd2,/mnt/disks/ssd3,/mnt/disks/ssd4</value>
        <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/hadoop/dfs/name</value>
        <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
    </property>

    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF

# Tell Hadoop where the hdfs name node lives
cat > /hadoop/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${PRESTO_MASTER}/</value>
    <description>NameNode URI</description>
  </property>

  <property>
    <name>fs.gs.metadata.cache.directory</name>
    <value>/hadoop_gcs_connector_metadata_cache</value>
    <description>
      Only used if fs.gs.metadata.cache.type is FILESYSTEM_BACKED, specifies
      the local path to use as the base path for storing mirrored GCS metadata.
      Must be an absolute path, must be a directory, and must be fully
      readable/writable/executable by any user running processes which use the
      GCS connector.
    </description>
  </property>
  <property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
    <description>The FileSystem for gs: (GCS) uris.</description>
  </property>
  <property>
    <name>fs.gs.project.id</name>
    <value>digital-arbor-400</value>
    <description>
      Google Cloud Project ID with access to configured GCS buckets.
    </description>
  </property>
    <property>
    <name>fs.gs.metadata.cache.enable</name>
    <value>false</value>
    <final>false</final>
    <source>Dataproc Cluster Properties</source>
  </property>
  <property>
    <name>fs.gs.implicit.dir.infer.enable</name>
    <value>true</value>
    <description>
      If set, we create and return in-memory directory objects on the fly when
      no backing object exists, but we know there are files with the same
      prefix.
    </description>
  </property>
  <property>
    <name>fs.gs.application.name.suffix</name>
    <value>-hadoop</value>
    <description>
      Appended to the user-agent header for API requests to GCS to help identify
      the traffic as coming from Dataproc.
    </description>
  </property>
  <property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
    <description>The AbstractFileSystem for gs: (GCS) uris.</description>
  </property>
  <property>
    <name>fs.gs.metadata.cache.type</name>
    <value>FILESYSTEM_BACKED</value>
    <description>
      Specifies which implementation of DirectoryListCache to use for
      supplementing GCS API &amp;amp;quot;list&amp;amp;quot; requests. Supported
      implementations:       IN_MEMORY: Enforces immediate consistency within
      same Java process.       FILESYSTEM_BACKED: Enforces consistency across
      all cooperating processes       pointed at the same local mirror
      directory, which may be an NFS directory       for massively-distributed
      coordination.
    </description>
  </property>
  <property>
    <name>fs.gs.block.size</name>
    <value>134217728</value>
    <final>false</final>
    <source>Dataproc Cluster Properties</source>
  </property>
</configuration>
EOF

# Install google cloud storage connector
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O /hadoop/share/hadoop/common/lib/gcs-connector-latest-hadoop2.jar
mkdir /hadoop_gcs_connector_metadata_cache

## Start HDFS daemons
# Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
/hadoop/bin/hdfs namenode -format
# Start the namenode daemon
/hadoop/sbin/hadoop-daemon.sh start namenode
# Start the datanode daemon
/hadoop/sbin/hadoop-daemon.sh start datanode

## Start YARN daemons
# Start the resourcemanager daemon
/hadoop/sbin/yarn-daemon.sh start resourcemanager
# Start the nodemanager daemon
/hadoop/sbin/yarn-daemon.sh start nodemanager

# Install Hive
wget http://mirrors.sonic.net/apache/hive/hive-2.3.3/apache-hive-2.3.3-bin.tar.gz
tar -zxf apache-hive-2.3.3-bin.tar.gz
mv apache-hive-2.3.3-bin /hive
export PATH=/hive/bin:$PATH
hadoop fs -mkdir /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

# Install postgres for our metastore
apt-get install -y postgresql
service postgresql start
sudo -u postgres psql <<EOF
CREATE USER hiveuser WITH PASSWORD 'mypassword';
CREATE DATABASE metastore;
EOF

# Configure hive
cat > /hive/conf/hive-site.xml <<EOF 
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>  
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost/metastore</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hiveuser</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mypassword</value>
  </property>

  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${PRESTO_MASTER}:9083</value>
    <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
  </property>
</configuration>
EOF
schematool -dbType postgres -initSchema

# Start hive metastore service
screen -S hive-metastore -d -m hive --service metastore

# Create tables
export SCHEMA=tpcds
export LOCATION=gs://fivetran-benchmark/tpcds/parquet

hive <<EOF
create database ${SCHEMA};

create external table ${SCHEMA}.call_center
(cc_call_center_sk bigint, cc_call_center_id string, cc_rec_start_date string, cc_rec_end_date string, cc_closed_date_sk bigint, cc_open_date_sk bigint, cc_name string, cc_class string, cc_employees int, cc_sq_ft int, cc_hours string, cc_manager string, cc_mkt_id int, cc_mkt_class string, cc_mkt_desc string, cc_market_manager string, cc_division int, cc_division_name string, cc_company int, cc_company_name string, cc_street_number string, cc_street_name string, cc_street_type string, cc_suite_number string, cc_city string, cc_county string, cc_state string, cc_zip string, cc_country string, cc_gmt_offset double, cc_tax_percentage double)
stored as parquet
location '${LOCATION}/call_center'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.catalog_page
(cp_catalog_page_sk bigint, cp_catalog_page_id string, cp_start_date_sk bigint, cp_end_date_sk bigint, cp_department string, cp_catalog_number int, cp_catalog_page_number int, cp_description string, cp_type string)
stored as parquet
location '${LOCATION}/catalog_page'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.catalog_returns
(cr_returned_date_sk bigint, cr_returned_time_sk bigint, cr_item_sk bigint, cr_refunded_customer_sk bigint, cr_refunded_cdemo_sk bigint, cr_refunded_hdemo_sk bigint, cr_refunded_addr_sk bigint, cr_returning_customer_sk bigint, cr_returning_cdemo_sk bigint, cr_returning_hdemo_sk bigint, cr_returning_addr_sk bigint, cr_call_center_sk bigint, cr_catalog_page_sk bigint, cr_ship_mode_sk bigint, cr_warehouse_sk bigint, cr_reason_sk bigint, cr_order_number bigint, cr_return_quantity int, cr_return_amount double, cr_return_tax double, cr_return_amt_inc_tax double, cr_fee double, cr_return_ship_cost double, cr_refunded_cash double, cr_reversed_charge double, cr_store_credit double, cr_net_loss double)
stored as parquet
location '${LOCATION}/catalog_returns'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.catalog_sales
(cs_sold_date_sk bigint, cs_sold_time_sk bigint, cs_ship_date_sk bigint, cs_bill_customer_sk bigint, cs_bill_cdemo_sk bigint, cs_bill_hdemo_sk bigint, cs_bill_addr_sk bigint, cs_ship_customer_sk bigint, cs_ship_cdemo_sk bigint, cs_ship_hdemo_sk bigint, cs_ship_addr_sk bigint, cs_call_center_sk bigint, cs_catalog_page_sk bigint, cs_ship_mode_sk bigint, cs_warehouse_sk bigint, cs_item_sk bigint, cs_promo_sk bigint, cs_order_number bigint, cs_quantity int, cs_wholesale_cost double, cs_list_price double, cs_sales_price double, cs_ext_discount_amt double, cs_ext_sales_price double, cs_ext_wholesale_cost double, cs_ext_list_price double, cs_ext_tax double, cs_coupon_amt double, cs_ext_ship_cost double, cs_net_paid double, cs_net_paid_inc_tax double, cs_net_paid_inc_ship double, cs_net_paid_inc_ship_tax double, cs_net_profit double)
stored as parquet
location '${LOCATION}/catalog_sales'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.customer_address
(ca_address_sk bigint, ca_address_id string, ca_street_number string, ca_street_name string, ca_street_type string, ca_suite_number string, ca_city string, ca_county string, ca_state string, ca_zip string, ca_country string, ca_gmt_offset double, ca_location_type string)
stored as parquet
location '${LOCATION}/customer_address'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.customer_demographics
(cd_demo_sk bigint, cd_gender string, cd_marital_status string, cd_education_status string, cd_purchase_estimate int, cd_credit_rating string, cd_dep_count int, cd_dep_employed_count int, cd_dep_college_count int )
stored as parquet
location '${LOCATION}/customer_demographics'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.customer
(c_customer_sk bigint, c_customer_id string, c_current_cdemo_sk bigint, c_current_hdemo_sk bigint, c_current_addr_sk bigint, c_first_shipto_date_sk bigint, c_first_sales_date_sk bigint, c_salutation string, c_first_name string, c_last_name string, c_preferred_cust_flag string, c_birth_day int, c_birth_month int, c_birth_year int, c_birth_country string, c_login string, c_email_address string, c_last_review_date string)
stored as parquet
location '${LOCATION}/customer'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.date_dim
(d_date_sk bigint, d_date_id string, d_date string, d_month_seq int, d_week_seq int, d_quarter_seq int, d_year int, d_dow int, d_moy int, d_dom int, d_qoy int, d_fy_year int, d_fy_quarter_seq int, d_fy_week_seq int, d_day_name string, d_quarter_name string, d_holiday string, d_weekend string, d_following_holiday string, d_first_dom int, d_last_dom int, d_same_day_ly int, d_same_day_lq int, d_current_day string, d_current_week string, d_current_month string, d_current_quarter string, d_current_year string )
stored as parquet
location '${LOCATION}/date_dim'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.household_demographics
(hd_demo_sk bigint, hd_income_band_sk bigint, hd_buy_potential string, hd_dep_count int, hd_vehicle_count int)
stored as parquet
location '${LOCATION}/household_demographics'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.income_band(ib_income_band_sk bigint, ib_lower_bound int, ib_upper_bound int)
stored as parquet
location '${LOCATION}/income_band'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.inventory
(inv_date_sk bigint, inv_item_sk bigint, inv_warehouse_sk bigint, inv_quantity_on_hand int)
stored as parquet
location '${LOCATION}/inventory'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.item
(i_item_sk bigint, i_item_id string, i_rec_start_date string, i_rec_end_date string, i_item_desc string, i_current_price double, i_wholesale_cost double, i_brand_id int, i_brand string, i_class_id int, i_class string, i_category_id int, i_category string, i_manufact_id int, i_manufact string, i_size string, i_formulation string, i_color string, i_units string, i_container string, i_manager_id int, i_product_name string)
stored as parquet
location '${LOCATION}/item'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.promotion
(p_promo_sk bigint, p_promo_id string, p_start_date_sk bigint, p_end_date_sk bigint, p_item_sk bigint, p_cost double, p_response_target int, p_promo_name string, p_channel_dmail string, p_channel_email string, p_channel_catalog string, p_channel_tv string, p_channel_radio string, p_channel_press string, p_channel_event string, p_channel_demo string, p_channel_details string, p_purpose string, p_discount_active string )
stored as parquet
location '${LOCATION}/promotion'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.reason(r_reason_sk bigint, r_reason_id string, r_reason_desc string )
stored as parquet
location '${LOCATION}/reason'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.ship_mode(sm_ship_mode_sk bigint, sm_ship_mode_id string, sm_type string, sm_code string, sm_carrier string, sm_contract string )
stored as parquet
location '${LOCATION}/ship_mode'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.store_returns
(sr_returned_date_sk bigint, sr_return_time_sk bigint, sr_item_sk bigint, sr_customer_sk bigint, sr_cdemo_sk bigint, sr_hdemo_sk bigint, sr_addr_sk bigint, sr_store_sk bigint, sr_reason_sk bigint, sr_ticket_number bigint, sr_return_quantity int, sr_return_amt double, sr_return_tax double, sr_return_amt_inc_tax double, sr_fee double, sr_return_ship_cost double, sr_refunded_cash double, sr_reversed_charge double, sr_store_credit double, sr_net_loss double )
stored as parquet
location '${LOCATION}/store_returns'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.store_sales
(ss_sold_date_sk bigint, ss_sold_time_sk bigint, ss_item_sk bigint, ss_customer_sk bigint, ss_cdemo_sk bigint, ss_hdemo_sk bigint, ss_addr_sk bigint, ss_store_sk bigint, ss_promo_sk bigint, ss_ticket_number bigint, ss_quantity int, ss_wholesale_cost double, ss_list_price double, ss_sales_price double, ss_ext_discount_amt double, ss_ext_sales_price double, ss_ext_wholesale_cost double, ss_ext_list_price double, ss_ext_tax double, ss_coupon_amt double, ss_net_paid double, ss_net_paid_inc_tax double, ss_net_profit double )
stored as parquet
location '${LOCATION}/store_sales'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.store
(s_store_sk bigint, s_store_id string, s_rec_start_date string, s_rec_end_date string, s_closed_date_sk bigint, s_store_name string, s_number_employees int, s_floor_space int, s_hours string, s_manager string, s_market_id int, s_geography_class string, s_market_desc string, s_market_manager string, s_division_id int, s_division_name string, s_company_id int, s_company_name string, s_street_number string, s_street_name string, s_street_type string, s_suite_number string, s_city string, s_county string, s_state string, s_zip string, s_country string, s_gmt_offset double, s_tax_precentage double )
stored as parquet
location '${LOCATION}/store'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.time_dim
(t_time_sk bigint, t_time_id string, t_time int, t_hour int, t_minute int, t_second int, t_am_pm string, t_shift string, t_sub_shift string, t_meal_time string)
stored as parquet
location '${LOCATION}/time_dim'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.warehouse(w_warehouse_sk bigint, w_warehouse_id string, w_warehouse_name string, w_warehouse_sq_ft int, w_street_number string, w_street_name string, w_street_type string, w_suite_number string, w_city string, w_county string, w_state string, w_zip string, w_country string, w_gmt_offset double )
stored as parquet
location '${LOCATION}/warehouse'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.web_page(wp_web_page_sk bigint, wp_web_page_id string, wp_rec_start_date string, wp_rec_end_date string, wp_creation_date_sk bigint, wp_access_date_sk bigint, wp_autogen_flag string, wp_customer_sk bigint, wp_url string, wp_type string, wp_char_count int, wp_link_count int, wp_image_count int, wp_max_ad_count int)
stored as parquet
location '${LOCATION}/web_page'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.web_returns
(wr_returned_date_sk bigint, wr_returned_time_sk bigint, wr_item_sk bigint, wr_refunded_customer_sk bigint, wr_refunded_cdemo_sk bigint, wr_refunded_hdemo_sk bigint, wr_refunded_addr_sk bigint, wr_returning_customer_sk bigint, wr_returning_cdemo_sk bigint, wr_returning_hdemo_sk bigint, wr_returning_addr_sk bigint, wr_web_page_sk bigint, wr_reason_sk bigint, wr_order_number bigint, wr_return_quantity int, wr_return_amt double, wr_return_tax double, wr_return_amt_inc_tax double, wr_fee double, wr_return_ship_cost double, wr_refunded_cash double, wr_reversed_charge double, wr_account_credit double, wr_net_loss double)
stored as parquet
location '${LOCATION}/web_returns'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.web_sales
(ws_sold_date_sk bigint, ws_sold_time_sk bigint, ws_ship_date_sk bigint, ws_item_sk bigint, ws_bill_customer_sk bigint, ws_bill_cdemo_sk bigint, ws_bill_hdemo_sk bigint, ws_bill_addr_sk bigint, ws_ship_customer_sk bigint, ws_ship_cdemo_sk bigint, ws_ship_hdemo_sk bigint, ws_ship_addr_sk bigint, ws_web_page_sk bigint, ws_web_site_sk bigint, ws_ship_mode_sk bigint, ws_warehouse_sk bigint, ws_promo_sk bigint, ws_order_number bigint, ws_quantity int, ws_wholesale_cost double, ws_list_price double, ws_sales_price double, ws_ext_discount_amt double, ws_ext_sales_price double, ws_ext_wholesale_cost double, ws_ext_list_price double, ws_ext_tax double, ws_coupon_amt double, ws_ext_ship_cost double, ws_net_paid double, ws_net_paid_inc_tax double, ws_net_paid_inc_ship double, ws_net_paid_inc_ship_tax double, ws_net_profit double)
stored as parquet
location '${LOCATION}/web_sales'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');

create external table ${SCHEMA}.web_site
(web_site_sk bigint, web_site_id string, web_rec_start_date string, web_rec_end_date string, web_name string, web_open_date_sk bigint, web_close_date_sk bigint, web_class string, web_manager string, web_mkt_id int, web_mkt_class string, web_mkt_desc string, web_market_manager string, web_company_id int, web_company_name string, web_street_number string, web_street_name string, web_street_type string, web_suite_number string, web_city string, web_county string, web_state string, web_zip string, web_country string, web_gmt_offset double, web_tax_percentage double)
stored as parquet
location '${LOCATION}/web_site'
tblproperties ('parquet.compression' = 'snappy', 'parquet.block.size' = '2000000000');
EOF

# Download and unpack Presto server
wget https://s3.us-east-2.amazonaws.com/starburstdata/presto/starburst/195e/0.195-e.0.5/presto-server-0.195-e.0.5.tar.gz
tar -zxf presto-server-0.195-e.0.5.tar.gz
mv presto-server-0.195-e.0.5 presto

# Install cli
if [[ "${ROLE}" == 'Master' ]]; then
	wget https://s3.us-east-2.amazonaws.com/starburstdata/presto/starburst/195e/0.195-e.0.5/presto-cli-0.195-e.0.5-executable.jar -O /usr/bin/presto
	chmod a+x /usr/bin/presto
	apt-get install unzip
fi

# Copy GCS connector
# TODO fiddle with caching options in https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/src/main/java/com/google/cloud/hadoop/fs/gcs/GoogleHadoopFileSystemBase.java
wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -O presto/plugin/hive-hadoop2/gcs-connector-latest-hadoop2.jar 

# Configure Presto
mkdir -p presto/etc/catalog

cat > presto/etc/node.properties <<EOF
node.environment=production
node.id=$(uuidgen)
node.data-dir=/var/presto/data
EOF

# Configure hive metastore
cat > presto/etc/catalog/hive.properties <<EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://${PRESTO_MASTER_FQDN}:9083
hive.parquet-optimized-reader.enabled=true
hive.parquet-predicate-pushdown.enabled=true
hive.non-managed-table-writes-enabled=true
EOF

# Configure JVM
cat > presto/etc/jvm.config <<EOF
-server
-Xmx${PRESTO_JVM_MB}m
-Xmn512m
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-Dhive.config.resources=/hadoop/etc/hadoop/core-site.xml
-Djava.library.path=/hadoop/lib/native/:/usr/lib/
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.ssl=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.port=10999 
-Dcom.sun.management.jmxremote.rmi.port=10999 
-Djava.rmi.server.hostname=127.0.0.1 
EOF

if [[ "${ROLE}" == 'Master' ]]; then
	# Configure master properties
	cat > presto/etc/config.properties <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery-server.enabled=true
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASKS_PER_INSTANCE_PER_QUERY}
EOF
else
	cat > presto/etc/config.properties <<EOF
coordinator=false
http-server.http.port=${HTTP_PORT}
query.max-memory=999TB
query.max-memory-per-node=${PRESTO_QUERY_NODE_MB}MB
resources.reserved-system-memory=${PRESTO_RESERVED_SYSTEM_MB}MB
discovery.uri=http://${PRESTO_MASTER_FQDN}:${HTTP_PORT}
query.max-history=1000
task.concurrency=${TASKS_PER_INSTANCE_PER_QUERY}
EOF
fi

cat > presto/etc/catalog/memory.properties <<EOF
connector.name=memory
memory.max-data-per-node=10GB
EOF

# Start presto
presto/bin/launcher start

# Tos how debug info for hive, use hive -hiveconf hive.root.logger=DEBUG,console