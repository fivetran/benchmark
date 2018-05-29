# Generate test data on the large cluster
# Run this script on the cluster you created in step 1
# gcloud compute ssh tpcds-m
# You will need to install AWS credentials on this machine using ~/.aws/credentials so that we can copy to S3 for Redshift and Snowflake

# Hive TPC-DS benchmarking
wget https://github.com/hortonworks/hive-testbench/archive/hive14.zip
unzip hive14.zip
cd hive-testbench-hive14/

# Download TPC-DS generator
./tpcds-build.sh

# Generate 100gb of TPC-DS data
./tpcds-setup.sh 1000