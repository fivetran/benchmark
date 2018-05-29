# Copy CSV files to s3 for Redshift and Snowflake
# You will need ~/.aws/credentials for this to work
# You should run this on a gcloud instance for speed
gsutil -m cp -r gs://fivetran-benchmark/tpcds_1000/csv s3://fivetran-benchmark/tpcds_1000/csv

