# Copy CSV files to s3 for Redshift and Snowflake
# You will need ~/.aws/credentials for this to work
# You should run this on a gcloud instance for speed
# rclone allows us to copy >5GB files using S3 multipart uploads, which gsutil does not support.
# https://rclone.org/
rclone copy -v gs://fivetran-benchmark/tpcds_1000_dat s3://fivetran-benchmark/tpcds_1000_dat

