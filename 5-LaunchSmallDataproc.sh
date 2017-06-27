# Creates a small dataproc cluster for benchmarking
# Be sure to delete the cluster created in 1-LaunchDataproc.sh before running this
set -e

# Copy Presto/Hive setup script to gs
gsutil cp ./Presto.sh gs://fivetran-benchmark/Presto.sh

# Launch a large dataproc cluster for generating data
gcloud dataproc clusters \
      create tpcds \
      --zone us-central1-a \
      --master-machine-type n1-standard-4 \
      --master-boot-disk-size 100 \
      --num-workers 4 \
      --worker-machine-type n1-standard-4 \
      --worker-boot-disk-size 100 \
      --scopes 'https://www.googleapis.com/auth/cloud-platform' \
      --project digital-arbor-400 \
      --initialization-actions gs://fivetran-benchmark/Presto.sh

gcloud compute scp --recurse . tpcds-m:~/benchmark

gcloud compute ssh tpcds-m

# Create external tables references the data in gs://fivetran-benchmark that we generated with the big cluster
hive -f ParquetDdl.sql