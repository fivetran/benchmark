# Creates a small dataproc cluster for benchmarking
# Be sure to delete the cluster created in 1-LaunchDataproc.sh before running this
set -e

CLUSTER=tpcds-50mb

# Copy Presto/Hive setup script to gs
gsutil cp ./Presto.sh gs://fivetran-benchmark/Presto.sh

# Launch a large dataproc cluster for generating data
gcloud dataproc clusters \
      create ${CLUSTER} \
      --zone us-central1-a \
      --master-machine-type n1-standard-4 \
      --master-boot-disk-size 100 \
      --num-workers 8 \
      --worker-machine-type n1-standard-4 \
      --worker-boot-disk-size 100 \
      --scopes 'https://www.googleapis.com/auth/cloud-platform' \
      --project digital-arbor-400 \
      --initialization-actions gs://fivetran-benchmark/Presto.sh

zip -r query.zip ParquetDdl.sql Warmup.sql PrestoTiming.sql 10-BenchmarkPresto.sh query
gcloud compute scp query.zip ${CLUSTER}-m:~

gcloud compute ssh ${CLUSTER}-m