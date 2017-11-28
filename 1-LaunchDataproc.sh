# Creates a large dataproc cluster so we can generate test data fast
# Run this script on your local machine with gsutil set up
set -e

# Copy Presto/Hive setup script to gs
gsutil cp ./Presto.sh gs://fivetran-benchmark/Presto.sh

# Launch a large dataproc cluster for generating data
gcloud dataproc clusters \
      create tpcds \
      --zone us-central1-a \
      --master-machine-type n1-standard-4 \
      --master-boot-disk-size 100 \
      --num-workers 10 \
      --worker-machine-type n1-standard-4 \
      --worker-boot-disk-size 100 \
      --num-preemptible-workers 40 \
      --preemptible-worker-boot-disk-size=100 \
      --scopes 'https://www.googleapis.com/auth/cloud-platform' \
      --project digital-arbor-400 \
      --initialization-actions gs://fivetran-benchmark/Presto.sh
