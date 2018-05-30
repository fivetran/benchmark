# Launch a single-node hive cluster that will act as a metastore for Presto
set -e

gcloud dataproc clusters create tpcds-hive \
    --subnet default \
    --zone us-central1-f \
    --single-node \
    --master-machine-type n1-standard-1 \
    --master-boot-disk-size 10 \
    --project digital-arbor-400

gcloud compute scp ParquetDdl.sql tpcds-hive-m:~ \
    --zone us-central1-f

gcloud compute ssh tpcds-hive-m \
    --zone us-central1-f \
    --command "hive -f ParquetDdl.sql"