# Creates a small dataproc cluster for benchmarking
# Be sure to delete the cluster created in 1-LaunchDataproc.sh before running this
set -e

CLUSTER=tpcds-presto 
MASTER=${CLUSTER}-m 
WORKER_GROUP=${CLUSTER}-w

# Create master instance
gcloud compute \
      --project "digital-arbor-400" \
      instances create "tpcds-presto-m" \
      --zone "us-central1-f" \
      --machine-type "n1-standard-4" \
      --image-project "ubuntu-os-cloud" \
      --image-family "ubuntu-1710" \
      --metadata "PrestoRole=Master,PrestoMaster=${MASTER}" \
      --metadata-from-file "startup-script=Presto.sh" \
      --boot-disk-size "10" 

# Create worker instance group
gcloud compute instance-templates delete "presto-worker" \
      --quiet
gcloud compute \
      --project "digital-arbor-400" \
      instance-templates create "presto-worker" \
      --machine-type "n1-standard-4" \
      --image-project "ubuntu-os-cloud" \
      --image-family "ubuntu-1710" \
      --metadata "PrestoRole=Worker,PrestoMaster=tpcds-presto-m" \
      --metadata-from-file "startup-script=Presto.sh" \
      --boot-disk-size "10" 
gcloud compute \
      --project "digital-arbor-400" \
      instance-groups managed create "tpcds-presto-w" \
      --zone "us-central1-f" \
      --base-instance-name "tpcds-presto-w" \
      --template "presto-worker" \
      --size "8"