# Creates a small dataproc cluster for benchmarking
# Be sure to delete the cluster created in 1-LaunchDataproc.sh before running this
set -e

# Create master instance
gcloud compute \
      --project "digital-arbor-400" \
      instances create "tpcds-presto-m" \
      --zone "us-central1-f" \
      --machine-type "n1-standard-8" \
      --min-cpu-platform=Intel\ Skylake \
      --image-project "ubuntu-os-cloud" \
      --image-family "ubuntu-1710" \
      --metadata "PrestoRole=Master,PrestoMaster=tpcds-presto-m" \
      --metadata-from-file "startup-script=Presto.sh" \
      --boot-disk-size "10" \
      --local-ssd interface=nvme

# Create worker instance group
gcloud compute instance-templates delete "presto-worker" \
      --quiet \
      || echo "...ignoring error"
gcloud compute \
      --project "digital-arbor-400" \
      instance-templates create "presto-worker" \
      --machine-type "n1-standard-8" \
      --min-cpu-platform=Intel\ Skylake \
      --image-project "ubuntu-os-cloud" \
      --image-family "ubuntu-1710" \
      --metadata "PrestoRole=Worker,PrestoMaster=tpcds-presto-m" \
      --metadata-from-file "startup-script=Presto.sh" \
      --boot-disk-size "10" \
      --local-ssd interface=nvme
gcloud compute \
      --project "digital-arbor-400" \
      instance-groups managed create "tpcds-presto-w" \
      --zone "us-central1-f" \
      --base-instance-name "tpcds-presto-w" \
      --template "presto-worker" \
      --size "31"

# # Create preemptible worker instance group
# gcloud compute instance-templates delete "presto-preemptible-worker" \
#       --quiet \
#       || echo "...ignoring error"
# gcloud compute \
#       --project "digital-arbor-400" \
#       instance-templates create "presto-preemptible-worker" \
#       --machine-type "n1-standard-16" \
#       --image-project "ubuntu-os-cloud" \
#       --image-family "ubuntu-1710" \
#       --preemptible \
#       --metadata "PrestoRole=Worker,PrestoMaster=tpcds-presto-m" \
#       --metadata-from-file "startup-script=Presto.sh" \
#       --boot-disk-size "10" 
# gcloud compute \
#       --project "digital-arbor-400" \
#       instance-groups managed create "tpcds-presto-p" \
#       --zone "us-central1-f" \
#       --base-instance-name "tpcds-presto-p" \
#       --template "presto-preemptible-worker" \
#       --size "32"