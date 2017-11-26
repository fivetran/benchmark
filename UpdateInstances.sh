CLUSTER=tpcds
INSTANCES="${CLUSTER}-m ${CLUSTER}-w-0 ${CLUSTER}-w-1 ${CLUSTER}-w-2 ${CLUSTER}-w-3 ${CLUSTER}-w-4 ${CLUSTER}-w-5 ${CLUSTER}-w-6 ${CLUSTER}-w-7"

# Stop all instances
gcloud compute instances stop $INSTANCES

# Update to Intel Skylake
for i in $INSTANCES; do 
    echo "Alter ${i}"
    gcloud beta compute instances update $i --min-cpu-platform "Intel Skylake"
done

# Start all instances
gcloud compute instances start $INSTANCES

# Restart presto
for i in $INSTANCES; do 
    echo "Start ${i}"
    gcloud compute ssh $i --command "sudo /presto-server-0.185/bin/launcher start"
done