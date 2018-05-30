# Copy benchmarking files to Presto master and connect
CLUSTER=tpcds-presto 
MASTER=${CLUSTER}-m 

zip -r query.zip \
      Warmup.sql \
      PrestoTiming.sql \
      RaptorDdl.sql \
      100-BenchmarkPresto.sh \
      query
gcloud compute scp query.zip ${MASTER}:~ \
      --zone "us-central1-f"
gcloud compute ssh ${MASTER} \
      --zone "us-central1-f"