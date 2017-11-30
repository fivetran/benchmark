# Copy benchmarking files to Presto master and connect
zip -r query.zip \
      ParquetDdl.sql \
      Warmup.sql \
      PrestoTiming.sql \
      10-BenchmarkPresto.sh \
      query
gcloud compute scp query.zip ${MASTER}:~ \
      --zone "us-central1-f"
gcloud compute ssh ${MASTER} \
      --zone "us-central1-f"