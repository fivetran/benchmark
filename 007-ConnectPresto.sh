# Copy benchmarking files to Presto master and connect
zip -r presto.zip \
      Warmup.sql \
      PrestoTiming.sql \
      100-PopulatePresto.sh \
      101-BenchmarkPresto.sh \
      102-PrestoTiming.sql \
      query
gcloud compute --project "digital-arbor-400" scp presto.zip tpcds-presto-m:~ 
gcloud compute --project "digital-arbor-400" ssh tpcds-presto-m 