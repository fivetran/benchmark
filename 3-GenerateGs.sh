# Generate TPCS data in gs://fivetran-benchmark
# zip -r scripts.zip \
#       3-GenerateGs.sh \
#       CsvDdl.sql \
#       PopulateCsv.sql \
#       ParquetDdl.sql \
#       PopulateParquet.sql
# gcloud compute scp scripts.zip tpcds-m:~
# gcloud compute ssh tpcds-m
# unzip scripts.zip

echo 'Generate CSVs...'
hive -f CsvDdl.sql
hive -f PopulateCsv.sql

echo 'Generate Parquet files...'
hive -f ParquetDdl.sql
hive -f PopulateParquet.sql
