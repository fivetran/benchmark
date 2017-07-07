# Generate TPCS data in gs://fivetran-benchmark

echo 'Generate CSVs...'
hive -f CsvDdl.sql
hive -f PopulateCsv.sql

echo 'Generate Parquet files...'
hive -f ParquetDdl.sql
hive -f PopulateParquet.sql
