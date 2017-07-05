# Generate TPCS data in gs://fivetran-benchmark

hive -f CsvDdl.sql
hive -f PopulateCsv.sql

hive -f ParquetDdl.sql
presto --catalog hive --schema tpcds_csv -f PopulateParquet.sql
