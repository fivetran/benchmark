# Generate TPCS data in gs://fivetran-benchmark

hive -f CsvDdl.sql
hive -f PopulateCsv.sql

hive -f ParquetDdl.sql
hive -f PopulateParquet.sql