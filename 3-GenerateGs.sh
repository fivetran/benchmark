-- Generate TPCS data in hive
-- Run this script in Hive
hive -f ParquetDdl.sql
hive -f PopulateParquet.sql
hive -f PopulateCsv.sql