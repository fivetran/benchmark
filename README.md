# Results
https://blog.fivetran.com/warehouse-benchmark-dce9f4c529c1

# Design
This is based on the TPC-DS benchmark, a standard data warehouse benchmark that uses lots of joins, aggregations and subqueries.
The TPC-DS queries have been modified somewhat to improve portability across implementations, and eliminate the use of obscure SQL features like grouping-sets.
There are two data configurations:

|  **Data size as uncompressed CSV** | **Largest fact table** |
|  ------ | ------ |
|  100 GB | 300 million rows |
|  1 TB | 3 billion rows |

There are two configurations for each warehouse:

| **Data size** | **Warehouse** | **Nodes** | **Cost / Hour** |
|  ------ | ------ | ------ | ------ |
|  **100 GB** | **Redshift** | 8 × dc2.large | $2.00 |
|   | **Snowflake** | X-Small | $2.00 |
|   | **Presto** | 4 × n1-standard-8 | $1.23 |
|   | **Azure** | ? | ? |
|   | **BigQuery** | - | - |
|  **1 TB** | **Redshift** | 4 × dc2.8xlarge | $19.20 |
|   | **Snowflake** | Large | $16.00 |
|   | **Presto** | ? | ? |
|   | **Azure** | ? | ? |
|   | **BigQuery** | - | - |

# Usage
These scripts are intended to be manually copy-pasted into various terminals.
You can skip steps 1-4 since gs://fivetran-benchmark and s3://fivetran-benchmark are already populated.