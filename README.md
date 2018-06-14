# Results
https://blog.fivetran.com/warehouse-benchmark-dce9f4c529c1

# Design
This is based on the TPC-DS benchmark, a standard data warehouse benchmark that includes a lot of complicated queries.
There are two data configurations:

|  **Data size as uncompressed CSV** | **Largest fact table** |
|  ------ | ------ |
|  100 GB | 300 million rows |
|  1 TB | 3 billion rows |

There are two configurations for each warehouse:

|   |  | **Type** | **Cost / Hour** |
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

* 100gb of data in uncompressed CSV form
* 100s of millions of rows in the larger fact tables
* Complex queries
* A small data warehouse
* Redshift has automatic compression on, but no sort or dist keys
* Presto is storing data in Parquet format on GCS

# Usage
These scripts are intended to be manually copy-pasted into various terminals.
You can skip steps 1-4 since gs://fivetran-benchmark and s3://fivetran-benchmark are already populated.