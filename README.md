# Results
https://fivetran.com/blog/warehouse-benchmark

# Design
This is based on the TPC-DS benchmark, a standard data warehouse benchmark that uses lots of joins, aggregations and subqueries.
The TPC-DS queries have been modified somewhat to improve portability across implementations, and eliminate the use of obscure SQL features like grouping-sets.
We generated 1 TB of data, which contains about 4 billion rows in the largest fact table.
We used the following warehouse configurations:

|           | Configuration       | Cost / Hour |
|-----------|---------------------|-------------|
| Redshift  | 5x ra3.4xlarge      | $16.30      |
| Snowflake | Large               | $16.00      |
| Presto    | 4x n2-highmem-32    | $8.02       |
| BigQuery  | Flat-rate 500 slots | $13.70      |

# Usage
These scripts are intended to be manually copy-pasted into various terminals.
You can skip steps 1-4 since gs://fivetran-benchmark and s3://fivetran-benchmark are already populated.
