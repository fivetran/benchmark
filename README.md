# Results
https://docs.google.com/a/fivetran.com/spreadsheets/d/1maw-SNZTZdqOySSx_JAxqwurheuQZnUharSyBTB3XvU/edit?usp=sharing

# Design
This is based on the TPC-DS benchmark, a standard data warehouse benchmark that includes a lot of complicated queries.
It's configured to approximate a Fivetran user: 
* 100gb of data in uncompressed CSV form
* 100s of millions of rows in the larger fact tables
* Complex queries
* A small data warehouse
* Redshift has automatic compression on, but no sort or dist keys
* Presto is storing data in Parquet format on GCS

# Usage
These scripts are intended to be manually copy-pasted into various terminals.
You can skip steps 1-4 since gs://fivetran-benchmark and s3://fivetran-benchmark are already populated.