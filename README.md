# Results
https://fivetran.com/blog/warehouse-benchmark

# Overview
The 2022 benchmark revisits the 2020 Fivetran benchmark with some new additions to respond to the ever-evolving data stack. Namely, we are expanding the benchmark to include the use of dbt. dbt “is a data transformation tool that enables data analysts and engineers to transform, test and document data in the cloud data warehouse.” By benchmarking results run through dbt, we are able to more accurately reflect the performance likely to be seen by the majority of users.

There are many variables present in a benchmark: What kind of data was used? How much? How were the queries run? We seek to standardize as many of these variables as possible, providing a consistent and repeatable way to emulate the behavior of typical workloads within each warehouse. In doing so, Brooklyn Data Co. has run this year's benchmark through the console of each warehouse, but more importantly, has also used dbt to orchestrate and run the queries used. 

It is important to note that the only criteria evaluated in this benchmarking exercise are query time and cost. These are not and should not be the sole criteria when evaluating a warehouse. 


# Configuration
Our goal was to set up each warehouse so that there was general comparability across each warehouse from a computational perspective. While it’s impossible to do an exact comparison of warehouses due to the design choices of each, our goal was to select each warehouse to be as closely comparable to the others as possible from a compute standpoint. We chose large warehouse configurations to run each benchmark workload on: 

| Warehouse | Configuration       | Cost / Hour | Pricing Strcuture | Region
|-----------|---------------------|-------------|-------------------|------------
| Redshift  | 5x ra3.4xlarge      | $16.30      | On-demand         | US-East-1
| Snowflake | Large               | $16.00      | Standard Tier     | US-East-1
| Databricks| m5d.24xlarge        | $15.14      | Pay as you go     | US-East-1C
| BigQuery  | Flat-rate 500 slots | $16.44      | Flat rate         | US (Compute & Storage)
| Azure Synapse | DW25000c        | $30.00      | On-demand         | East US


# Methodology

## What data was used? 
We generated the TPC-DS data set at 1TB scale and used the same generated data in every warehouse. These data were loaded into each warehouse at the beginning of the exercise (we did not use pre-baked datasets, such as the dataset Snowflake offers when creating a new account). TPC-DS has 24 tables in a snowflake schema; the tables represent web, catalog, and store sales of an imaginary retailer. The largest fact table had roughly 3 billion rows. Below is the row and column count per table used in the benchmark: 

| Table Name           | Table Rows         | Table Column Count
|----------------------|--------------------|-------------------
|call_center           |42                  | 31
|catalog_page          |30,000              |9
|catalog_returns       |143,996,756         |27
|catalog_sales         |1,439,980,416       |34
|customer              |12,000,000          |18
|customer_address      |6,000,000           |13
|customer_demographics |1,920,800           |9
|date_dim              |73,049              |28
|household_demographics|7,200               |5
|income_band           |20                  |3
|inventory             |783,000,00          |4
|item                  |300,000             |22
|promotion             |1,500               |19
|reason                |65                  |3
|ship_mode             |20                  |6
|store                 |1,002               |29
|store_returns         |287,999,764         |20
|store_sales           |2,879,987,999       |23
|time_dim              |86,400              |10
|warehouse             |20                  |14
|web_page              |3,000               |14
|web_returns           |71,997,52           |24
|web_sales             |720,000,37          |34
|web_site              |54                  |26



## What queries were run? 
We ran 99 TPC-DS queries in September-March of 2021-2022. These queries are complex: They have lots of joins, aggregations, and subqueries. To prevent the warehouse from caching previous results, we ran each query only once. 

## How did we run the queries? 
We ran all queries through the native console of each warehouse in the benchmark. For Synapse, we used Azure Data Studio as the console to run queries through. We believe this behavior emulates that of a typical data user. The time for each query was tracked and the geometric mean was calculated across all 99 results in order to arrive at the average time per query in seconds. 

In 2022, we added dbt to the benchmark and used dbt-reported times for both running and orchestrating queries and materializing tables. This decision was made for three reasons. First, by using dbt, we are including an increasingly popular data stack tool that allows warehouse users to have a better understanding of the warehouse performance. Secondly, we used dbt to orchestrate queries, in addition to just running them in the console of each warehouse. Finally, by using dbt, we could more easily open source this benchmarking exercise in the form of a dbt project. 

When running queries through dbt, we used a single thread in order to replicate the original run method of the TPC-DS.

## How did we tune the warehouses? 
These data warehouses each offer advanced features like sort keys, clustering keys and date partitioning. We chose not to use any of these features in this benchmark. We did apply column compression encodings in Redshift and column store indexing for Synapse; Snowflake, Databricks, and BigQuery apply compression automatically.

We recognize that in many cases, warehouses are tuned and configured for specific use cases. While we encourage this practice, not all warehouses have the same configuration options available. For the sake of consistency and transparency, we opted to run each warehouse on its default settings out of the box, ensuring that they were comparable to the other warehouses in the benchmark.

