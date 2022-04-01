# Fivetran's TPCS Benchmark dbt Project

This dbt project has been created to run benchmarks against Snowflake, BigQuery, Redshift, Databricks and Azure Synapse.

There are two materializations which can be used: query and table. The query materialization is designed to emulate just-in-time query workloads such as BI tools and ad-hoc analysis. The table materialization emulates a batch data processing workload, as it writes the output of each query to a table.

## Setup

### Install dbt adapters

The tested dbt package versions to run each benchmark are:
```
dbt-databricks==1.0.1
dbt-redshift==1.0.0
dbt-bigquery==1.0.0
dbt-snowflake==1.0.0
dbt-synapse==1.0.2b1
```

We recommend using [dbtenv](https://github.com/brooklyn-data/dbtenv) to easily switch between adapter versions. Alternatively, a virtual environment could be created for each.

#### Synapse
For the Synapse dbt adapter to function, install the Microsoft SQL ODBC driver:

```
brew install msodbcsql17 mssql-tools
```

### Configure profiles.yml

See `example-profiles.yml` for an example dbt profile configuration for the project. See https://docs.getdbt.com/dbt-cli/configure-your-profile for more information on configuring your profile. Note that the thread count for each target is set to 1 in order to run the queries sequentially per the TPC-DS standard benchmark.

### Load the benchmark data into the warehouse

A TPC-DS dataset is hosted by Fivetran in a [public GCP bucket](https://console.cloud.google.com/storage/browser/fivetran-benchmark). Load this data into the warehouse(s) you are testing.

## Running the benchmarks

### Choosing the warehouse to run against

Specify the name of the target in `profiles.yml` for the desired warehouse using the `--target` dbt argument:

```
dbt run --target redshift # This would run against the target named 'redshift' in profiles.yml
dbt run --target bigquery # This would run against the target named 'bigquery' in profiles.yml
```

### Choosing query vs table materialization
The choice of query vs table materialization is made via a variable named 'run_mode':

```
dbt run --vars '{"run_mode": "query"}' # runs the benchmark using the query materialization
dbt run --vars '{"run_mode": "table"}' # runs the benchmark using the table materialization
```

### Specifying the TPC-DS dataset location


If the database and schema containg the TPC-DS dataset differs from the database and schema set in the `profiles.yml` entry for the warehouse you're testing, set the following variables when running `dbt run`:

```
dataset_database
dataset_schema
```

For example, if running on redshift with the query materialization, and the TPC-DS dataset is in a database named 'data' and schema named 'tpcds', you'd run:

```
dbt run --target redshift --vars '{"run_mode": "query", "dataset_database": "data", "dataset_schema": "tpcds"}'
```

## Cross database compatibility notes/caveats

The dbt models have been templated to be cross-database compatible.

## Azure Synapse

Model 67 did not complete in testing, so it is disabled for the Synapse adapter type.

Synapse does not allow `order by` clauses in `create table` DDL statements unless `TOP` or `FOR XML` is also specified. Consequently, the `order by` statements are templated out for the Synapse adapter type when running with the 'table' materialization.

The exact error message returned is:
```
The ORDER BY clause is not valid in views, inline functions, derived tables, sub-queries, and common table expressions, unless TOP or FOR XML is also specified.
```
