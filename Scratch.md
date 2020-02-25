
```sh
export HOST=tpcds-benchmark.cw43lptekopo.us-east-1.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=NumeroFoo0
export USER=tpcds_user

psql --host ${HOST} --port 5439 --user ${USER} ${DB}
```


dev=> \d+ stl_query
                                                   Table "pg_catalog.stl_query"
           Column           |            Type             | Collation | Nullable | Default | Storage  | Stats target | Description 
----------------------------+-----------------------------+-----------+----------+---------+----------+--------------+-------------
 userid                     | integer                     |           | not null |         | plain    |              | 
 query                      | integer                     |           | not null |         | plain    |              | 
 label                      | character(30)               |           | not null |         | extended |              | 
 xid                        | bigint                      |           | not null |         | plain    |              | 
 pid                        | integer                     |           | not null |         | plain    |              | 
 database                   | character(32)               |           | not null |         | extended |              | 
 querytxt                   | character(4000)             |           | not null |         | extended |              | 
 starttime                  | timestamp without time zone |           | not null |         | plain    |              | 
 endtime                    | timestamp without time zone |           | not null |         | plain    |              | 
 aborted                    | integer                     |           | not null |         | plain    |              | 
 insert_pristine            | integer                     |           | not null |         | plain    |              | 
 concurrency_scaling_status | integer                     |           | not null |         | plain    |              | 
Replica Identity: ???

dev=> \d+ svl_compile
                                  View "pg_catalog.svl_compile"
  Column   |            Type             | Collation | Nullable | Default | Storage | Description 
-----------+-----------------------------+-----------+----------+---------+---------+-------------
 userid    | integer                     |           |          |         | plain   | 
 xid       | bigint                      |           |          |         | plain   | 
 pid       | integer                     |           |          |         | plain   | 
 query     | integer                     |           |          |         | plain   | 
 segment   | integer                     |           |          |         | plain   | 
 locus     | integer                     |           |          |         | plain   | 
 starttime | timestamp without time zone |           |          |         | plain   | 
 endtime   | timestamp without time zone |           |          |         | plain   | 
 compile   | integer                     |           |          |         | plain   | 
View definition:
 SELECT stl_compile_info.userid, stl_compile_info.xid, stl_compile_info.pid, stl_compile_info.query, stl_compile_info.segment, stl_compile_info.locus, stl_compile_info.starttime, stl_compile_info.endtime, stl_compile_info.compile
   FROM stl_compile_info;


dev=> \d+ stl_querytext
                                  Table "pg_catalog.stl_querytext"
  Column  |      Type      | Collation | Nullable | Default | Storage  | Stats target | Description 
----------+----------------+-----------+----------+---------+----------+--------------+-------------
 userid   | integer        |           | not null |         | plain    |              | 
 xid      | bigint         |           | not null |         | plain    |              | 
 pid      | integer        |           | not null |         | plain    |              | 
 query    | integer        |           | not null |         | plain    |              | 
 sequence | integer        |           | not null |         | plain    |              | 
 text     | character(200) |           | not null |         | extended |              | 
Replica Identity: ???

```sql
select substring(querytxt, 1, 60), query, starttime, endtime, endtime-starttime, max(endtime) over () - min(starttime) over () from stl_query where xid = 21006 order by query;

select query, segment, compile, starttime, endtime, endtime-starttime, max(endtime) over () - min(starttime) over () from svl_compile where xid = 21006 order by query;

select
    stl_query.xid,
    substring(text,1,60) as query_text,
    svl_compile.starttime as compile_start_time,
    svl_compile.endtime as compile_end_time,
    stl_query.endtime,
    stl_query.starttime
    )-min() as run_time
from stl_query, stl_querytext, svl_compile
where stl_query.query = stl_querytext.query
and stl_query.query = svl_compile.query
group by stl_query.xid
order by min(stl_query.starttime);

with compile_segments as (select xid, query, starttime, endtime from svl_compile),
exec_segments as (select xid, query, starttime, endtime from stl_query),
all_segments as (select * from compile_segments union all select * from exec_segments),
query_text as (select distinct xid, last_value(querytxt) over (partition by xid order by query rows between unbounded preceding and unbounded following) as text from stl_query)
select all_segments.xid, max(substring(query_text.text, 1, 60)) as text, max(endtime)-min(starttime)
from all_segments, query_text 
where all_segments.xid = query_text.xid
group by all_segments.xid 
order by all_segments.xid;

select xid, query, substring(querytxt, 1, 60), starttime, endtime from stl_query
union all select xid, null, substring(text, 1, 60), starttime, endtime from stl_ddltext
union all select xid, null, substring(text, 1, 60), starttime, endtime from stl_utilitytext
order by xid desc limit 100;

with check_superuser as (select * from stl_analyze limit 1),
recent as (
  select * from svl_qlog 
  where userid > 1 and starttime > current_timestamp - interval '1 day'),
counter as (
  select starttime as t, 1 as n from recent 
  union all select endtime as t, -1 as n from recent),
concurrency as (
  select t, sum(n) over (order by t rows unbounded preceding) as n, extract(second from lead(t, 1) over (order by t) - t) as duration 
  from counter)
select 100 - round(sum(case when n > 0 then duration else 0 end) / sum(duration) * 100, 1) as idle_pct 
from concurrency;

```