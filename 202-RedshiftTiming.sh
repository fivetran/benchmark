# Fetch compile and runtimes from Redshift

export HOST=tpcds-benchmark.cw43lptekopo.us-east-1.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=NumeroFoo0
export USER=tpcds_user

psql -A -F ";" --host ${HOST} --port 5439 --user ${USER} ${DB} > RedshiftResults.csv <<EOF
with compile_segments as (select xid, query, starttime, endtime from svl_compile),
exec_segments as (select xid, query, starttime, endtime from stl_query),
all_segments as (select * from compile_segments union all select * from exec_segments),
query_text as (select distinct xid, last_value(querytxt) over (partition by xid order by query rows between unbounded preceding and unbounded following) as text from stl_query)
select all_segments.xid, max(substring(query_text.text, 1, 60)) as text, max(endtime)-min(starttime)
from all_segments, query_text 
where all_segments.xid = query_text.xid
group by all_segments.xid 
order by all_segments.xid;
EOF