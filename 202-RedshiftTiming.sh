# Fetch compile and runtimes from Redshift

export HOST=tpcds-benchmark.cw43lptekopo.us-east-1.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=NumeroFoo0
export USER=tpcds_user

psql -A -F ";" --host ${HOST} --port 5439 --user ${USER} ${DB} > RedshiftResults.csv <<EOF
with compile_times as (select query, sum(endtime-starttime) as compile_time from svl_compile group by 1)
select
    stl_query.xid,
    max(substring(text,1,60)) as query_text,
    sum(compile_time) as compile_time,
    sum(endtime-starttime) as run_time
from stl_query, stl_querytext, compile_times
where stl_query.query = stl_querytext.query
and stl_query.query = compile_times.query
group by stl_query.xid
order by max(stl_query.starttime);
EOF