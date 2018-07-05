# Fetch compile and runtimes from Redshift

DB=dev
USER=tpcds_user

psql -A -F ';' --host ${HOST} --port 5439 --user ${USER} ${DB} > time.txt <<EOF
with compile_times as (
    select query, sum(datediff(milliseconds, starttime, endtime) / 1000.0) as compile_time 
    from svl_compile 
    group by 1
), run_times as (
    select xid, query, substring(querytxt, 1, 30) as querytxt, starttime, datediff(milliseconds, starttime, endtime) / 1000.0 as run_time
    from stl_query
), combined as (
    select xid, run_times.query, querytxt, starttime, compile_time, run_time 
    from run_times 
    left join compile_times on run_times.query = compile_times.query
), last_q as (
    select 
        xid, query, starttime, compile_time, run_time,
        last_value(querytxt) over (partition by xid order by starttime asc rows between current row and unbounded following) as querytxt
    from combined 
), agg_stages as (
    select xid, querytxt, min(starttime) as starttime, sum(compile_time) as compile_time, sum(run_time) as run_time
    from last_q 
    group by 1, 2
)
select * from agg_stages
order by starttime;
EOF