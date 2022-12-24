# Fetch compile and runtimes from Redshift

export HOST=benchmark-05x.cnr9x5mk2fpk.us-east-2.redshift.amazonaws.com
export DB=dev
export PGPASSWORD=AtAtDocks1
export USER=tpcds_user

psql -A -F ";" --host ${HOST} --port 5439 --user ${USER} ${DB} > results/RedshiftResults.csv <<EOF
select xid, substring(text, 1, 60) as txt, compile_start, compile_end, exec_start, exec_end
from (
    select xid, min(starttime) as compile_start, max(endtime) as compile_end
    from svl_compile
    group by 1
)
join (
    select xid, min(starttime) as exec_start, max(endtime) as exec_end
    from stl_query
    group by 1
) using (xid)
join (
    select distinct xid, last_value(querytxt) over (partition by xid order by query rows between unbounded preceding and unbounded following) as text 
    from stl_query
) using (xid)
order by 1
EOF


sed -i '' -e 's/;;/;/g' results/RedshiftResults.csv