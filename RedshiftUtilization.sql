-- Compute average utilization so we can compare with BigQuery
with acc as (
    select starttime as t, 1 as increment 
    from stl_query 
    where endtime is not null 
    union all select endtime as t, -1 as increment 
    from stl_query 
    where endtime is not null 
),
counts as (
    select 
        t, 
        sum(increment) over (order by t rows unbounded preceding) as running, 
        datediff(millisecond, t, lead(t, 1) over (order by t)) as elapsed
    from acc
),
active as (
    select 
        sum(case when running > 0 then elapsed else 0 end) as running,
        sum(case when running > 0 then 0 else elapsed end) as quiet
    from counts
)
select running::double precision / quiet::double precision as utilization
from active;