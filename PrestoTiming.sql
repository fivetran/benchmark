select substring(regexp_split(query, '\n')[1], 1, 20) as q, started, date_diff('millisecond', started, "end") as elapsed 
from system.runtime.queries 
where state = 'FINISHED' 
and source = 'presto-cli'
order by "end";