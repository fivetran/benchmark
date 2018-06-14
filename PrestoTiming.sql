select substring(regexp_split(query, '\n')[1], 1, 20) as q, date_diff('millisecond', started, "end") as execution, date_diff('millisecond', created, "end") as total
from system.runtime.queries 
where state = 'FINISHED' 
and source = 'presto-cli'
order by "end";