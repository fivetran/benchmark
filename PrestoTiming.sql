select substring(regexp_split(query, '\n')[1], 1, 20) as q, date_diff('millisecond', started, "end")/1000.0 as execution, date_diff('millisecond', created, "end")/1000.0 as total
from system.runtime.queries 
where state = 'FINISHED' 
and source = 'presto-cli'
order by "end";