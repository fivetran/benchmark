select 
    substring(query_text, 0, 20), 
    start_time, 
    date_part(epoch_millisecond, end_time) - date_part(epoch_millisecond, start_time) as elapsed
from table(information_schema.query_history_by_session(session_id => current_session()::int))
where execution_status = 'SUCCESS'
order by start_time;