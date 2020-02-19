select substr(query_text, 0, 50), start_time, compilation_time, execution_time, total_elapsed_time 
from table(information_schema.query_history_by_warehouse(result_limit => 200))
order by start_time