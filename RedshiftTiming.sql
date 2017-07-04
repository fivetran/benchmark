select substring(querytxt, 1, 30), starttime, datediff(milliseconds, starttime, endtime) 
from stl_query 
where userid = 100 
order by starttime;