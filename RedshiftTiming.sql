select substring(querytxt, 0, 20), starttime, datediff(milliseconds, starttime, endtime) 
from stl_query 
where userid = 100 
order by starttime;