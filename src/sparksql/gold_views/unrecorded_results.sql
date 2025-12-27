select
 r.event_key 
 ,c.comp_name
 ,c.comp_year
 ,c.start_date
 ,e.short_desc
 ,e.long_desc
from results_silver r
inner join competitions_silver c
on r.comp_id = c.comp_id
and size(r.drawTournResults) = 0
inner join events_silver e
on r.event_key = e.event_key
