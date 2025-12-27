/* Searches the last 10 years and organises the fencers who won the most
 * tournaments, by category i.e. Women's Foil (WF), Men's Sabre (MS), Men's Epee
 * (ME)....etc. in the open division
 * Also recorded are how many tournaments they attended and when the first/last
 * wins were.
 */
 SELECT
  results_by_event.name,
  e.short_desc AS event,
  COUNT(DISTINCT c.comp_id) AS tourns_attended,
  COUNT(DISTINCT CASE WHEN results_by_event.place = '1' THEN c.comp_id END) AS tourns_won,
  MIN(CASE WHEN results_by_event.place = '1' THEN c.start_date END) AS first_win_date,
  MAX(CASE WHEN results_by_event.place = '1' THEN c.start_date END) AS last_win_date
FROM results_silver r
CROSS JOIN LATERAL inline(drawTournResults) AS results_by_event
INNER JOIN competitions_silver c ON r.comp_id = c.comp_id
AND c.comp_year >= '2015'
AND c.category = 'open'
INNER JOIN events_silver e ON r.event_key = e.event_key
GROUP BY results_by_event.name, e.short_desc
HAVING tourns_won > 0
order by tourns_won desc
