select * 
from publication_stats as stats 
inner join profile_post as post 
	on (post.post_id = stats.publication_id)
where stats.total_amount_of_collects > 0
limit 10;




WITH MAX_DATE AS (
  SELECT max(date) as maxdate
  FROM globaltrust
	WHERE strategy_id = 5
)
SELECT date, ROW_NUMBER() OVER(ORDER BY v desc), g.v, p.handle  
FROM globaltrust AS g
	INNER JOIN profiles AS p ON p.id = g.i
WHERE 
	strategy_id = 5 
  AND date = (SELECT maxdate FROM MAX_DATE)
LIMIT 1000