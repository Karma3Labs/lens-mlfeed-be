select * 
from publication_stats as stats 
inner join profile_post as post 
	on (post.post_id = stats.publication_id)
where stats.total_amount_of_collects > 0
limit 10;


select 
	(collect_dtls.currency / 1e18) as amt,  
  case 
  	when (collect_dtls.currency = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270') 
    	then 'MATIC'
    else 'OTHER'
  end as currency_symbol,
  collect_dtls.*
from publication_collect_module_details as collect_dtls 
inner join profile_post as post 
	on (post.post_id = collect_dtls.publication_id)
where amount is not null
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