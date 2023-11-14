WITH geo_performance as (
	SELECT * FROM {{source('google_ads__custom','geo_performance')}}
	),
	geo_target as (
	SELECT * FROM {{source('google_ads__custom','geo_target')}}
	),	 
     result AS (
       WITH gp AS ( -- ADD geo_performance
        SELECT *, 
          cast(REGEXP_REPLACE(geo_target_state, r".*/", "") as int64) as geo_target_id,
          cost_micros/1000000 as cost
        FROM `geo_performance`
        )
      SELECT * FROM gp
      left join `geo_target` as gt  -- ADD geo_target
      ON gp.geo_target_id = gt.id
    )
    
   SELECT
      date,
      campaign_name,
      country_code,
      name as state,
      SUM(clicks) as clicks,
      SUM(cost) as cost,
      SUM(impressions) as impressions,
      FROM result
      GROUP BY 1,2,3,4

