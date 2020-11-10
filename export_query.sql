SELECT
  rental_id,
  duration,
  bike_id,
  end_date,
  end_station_id,
  end_station_name,
  start_date,
  start_station_id,
  start_station_name,
  CONCAT(DATE(CURRENT_TIMESTAMP),"/", start_station_id, FORMAT("_%03d", CAST(ROW_NUMBER() OVER(PARTITION BY start_station_id ORDER BY start_date)/15000 AS INT64))) AS file_name
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`