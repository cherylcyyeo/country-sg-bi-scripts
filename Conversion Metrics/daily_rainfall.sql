SELECT
  zone AS zone_name,
  DATE(timestamp) AS date_local,
  EXTRACT(HOUR FROM timestamp) AS hour_,
  SUM(rainfall_mm) AS total_rainfall
FROM `logistics-sg-cloud-4435.logs_sg.sg_rain`
GROUP BY zone, date_local, hour_
