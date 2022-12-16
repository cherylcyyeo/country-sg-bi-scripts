WITH vendor_list AS (
  SELECT
    vendor_code,
    chain_name,
    name
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`
  WHERE global_entity_id = 'FP_SG'
    AND is_active = TRUE
    AND is_test = FALSE
    AND LOWER(name) NOT LIKE '%islandwide%'
    AND LOWER(name) NOT LIKE '%restaurant delivery%'
    AND LOWER(name) NOT LIKE '%vendor delivery%'
    AND LOWER(name) NOT LIKE '%pandago%'
    AND LOWER(name) NOT LIKE '%pandamerchant%'
    AND vertical = 'Restaurant'
),

lg_zones AS (
  SELECT DISTINCT
    vendor_code,
    lg_zones.lg_zone_name AS zone_name, -- note that one vendor can be tagged to multiple zones
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_vendors_agg_lg_zones`
  LEFT JOIN UNNEST(lg_zones) lg_zones
  WHERE global_entity_id = 'FP_SG'
  -- AND is_closest_point = TRUE
),

unavailabilities AS (
  SELECT
    vendor_code,
    report_date,
    IFNULL(total_open_seconds, 0) AS total_open_seconds,
    IFNULL(total_unavailable_seconds, 0) AS total_unavailable_seconds,
  FROM `fulfillment-dwh-production.pandata_report.pandora_vendor_offline`
  LEFT JOIN UNNEST(availabilities) AS avail
  WHERE global_entity_id = 'FP_SG'
    AND report_date >= '2022-01-01'
    AND total_open_seconds <> 0
  GROUP BY 1,2,3,4
),

summary AS (
  SELECT
    v.vendor_code,
    v.chain_name,
    name,
    report_date,
    ROUND((total_open_seconds / 3600), 2) AS total_open_hour,
    ROUND((total_unavailable_seconds / 3600), 2) AS total_unavailable_hour,
  FROM vendor_list v
  LEFT JOIN unavailabilities u ON v.vendor_code = u.vendor_code
  WHERE 1 = 1
)

SELECT
  summary.vendor_code,
  name AS vendor_name,
  report_date AS date_local,
  zone_name,
  SUM(summary.total_unavailable_hour) AS total_unavailable_hour,
  SUM(total_open_hour) AS total_open_hour,

FROM summary
LEFT JOIN lg_zones ON summary.vendor_code = lg_zones.vendor_code

WHERE 1 = 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
