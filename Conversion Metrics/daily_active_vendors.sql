WITH lg_zones AS (
  SELECT DISTINCT
    vendor_code,
    lg_zones.lg_zone_name AS zone_name, -- note that one vendor can be tagged to multiple zones
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_vendors_agg_lg_zones`
  LEFT JOIN UNNEST(lg_zones) lg_zones
  WHERE global_entity_id = 'FP_SG'
  -- AND is_closest_point = TRUE
),

vendor_contracts AS (
  SELECT *
  FROM (
    SELECT
      pd_v.vendor_code,
      zone_name,
      MAX(sf_cont.start_date_local) AS start_date_local,
      -- If there's no end date, contract still ongoing
      MAX(IFNULL(sf_cont.end_date_local, '2200-01-01')) AS end_date_local,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` pd_v

    LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` sf_acc
    ON pd_v.vendor_code = sf_acc.vendor_code

    LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_contracts` sf_cont
    ON sf_acc.id = sf_cont.sf_account_id

    LEFT JOIN lg_zones
    ON pd_v.vendor_code = lg_zones.vendor_code

    WHERE pd_v.global_entity_id = 'FP_SG'
      AND sf_acc.global_entity_id = 'FP_SG'
      AND sf_cont.global_entity_id = 'FP_SG'
      AND is_test = FALSE
      AND LOWER(pd_v.name) NOT LIKE '%islandwide%'
      AND LOWER(pd_v.name) NOT LIKE '%restaurant delivery%'
      AND LOWER(pd_v.name) NOT LIKE '%vendor delivery%'
      AND LOWER(pd_v.name) NOT LIKE '%pandago%'
      AND LOWER(pd_v.name) NOT LIKE '%pandamerchant%'
      AND pd_v.vertical = 'Restaurant'
    GROUP BY vendor_code, zone_name
    ORDER BY vendor_code, zone_name
  )
  JOIN UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE() - 1, INTERVAL 1 DAY)) AS date_local
),

active_vendors AS (
  SELECT *,
    CASE WHEN date_local BETWEEN start_date_local AND end_date_local THEN TRUE ELSE FALSE END AS is_active
  FROM vendor_contracts
)

SELECT
  date_local,
  zone_name,
  COUNT(DISTINCT CASE WHEN is_active = TRUE THEN vendor_code END) AS active_vendors
FROM active_vendors
WHERE zone_name IS NOT NULL
GROUP BY date_local, zone_name
ORDER BY date_local, zone_name
