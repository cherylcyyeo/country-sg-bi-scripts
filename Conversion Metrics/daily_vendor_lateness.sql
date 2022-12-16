WITH vendor_list as (
  SELECT
    vendor_code,
    chain_name,
    name
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`
  WHERE global_entity_id = 'FP_SG'
    AND is_active = true
    AND is_test = false
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

order_detail AS (
  SELECT DISTINCT
    lg_orders.vendor_code,
    name AS vendor_name,
    order_code,
    vendor_list.chain_name,
    DATE(rider.promised_delivery_at_local) date_,
    CASE
      WHEN rider.rider_late_in_seconds > 0
      AND rider.rider_late_in_seconds < 600
      AND rider.at_vendor_time_in_seconds > 300 THEN ROUND(rider.at_vendor_time_cleaned_in_seconds / 60, 1)
      WHEN rider.rider_late_in_seconds <= 0
      AND vendor.picked_up_at_local > rider.original_scheduled_pickup_at_local THEN ROUND(DATETIME_DIFF(vendor.picked_up_at_local, rider.original_scheduled_pickup_at_local, second) / 60, 1) ELSE NULL
    END AS vendor_lateness_in_min
  FROM `fulfillment-dwh-production.pandata_curated.lg_orders` AS lg_orders
  INNER JOIN vendor_list ON lg_orders.vendor_code = vendor_list.vendor_code
  WHERE global_entity_id = 'FP_SG'
    AND created_date_local >= '2022-01-01'
    AND created_date_utc < CURRENT_DATE()
    AND DATE(rider.promised_delivery_at_local) >= '2022-01-01'
)

SELECT
  order_detail.vendor_code,
  vendor_name,
  zone_name,
  date_ AS date_local,
  SUM(IFNULL(vendor_lateness_in_min, 0)) AS total_vendor_lateness,
  COUNT(DISTINCT order_code) AS total_order_count,
FROM order_detail
LEFT JOIN lg_zones ON lg_zones.vendor_code = order_detail.vendor_code
GROUP BY 1,2,3,4
