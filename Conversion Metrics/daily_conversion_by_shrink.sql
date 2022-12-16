WITH all_zones AS (
  SELECT
    zones.name as zone_name,
    zones.id,
    zones.lg_zone_uuid as lg_zone_uuid,
  FROM
  `fulfillment-dwh-production.pandata_curated.lg_countries` c,
    UNNEST(c.cities) city,
    UNNEST(city.zones) zones
  WHERE global_entity_id = 'FP_SG'
    AND zones.is_active = TRUE
    AND LOWER(zones.name) NOT LIKE '%dmart%'
    AND zones.has_default_delivery_area_settings = TRUE
),

das_events as (
  SELECT
    uuid,
    events.lg_zone_uuid,
    zone_name,
    activation_threshold,
    deactivation_threshold,
    title,
    start_at_local,
    end_at_local
  FROM `fulfillment-dwh-production.pandata_curated.lg_delivery_areas_events` events
  LEFT JOIN all_zones ON events.lg_zone_uuid = all_zones.lg_zone_uuid
  WHERE created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND global_entity_id = "FP_SG"
    AND activation_type = "automatic"
    AND is_active
    AND is_shape_in_sync
),

shrink_in_mins AS (
  SELECT
    das_events.*,
    datetime_
  FROM das_events
  LEFT JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(CAST(DATE_TRUNC(DATETIME_ADD(start_at_local, INTERVAL 1 MINUTE), MINUTE) AS timestamp), CAST(DATE_TRUNC(DATETIME_SUB(end_at_local, INTERVAL 1 MINUTE), MINUTE) AS timestamp), INTERVAL 1 MINUTE)) AS datetime_
  WHERE (LOWER(title) LIKE '%shrink%'OR LOWER(title) LIKE '%close%')
),

pd_orders AS (
  SELECT
    code,
    CASE
      WHEN ((pd.delivery_fee_local = 0.93 AND (1 -(d1.attributions_foodpanda_ratio / 100) = 0 
      OR 1 -(d1.attributions_foodpanda_ratio / 100) IS NULL)) 
      OR (pd.delivery_fee_local = 0.99 AND (1 -(d1.attributions_foodpanda_ratio / 100) = 0 OR 1 -(d1.attributions_foodpanda_ratio / 100) IS NULL))) THEN 1
      ELSE 0
    END AS is_99c,
    CASE 
      WHEN ((pd.delivery_fee_local = 0.93 AND (1 -(d1.attributions_foodpanda_ratio / 100) = 0 OR 1 -(d1.attributions_foodpanda_ratio / 100) IS NULL)) 
      OR (pd.delivery_fee_local = 0.99 AND (1 -(d1.attributions_foodpanda_ratio / 100) = 0 OR 1 -(d1.attributions_foodpanda_ratio / 100) IS NULL)) 
      OR (LOWER(d1.discount_type) LIKE '%free_delivery%') 
      OR (LOWER(v1.type) LIKE '%free_delivery%')) THEN 1
      ELSE 0
    END AS is_df_incentivised
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` pd
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_vouchers` v1 ON pd.uuid = v1.uuid
    AND v1.global_entity_id = "FP_SG"
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_discounts` d1 ON pd.uuid = d1.uuid
    AND d1.global_entity_id = "FP_SG"
  WHERE pd.global_entity_id = 'FP_SG'
    AND v1.global_entity_id = 'FP_SG'
    AND d1.global_entity_id = 'FP_SG'
    AND DATE(expected_delivery_at_local) BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND pd.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND v1.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND d1.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
  ),

  raw_sessions AS (
    SELECT
      EXTRACT(HOUR FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 HOUR)) hour_,
      EXTRACT(minute FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) minute_,
      EXTRACT(date FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) date_,
      sessions.ga_session_id,
      lg_zone_name,
      CASE
        WHEN is_transaction IS NULL THEN FALSE
        ELSE is_transaction
      END AS is_transaction,
      CASE
        WHEN LOWER(title) LIKE '%shrink_0%' THEN 'shrink_0'
        WHEN LOWER(title) LIKE '%shrink_1%' THEN 'shrink_1'
        WHEN LOWER(title) LIKE '%shrink_2%' THEN 'shrink_2'
        WHEN LOWER(title) LIKE '%shrink_3%' THEN 'shrink_3'
        WHEN LOWER(title) LIKE '%shrink_4%' THEN 'shrink_4'
        WHEN LOWER(title) LIKE '%close%' THEN 'close'
      ELSE 'no_shrink'
      END AS shrink_event,
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_ga_sessions_agg_lg_city_zones` sessions
  INNER JOIN `fulfillment-dwh-production.pandata_report.regional_apac_ga_events_agg_business_types` vertical ON sessions.ga_session_id = vertical.ga_session_id
  LEFT JOIN shrink_in_mins ON (DATE(DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = DATE(shrink_in_mins.datetime_)
    AND EXTRACT(hour FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = EXTRACT(HOUR FROM shrink_in_mins.datetime_)
    AND EXTRACT(minute FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = EXTRACT(MINUTE FROM shrink_in_mins.datetime_)
    AND sessions.lg_zone_uuid = shrink_in_mins.lg_zone_uuid)
  WHERE sessions.global_entity_id = 'FP_SG'
    AND vertical.global_entity_id = 'FP_SG'
    AND expedition_type = 'delivery'
    AND DATE(DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND sessions.partition_date BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND vertical.partition_date BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND has_restaurants_impression = TRUE
),

sessions AS (
  SELECT
    date_,
    hour_,
    lg_zone_name,
    shrink_event,
    COUNT(DISTINCT CASE WHEN is_transaction = TRUE THEN ga_session_id END) AS total_transaction,
    COUNT(DISTINCT ga_session_id) AS total_session,
  FROM raw_sessions
  WHERE lg_zone_name IS NOT NULL
  GROUP BY 1,2,3,4
),

orders AS (
  SELECT
    date_local,
    EXTRACT(hour FROM expected_delivery_at_local) AS hour_,
    gpo.zone_name,
    order_code,
    gpo_excl_wastage,
    ad_rev_per_order,
    sub_rev_per_order,
    -- IFNULL(ROUND(o.rider.actual_delivery_time_in_seconds/60,2), 0) AS actual_delivery_time_in_mins,
    CASE
      WHEN LOWER(title) LIKE '%shrink_0%' THEN 'shrink_0'
      WHEN LOWER(title) LIKE '%shrink_1%' THEN 'shrink_1'
      WHEN LOWER(title) LIKE '%shrink_2%' THEN 'shrink_2'
      WHEN LOWER(title) LIKE '%shrink_3%' THEN 'shrink_3'
      WHEN LOWER(title) LIKE '%shrink_4%' THEN 'shrink_4'
      WHEN LOWER(title) LIKE '%close%' THEN 'close'
      ELSE 'no_shrink'
    END AS shrink_event,
  FROM `fulfillment-dwh-production.pandata_report.country_SG_rs_gross_profit_per_order` gpo
  LEFT JOIN shrink_in_mins ON (DATE(expected_delivery_at_local) = DATE(shrink_in_mins.datetime_)
    AND EXTRACT(HOUR FROM expected_delivery_at_local) = EXTRACT(HOUR
  FROM shrink_in_mins.datetime_)
    AND EXTRACT(MINUTE FROM expected_delivery_at_local) = EXTRACT(MINUTE FROM shrink_in_mins.datetime_)
    AND gpo.zone_name = shrink_in_mins.zone_name)
  WHERE vendor_business_type = 'restaurants'
    AND date_local BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH)
    AND (CURRENT_DATE - 1)
    AND gpo.order_code NOT IN (SELECT DISTINCT code FROM pd_orders WHERE is_df_incentivised = 1)
),

orders_agg AS (
  SELECT
    date_local,
    hour_,
    zone_name,
    shrink_event,
    COUNT(DISTINCT orders.order_code) AS total_orders,
    SUM(IFNULL(gpo_excl_wastage, 0)) AS total_gp,
    SUM(gpo_excl_wastage - IFNULL(ad_rev_per_order, 0) - IFNULL(sub_rev_per_order, 0)) AS total_gp_wo_ad,
  FROM orders
  GROUP BY 1,2,3,4
)

SELECT
  sessions.*,
  CASE
    WHEN (sessions.shrink_event = 'close') THEN 0
    ELSE IFNULL(orders_agg.total_orders, 0)
  END AS total_orders,
  IFNULL(orders_agg.total_gp, 0) AS total_gp,
  IFNULL(orders_agg.total_gp_wo_ad, 0) AS total_gp_wo_ad,
FROM sessions
LEFT JOIN orders_agg ON (sessions.date_ = orders_agg.date_local
  AND sessions.hour_ = orders_agg.hour_
  AND sessions.lg_zone_name = orders_agg.zone_name
  AND sessions.shrink_event = orders_agg.shrink_event)
ORDER BY 1,2,3,4
