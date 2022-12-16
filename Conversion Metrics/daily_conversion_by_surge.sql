WITH all_zones AS (
  SELECT
    zones.name as zone_name,
    zones.id,
    zones.lg_zone_uuid as lg_zone_uuid,

  FROM `fulfillment-dwh-production.pandata_curated.lg_countries` c,
    UNNEST(c.cities) city,
    UNNEST(city.zones) zones

  WHERE global_entity_id = 'FP_SG'
    AND zones.is_active = TRUE
    AND LOWER(zones.name) NOT LIKE '%dmart%'
    AND zones.has_default_delivery_area_settings = TRUE
),

zone_stats AS (
  SELECT DISTINCT
    lg.lg_zone_uuid,
    a.zone_name,
    CAST(ROUND(stats.mean_delay_in_minutes,2) AS FLOAT64) AS mean_delay,
    EXTRACT(HOUR FROM DATE_ADD(created_at_utc, INTERVAL 8 HOUR)) hour_,
    EXTRACT(MINUTE FROM DATE_ADD(created_at_utc, INTERVAL 8 HOUR)) minute_,
    EXTRACT(DATE FROM DATE_ADD(created_at_utc, INTERVAL 8 HOUR)) date_,
    TIMESTAMP(DATETIME_TRUNC(DATE_ADD(created_at_utc, INTERVAL 8 HOUR), MINUTE)) AS local_timestamp,

  FROM `fulfillment-dwh-production.pandata_curated.lg_zone_stats` lg, UNNEST(lg.stats) stats
  LEFT JOIN all_zones a

  ON lg.lg_zone_uuid = a.lg_zone_uuid
  WHERE lg.created_date_utc >= '2022-06-30'
    AND lg.global_entity_id = "FP_SG"
    AND zone_name IS NOT NULL
),

thresholds AS (
  SELECT
    zone_name,
    MIN(TIMESTAMP(local_timestamp)) min_dt,
    MAX(TIMESTAMP(local_timestamp)) max_dt
  FROM zone_stats
  GROUP BY 1
),

all_timestamps AS (
  SELECT
    zone_name,
    local_timestamp
  FROM thresholds,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(min_dt, max_dt, INTERVAL 1 MINUTE)) local_timestamp
),

all_stats AS (
  SELECT
    DATE(a.local_timestamp) AS created_date_local,
    a.zone_name,
    a.local_timestamp,
    FORMAT_TIME("%R", TIME(a.local_timestamp)) as time_local,
    EXTRACT(hour FROM a.local_timestamp) hour,
    EXTRACT(minute FROM a.local_timestamp) minute,
    LAST_VALUE(z.mean_delay IGNORE NULLS) OVER (PARTITION BY a.zone_name ORDER BY a.local_timestamp) mean_delay

  FROM all_timestamps a
  LEFT JOIN zone_stats z
  ON a.local_timestamp = TIMESTAMP(z.local_timestamp)
    AND a.zone_name = z.zone_name
),

mean_delay_stats AS (
  SELECT DISTINCT
    date_updated,
    zone_name,
    mean_delay_start,
    mean_delay_end,
    surge_event,
    surge_fee
  FROM `fulfillment-dwh-production.pandata_report.country_SG_surge_fee_mastersheet`
  WHERE zone_name IS NOT NULL
    AND date_updated >= '2022-07-01'
),

daily_min AS (
  SELECT DISTINCT
    date_updated,
    f1.zone_name,
    MIN(f1.surge_fee) AS min_fee
  FROM mean_delay_stats f1
  WHERE f1.zone_name IS NOT NULL
  GROUP BY 1, 2
),

min_mean_delay_stats AS (
  SELECT DISTINCT
    f1.date_updated,
    f1.zone_name,
    f2.surge_event,
    f1.min_fee
  FROM daily_min f1
  LEFT JOIN mean_delay_stats f2
  ON f1.zone_name = f2.zone_name
    AND f1.min_fee = f2.surge_fee
    AND f1.date_updated = f2.date_updated
),

daily_max AS (
  SELECT DISTINCT
    f1.date_updated,
    f1.zone_name,
    MAX(f1.mean_delay_end) AS max_mean_delay_end,
    MAX(f1.surge_fee) AS max_fee
  FROM mean_delay_stats f1
  WHERE f1.zone_name IS NOT NULL
  GROUP BY 1, 2
),

max_mean_delay_stats AS (
  SELECT DISTINCT
    f1.date_updated,
    f1.zone_name,
    f2.surge_event,
    f1.max_fee,
    f1.max_mean_delay_end
  FROM daily_max f1
  LEFT JOIN mean_delay_stats f2
  ON f1.zone_name = f2.zone_name
    AND f1.max_fee = f2.surge_fee
    AND f1.date_updated = f2.date_updated
),

combined_stats AS (
  SELECT DISTINCT
    a.created_date_local,
    a.zone_name,
    a.local_timestamp,
    a.time_local,
    a.hour,
    a.minute,
    a.mean_delay,
    CASE
      WHEN a.mean_delay < -1 THEN md.surge_event
      WHEN a.mean_delay > maxd.max_mean_delay_end THEN maxd.surge_event
      ELSE m.surge_event
    END AS surge_event
  FROM
  all_stats a
  LEFT JOIN mean_delay_stats m ON (a.zone_name = m.zone_name
    AND a.created_date_local = m.date_updated
    AND a.mean_delay BETWEEN m.mean_delay_start AND m.mean_delay_end)
  LEFT JOIN min_mean_delay_stats md ON (a.zone_name = md.zone_name
    AND a.created_date_local = md.date_updated)
  LEFT JOIN max_mean_delay_stats maxd ON (a.zone_name = maxd.zone_name
    AND a.created_date_local = maxd.date_updated)
),

pd_orders AS (
  SELECT
    code,
    CASE WHEN ((pd.delivery_fee_local = 0.93 AND (1-(d1.attributions_foodpanda_ratio/100) = 0
    OR 1-(d1.attributions_foodpanda_ratio/100) IS NULL)) OR (pd.delivery_fee_local = 0.99 AND (1-(d1.attributions_foodpanda_ratio/100) = 0
    OR 1-(d1.attributions_foodpanda_ratio/100) IS NULL))) THEN 1 ELSE 0
    END AS is_99c,
    CASE WHEN ((pd.delivery_fee_local = 0.93 AND (1-(d1.attributions_foodpanda_ratio/100) = 0
    OR 1-(d1.attributions_foodpanda_ratio/100) IS NULL)) OR (pd.delivery_fee_local = 0.99 AND (1-(d1.attributions_foodpanda_ratio/100) = 0
    OR 1-(d1.attributions_foodpanda_ratio/100) IS NULL)) OR (LOWER(d1.discount_type) LIKE '%free_delivery%') OR (LOWER(v1.type) LIKE '%free_delivery%')) THEN 1 ELSE 0
    END AS is_df_incentivised

  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` pd

  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_vouchers` v1
  ON pd.uuid = v1.uuid
    AND v1.global_entity_id = "FP_SG"
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_discounts` d1
  ON pd.uuid = d1.uuid
    AND d1.global_entity_id = "FP_SG"

  WHERE pd.global_entity_id = 'FP_SG'
    AND v1.global_entity_id = 'FP_SG'
    AND d1.global_entity_id = 'FP_SG'
    AND DATE(expected_delivery_at_local) BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND pd.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND v1.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND d1.created_date_utc BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
),

raw_sessions AS (
  SELECT
    EXTRACT(hour FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) hour_,
    EXTRACT(minute FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) minute_,
    EXTRACT(date FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) date_,
    sessions.ga_session_id,
    lg_zone_name,
    CASE WHEN is_transaction IS NULL THEN FALSE ELSE is_transaction END AS is_transaction,
    surge_event,
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_ga_sessions_agg_lg_city_zones` sessions
  INNER JOIN `fulfillment-dwh-production.pandata_report.regional_apac_ga_events_agg_business_types` vertical
  ON sessions.ga_session_id = vertical.ga_session_id

  LEFT JOIN combined_stats
  ON (DATE(DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = combined_stats.created_date_local
    AND EXTRACT(hour FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = combined_stats.hour
    AND EXTRACT(minute FROM DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) = combined_stats.minute
    AND lg_zone_name = combined_stats.zone_name)
  WHERE sessions.global_entity_id = 'FP_SG'
    AND vertical.global_entity_id = 'FP_SG'
    AND expedition_type = 'delivery'
    AND DATE(DATE_ADD(session_start_timestamp_utc, INTERVAL 8 hour)) BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND sessions.partition_date BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND vertical.partition_date BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    AND has_restaurants_impression = TRUE
),

sessions AS (
  SELECT
    date_,
    hour_,
    lg_zone_name,
    surge_event,
    COUNT(DISTINCT CASE WHEN is_transaction=TRUE THEN ga_session_id END) AS total_transaction,
    COUNT(DISTINCT ga_session_id) AS total_session,
  FROM raw_sessions
  WHERE lg_zone_name IS NOT NULL
  GROUP BY date_, hour_, lg_zone_name, surge_event
),

orders AS (
  SELECT
    date_local,
    EXTRACT(hour FROM expected_delivery_at_local) AS hour_,
    zone_name,
    IFNULL(surge_event, 'no_surge') AS surge_event,
    COUNT(DISTINCT gpo.order_code) AS total_orders,
    SUM(gpo_excl_wastage) AS total_gp,
    SUM(gpo_excl_wastage - IFNULL(ad_rev_per_order, 0) - IFNULL(sub_rev_per_order, 0)) AS total_gp_wo_ad,
    -- SUM(IFNULL(ROUND(o.rider.actual_delivery_time_in_seconds/60,2), 0)) AS total_dt,
  FROM `fulfillment-dwh-production.pandata_report.country_SG_rs_gross_profit_per_order` gpo
  -- LEFT JOIN `fulfillment-dwh-production.pandata_curated.lg_orders` AS o
  -- ON o.order_code = gpo.order_code
  WHERE vendor_business_type = 'restaurants'
    AND date_local BETWEEN DATE_SUB(CURRENT_DATE - 1, INTERVAL 2 MONTH) AND (CURRENT_DATE - 1)
    -- AND o.global_entity_id = 'FP_SG'
    -- AND o.created_date_utc >= '2022-10-30'
    AND gpo.order_code NOT IN (SELECT DISTINCT code FROM pd_orders WHERE is_df_incentivised = 1)
  GROUP BY date_local, hour_, zone_name, surge_event
)

SELECT
  sessions.*,
  orders.total_orders,
  orders.total_gp,
  orders.total_gp_wo_ad,
  -- orders.total_dt,
FROM sessions
LEFT JOIN orders
ON (sessions.date_ = orders.date_local
  AND sessions.hour_ = orders.hour_
  AND sessions.lg_zone_name = orders.zone_name
  AND sessions.surge_event = orders.surge_event)
ORDER BY 1,2,3,4
