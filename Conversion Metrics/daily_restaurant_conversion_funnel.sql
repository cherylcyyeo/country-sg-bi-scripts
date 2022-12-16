WITH ga_events_wo_trans AS (
  SELECT *
  FROM `fulfillment-dwh-production.pandata_report.country_SG_ga_events`
  WHERE event_action != 'transaction'
    AND date_local >= '2022-01-01'
),

ga_events_w_trans AS (
  SELECT * EXCEPT(dedup)
  FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_code ORDER BY CASE
      WHEN traffic_source = 'direct' THEN 1
      WHEN traffic_source = 'search engine' THEN 2 END ASC NULLS LAST) AS dedup,
    FROM `fulfillment-dwh-production.pandata_report.country_SG_ga_events`
    WHERE event_action = 'transaction'
      AND date_local >= '2022-01-01'
  )
  WHERE dedup = 1
),

ga_events AS (
  SELECT ga_events_wo_trans.*
  FROM ga_events_wo_trans
  UNION ALL
  SELECT ga_events_w_trans.*
  FROM ga_events_w_trans
),

pd_orders AS (
  SELECT
  code,
  o.vendor_code,
  CASE
    WHEN business_type_apac IN ('restaurants', 'caterers', 'kitchens') THEN 'restaurants'
    WHEN business_type_apac = 'shops' THEN 'shops'
    WHEN business_type_apac = 'dmart' THEN 'dmart'
  END AS vertical_type
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_vendors_agg_business_types` vertical
  ON o.vendor_code = vertical.vendor_code
  WHERE o.global_entity_id = 'FP_SG'
    AND vertical.global_entity_id = 'FP_SG'
    AND created_date_utc >= '2021-12-31'
    AND ordered_at_date_local >= '2022-01-01'
    AND is_valid_order = TRUE
    AND is_test_order = FALSE
    AND is_gross_order = TRUE
    AND code IS NOT NULL
),

resto_sessions AS (
  SELECT
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    COUNT(DISTINCT ga_session_id) AS cvr_denominator, --cvr
    COUNT(DISTINCT CASE WHEN event_action = 'transaction' AND order_code IN (SELECT DISTINCT code FROM pd_orders WHERE vertical_type = 'restaurants') THEN ga_session_id END) AS cvr_numerator, --cvr
    COUNT(DISTINCT CASE WHEN event_action = 'transaction' AND order_code IN (SELECT DISTINCT code FROM pd_orders WHERE vertical_type = 'restaurants') THEN order_code END) AS mocvr_numerator
  FROM ga_events
  WHERE ((event_action IN ("shop_list.loaded", "shop_list.updated", "shop_details.loaded", "cart.loaded", "checkout.loaded", "transaction") AND vendor_type IN ('restaurants', 'street_food', 'caterer', 'web.city page'))
  OR (event_action = 'navigation.clicked' AND screen_name IN ("my_order", "myorders", "myorderscreen", "myorderdetails", "pastordersdetails", "pastordersdetailsscreen")))
    AND date_local IS NOT NULL
    AND date_local >= '2022-01-01'
  GROUP BY date_local, hour_, platform, zone_name, traffic_source, channel_grouping
),

resto_funnel AS (
  SELECT
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    ga_session_id,
    COUNT(DISTINCT IF(event_action = "home_screen.loaded", ga_session_id, NULL)) AS homescreen_visited,
    COUNT(DISTINCT IF(event_action IN ("shop_list.loaded", "shop_list.updated"), ga_session_id, NULL)) AS list_visited,
    COUNT(DISTINCT IF(event_action = "shop_details.loaded", ga_session_id, NULL)) AS details_visited,
    COUNT(DISTINCT IF(event_action = "checkout.loaded", ga_session_id, NULL)) AS checkout_visited,
    COUNT(DISTINCT IF(event_action = "transaction", ga_session_id, NULL)) AS transaction_done,
  FROM ga_events
  WHERE vendor_type IN ('restaurants', 'caterers', 'street_food', 'homescreen', 'web.city page')
    AND date_local IS NOT NULL
    AND date_local >= '2022-01-01'
  GROUP BY 1,2,3,4,5,6,7
),

resto_conversion AS (
  SELECT
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    COUNT(DISTINCT IF(list_visited > 0, ga_session_id, NULL)) AS mcvr2_denominator, --mcvr2
    COUNT(DISTINCT IF(list_visited > 0 AND details_visited > 0, ga_session_id, NULL)) AS mcvr2_numerator, --mcvr2
    COUNT(DISTINCT IF(details_visited > 0, ga_session_id, NULL)) AS mcvr3_denominator, --mcvr3
    COUNT(DISTINCT IF(details_visited > 0 AND checkout_visited > 0, ga_session_id, NULL)) AS mcvr3_numerator, --mcvr3
    COUNT(DISTINCT IF(checkout_visited > 0, ga_session_id, NULL)) AS mcvr4_denominator, --mcvr4
    COUNT(DISTINCT IF(checkout_visited > 0 AND transaction_done > 0, ga_session_id, NULL)) AS mcvr4_numerator --mcvr4
  FROM resto_funnel
  GROUP BY 1,2,3,4,5,6
),

combined_resto AS (
  SELECT
    'restaurants' AS vertical,
    resto_conversion.*,
    cvr_denominator,
    cvr_numerator,
    mocvr_numerator,
  FROM resto_conversion
  FULL JOIN resto_sessions
    ON (resto_conversion.date_local = resto_sessions.date_local
    AND resto_conversion.hour_ = resto_sessions.hour_
    AND resto_conversion.platform = resto_sessions.platform
    AND resto_conversion.zone_name = resto_sessions.zone_name
    AND resto_conversion.traffic_source = resto_sessions.traffic_source
    AND resto_conversion.channel_grouping = resto_sessions.channel_grouping
  )
),

platform_transactions AS (
  SELECT
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    COUNT(DISTINCT order_code) AS total_orders,
  FROM ga_events
  WHERE event_action = 'transaction'
    AND order_code IN (SELECT DISTINCT code FROM pd_orders)
    AND order_code IS NOT NULL
  GROUP BY 1,2,3,4,5,6
),

platform_funnel AS (
  SELECT
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    ga_session_id,
    COUNT(DISTINCT IF(event_action = "home_screen.loaded", ga_session_id, NULL)) AS homescreen_visited,
    COUNT(DISTINCT IF(event_action IN ("shop_list.loaded", "shop_list.updated"), ga_session_id, NULL)) AS list_visited,
    COUNT(DISTINCT IF(event_action = "shop_details.loaded", ga_session_id, NULL)) AS details_visited,
    COUNT(DISTINCT IF(event_action = "checkout.loaded", ga_session_id, NULL)) AS checkout_visited,
    COUNT(DISTINCT IF(event_action = "transaction", ga_session_id, NULL)) AS transaction_done,
  FROM ga_events
  WHERE date_local IS NOT NULL
    AND date_local >= '2022-01-01'
  GROUP BY 1,2,3,4,5,6,7
),

platform_conversion_inter AS (
  SELECT
    'platform' AS vertical,
    date_local,
    hour_,
    platform,
    zone_name,
    traffic_source,
    channel_grouping,
    COUNT(DISTINCT IF(list_visited > 0, ga_session_id, NULL)) AS mcvr2_denominator, --mcvr2
    COUNT(DISTINCT IF(list_visited > 0 AND details_visited > 0, ga_session_id, NULL)) AS mcvr2_numerator, --mcvr2
    COUNT(DISTINCT IF(details_visited > 0, ga_session_id, NULL)) AS mcvr3_denominator, --mcvr3
    COUNT(DISTINCT IF(details_visited > 0 AND checkout_visited > 0, ga_session_id, NULL)) AS mcvr3_numerator, --mcvr3
    COUNT(DISTINCT IF(checkout_visited > 0, ga_session_id, NULL)) AS mcvr4_denominator, --mcvr4
    COUNT(DISTINCT IF(checkout_visited > 0 AND transaction_done > 0, ga_session_id, NULL)) AS mcvr4_numerator, --mcvr4
    COUNT(DISTINCT ga_session_id) AS cvr_denominator,
    COUNT(DISTINCT IF(transaction_done > 0, ga_session_id, NULL)) AS cvr_numerator,
  FROM platform_funnel
  GROUP BY 1,2,3,4,5,6,7
),

platform_conversion AS (
  SELECT 
    platform_conversion_inter.*,
    total_orders AS mocvr_numerator,
  FROM platform_conversion_inter
  LEFT JOIN platform_transactions
    ON (platform_conversion_inter.date_local = platform_transactions.date_local
    AND platform_conversion_inter.hour_ = platform_transactions.hour_
    AND platform_conversion_inter.platform = platform_transactions.platform
    AND platform_conversion_inter.zone_name = platform_transactions.zone_name
    AND platform_conversion_inter.traffic_source = platform_transactions.traffic_source
    AND platform_conversion_inter.channel_grouping = platform_transactions.channel_grouping)
)

SELECT platform_conversion.*
FROM platform_conversion

UNION ALL

SELECT combined_resto.*
FROM combined_resto

ORDER BY vertical, date_local, hour_, platform, zone_name, traffic_source, channel_grouping
