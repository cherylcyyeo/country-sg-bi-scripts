WITH pd_orders AS (
  SELECT
    DATE(pd.expected_delivery_at_local) AS date_local,
    EXTRACT(HOUR FROM pd.expected_delivery_at_local) AS hour,
    pd.uuid,
    code,
    CASE
      WHEN (b.is_kitchens) THEN 'k&c'
      ELSE b.business_type_apac
    END AS vendor_business_type,
    minimum_delivery_value_local,
    total_value_local,
    CASE
      WHEN (b.business_type_apac <> 'pandamart')
      AND (b.is_kitchens != TRUE)
      AND (v1.type = 'free_delivery') THEN ABS((v1.attributions_foodpanda_ratio / 100) * v1.value_local)
      ELSE 0
    END AS df_voucher_off,
    CASE
      WHEN (b.business_type_apac <> 'pandamart')
      AND (b.is_kitchens != TRUE)
      AND (d1.discount_type = 'free_delivery') THEN ABS((d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local)
    ELSE 0
    END AS df_discount_off,
    CASE
      WHEN ((1 - d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (1 - v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0
      OR (v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0) THEN 1
      ELSE 0
    END is_incentived,
    CASE
      WHEN delivery_fee_original_local = 0.99 OR delivery_fee_original_local = 0.93 THEN 1
      ELSE 0
    END is_99cents,
    CASE
      WHEN ((1 - d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (1 - v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0
      OR (v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0
      OR (delivery_fee_original_local = 0.99 OR delivery_fee_original_local = 0.93)) THEN 1
      ELSE 0
    END is_incentived_or_99cents,
    CASE
      WHEN (((1 - d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local > 0
      OR (1 - v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0
      OR (v1.attributions_foodpanda_ratio / 100) * v1.value_local > 0)
      AND (delivery_fee_original_local = 0.99 OR delivery_fee_original_local = 0.93)) THEN 1
      ELSE 0
    END is_incentived_and_99cents,
    (1 - d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local AS vendor_subsidized_discount,
    (d1.attributions_foodpanda_ratio / 100) * d1.discount_amount_local AS foodpanda_subsidized_discount,
    (1 - v1.attributions_foodpanda_ratio / 100) * v1.value_local AS vendor_subsidized_voucher,
    (v1.attributions_foodpanda_ratio / 100) * v1.value_local AS foodpanda_subsidized_voucher,
    delivery_fee_original_local,
    delivery_fee_vat_rate,
    (pd.delivery_fee_original_local /(pd.delivery_fee_vat_rate / 100 + 1)) AS df_wo_gst,
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_vendors_agg_business_types` b ON b.vendor_code = pd.vendor_code
    AND b.global_entity_id = pd.global_entity_id
    AND b.global_entity_id = "FP_SG"
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_vouchers` v1 ON pd.uuid = v1.uuid
    AND v1.global_entity_id = "FP_SG"
  LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_discounts` d1 ON pd.uuid = d1.uuid
    AND d1.global_entity_id = "FP_SG"
  WHERE pd.global_entity_id = "FP_SG"
    AND DATE(expected_delivery_at_local) >= '2022-06-01'
    AND pd.created_date_utc < CURRENT_DATE()
    AND v1.created_date_utc < CURRENT_DATE()
    AND d1.created_date_utc < CURRENT_DATE()
    AND pd.is_test_order = FALSE
    AND pd.is_valid_order = TRUE
    AND pd.expedition_type = 'delivery'
    AND b.business_type_apac <> 'dmart'
    AND minimum_delivery_value_local != 25 -- remove islandwide
),

pd_order_final AS (
  SELECT
    pd_orders.*,
    CASE
    WHEN ROUND(df_wo_gst - df_voucher_off - df_discount_off, 2) < 0 THEN 0
    ELSE ROUND(df_wo_gst - df_voucher_off - df_discount_off, 2)
  END AS final_df_wo_gst,
  FROM pd_orders
)

SELECT
  zone_name,
  pd_order_final.date_local,
  pd_order_final.hour,
  COUNT(DISTINCT order_code) AS number_of_orders,
  SUM(is_incentived) AS number_of_incentive_orders,
  SUM(is_99cents) AS number_of_99cents_orders,
  SUM(is_incentived_or_99cents) AS number_of_incentived_or_99cents,
  SUM(is_incentived_and_99cents) AS number_of_incentived_and_99cents,
  ROUND(SUM(gpo_excl_wastage), 2) AS total_GP,
  ROUND(SUM(total_value_local), 2) AS total_order_value,
  ROUND(SUM(gmv_local), 2) AS gmv_local,
  SUM(vendor_subsidized_discount) AS vendor_subsidized_discount,
  SUM(foodpanda_subsidized_discount) AS foodpanda_subsidized_discount,
  SUM(vendor_subsidized_voucher) AS vendor_subsidized_voucher,
  SUM(foodpanda_subsidized_voucher) AS foodpanda_subsidized_voucher,
  ROUND(SAFE_DIVIDE(SUM(minimum_delivery_value_local),COUNT(DISTINCT order_code)), 2) AS avg_MOV,
  ROUND(SAFE_DIVIDE(SUM(pd_order_final.final_df_wo_gst), COUNT(DISTINCT order_code)), 2) AS avg_DF,
  ROUND(SAFE_DIVIDE(SUM(vendor_subsidized_discount), SUM(gmv_local)), 2) AS VF_discounts_prop,
  ROUND(SAFE_DIVIDE(SUM(foodpanda_subsidized_discount), SUM(gmv_local)), 2) AS FP_discounts_prop,
  SAFE_DIVIDE(SUM(foodpanda_subsidized_discount), SUM(vendor_subsidized_discount)) AS FP_VF_discount_ratio,
  ROUND(SAFE_DIVIDE(SUM(vendor_subsidized_voucher), SUM(gmv_local)), 2) AS VF_vouchers_prop,
  ROUND(SAFE_DIVIDE(SUM(foodpanda_subsidized_voucher), SUM(gmv_local)), 2) AS FP_vouchers_prop,
  SAFE_DIVIDE(SUM(foodpanda_subsidized_voucher), SUM(vendor_subsidized_voucher)) AS FP_VF_voucher_ratio,
FROM pd_order_final
INNER JOIN `fulfillment-dwh-production.pandata_report.country_SG_rs_gross_profit_per_order` AS gpo ON pd_order_final.code = gpo.order_code
WHERE gpo.vendor_business_type = 'restaurants'
GROUP BY 1,2,3
ORDER BY 1,2,3
