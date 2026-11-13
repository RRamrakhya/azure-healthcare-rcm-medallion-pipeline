-- Parameterize the as-of date for reproducibility. Replace rcm with your catalog.
-- Outstanding AR and aging buckets
CREATE OR REPLACE VIEW rcm.gold.v_ar_aging AS
SELECT hospital_id, payor_id,
  CASE WHEN datediff(current_date(), service_date) <= 30 THEN '0-30'
       WHEN datediff(current_date(), service_date) <= 60 THEN '31-60'
       WHEN datediff(current_date(), service_date) <= 90 THEN '61-90' ELSE '90+' END AS aging_bucket,
  sum(outstanding) AS ar_balance
FROM rcm.gold.fact_transaction WHERE outstanding > 0 GROUP BY 1,2,3;

-- Days in AR = Net AR / (Net charges over last 90 days / 90)
CREATE OR REPLACE VIEW rcm.gold.v_days_in_ar AS
WITH ar AS (SELECT hospital_id, sum(outstanding) AS ar_total FROM rcm.gold.fact_transaction GROUP BY 1),
     chg AS (SELECT hospital_id, sum(amount) / 90.0 AS avg_daily_charges
             FROM rcm.gold.fact_transaction WHERE service_date >= date_sub(current_date(), 90) GROUP BY 1)
SELECT ar.hospital_id, ar.ar_total, chg.avg_daily_charges,
       round(ar.ar_total / nullif(chg.avg_daily_charges, 0), 1) AS days_in_ar
FROM ar JOIN chg USING (hospital_id);

-- High-risk share: AR older than 90 days
CREATE OR REPLACE VIEW rcm.gold.v_ar_over_90 AS
SELECT hospital_id, sum(CASE WHEN aging_bucket = '90+' THEN ar_balance END) / sum(ar_balance) AS pct_ar_over_90
FROM rcm.gold.v_ar_aging GROUP BY hospital_id;

-- Denial rate by payor (needs claims in Silver/Gold)
-- SELECT payor_id, count_if(claim_status='Denied')/count(*) AS denial_rate FROM rcm.gold.fact_claim GROUP BY payor_id;
