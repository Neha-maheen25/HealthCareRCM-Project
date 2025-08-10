# the SQL queries used in BigQuery for analysis :
-- ===============================================
-- REVENUE METRICS
-- ===============================================

-- 1. Total Revenue
SELECT
  SUM(PaidAmount) AS total_revenue
FROM `healthcare_rcm.fact_claims`;

-- 2. Revenue by Hospital (assuming ProviderID maps to hospitals)
SELECT
  ProviderID,
  SUM(PaidAmount) AS total_revenue
FROM `healthcare_rcm.fact_claims`
GROUP BY ProviderID
ORDER BY total_revenue DESC;

-- 3. Monthly Revenue Trends
SELECT
  FORMAT_DATE('%Y-%m', DATE(ClaimDate)) AS month,
  SUM(PaidAmount) AS monthly_revenue
FROM `healthcare_rcm.fact_claims`
GROUP BY month
ORDER BY month;

-- ===============================================
-- CLAIMS PERFORMANCE
-- ===============================================

-- 4. Approval & Denial Rates
SELECT
  COUNTIF(ClaimStatus = 'Approved') AS approved_claims,
  COUNTIF(ClaimStatus = 'Denied') AS denied_claims,
  COUNT(*) AS total_claims,
  ROUND(COUNTIF(ClaimStatus = 'Approved') / COUNT(*) * 100, 2) AS approval_rate_percent,
  ROUND(COUNTIF(ClaimStatus = 'Denied') / COUNT(*) * 100, 2) AS denial_rate_percent
FROM `healthcare_rcm.fact_claims`;

-- 5. Average Processing Time (days)
SELECT
  ROUND(AVG(DATE_DIFF(DATE(ModifiedDate), DATE(ClaimDate), DAY)), 2) AS avg_processing_days
FROM `healthcare_rcm.fact_claims`
WHERE ClaimStatus = 'Approved';

-- ===============================================
-- PATIENT METRICS
-- ===============================================

-- 6. Patient Volume (Total unique patients)
SELECT
  COUNT(DISTINCT PatientID) AS total_patients
FROM `healthcare_rcm.fact_claims`;

-- 7. Patient Demographics (Gender)
SELECT
  gender,
  COUNT(*) AS patient_count
FROM `healthcare_rcm.dim_patients`
WHERE is_current = 1  -- INT64 type check for current record
GROUP BY gender;

-- 8. Insurance Mix
SELECT
  insurance,
  COUNT(*) AS patient_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 2) AS percentage
FROM `healthcare_rcm.dim_patients`
WHERE is_current = 1
GROUP BY insurance;

-- ===============================================
-- OPERATIONAL EFFICIENCY
-- ===============================================

-- 9. Days in A/R (average)
SELECT
  ROUND(AVG(DATE_DIFF(DATE(PaidDate), DATE(ClaimDate), DAY)), 2) AS avg_days_in_ar
FROM `healthcare_rcm.fact_claims`
WHERE PaidAmount > 0;

-- 10. Collection Rate
SELECT
  ROUND(SUM(PaidAmount) / NULLIF(SUM(ClaimAmount), 0) * 100, 2) AS collection_rate_percent
FROM `healthcare_rcm.fact_claims`;

-- 11. Write-off Amounts
SELECT
  SUM(ClaimAmount - PaidAmount) AS total_write_off
FROM `healthcare_rcm.fact_claims`
WHERE PaidAmount < ClaimAmount;
