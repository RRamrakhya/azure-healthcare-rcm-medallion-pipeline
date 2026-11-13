# Databricks notebook source
# Gold = current + non-quarantined rows only. Facts/dims as Delta tables; KPI views in sql/gold_kpis.sql
CAT = spark.conf.get("catalog", "rcm")
spark.sql(f"""CREATE OR REPLACE TABLE {CAT}.gold.dim_patient AS
  SELECT patient_key, hospital_id, patient_id, first_name, last_name, gender, dob, ssn_hash, inserted_date AS valid_from
  FROM {CAT}.silver.dim_patient WHERE is_current AND NOT is_quarantined""")
spark.sql(f"""CREATE OR REPLACE TABLE {CAT}.gold.fact_transaction AS
  SELECT transaction_key, hospital_id, patient_key, encounter_id, service_date, amount, paid_amount, outstanding,
         payor_id, procedure_code, icd_code, claim_id
  FROM {CAT}.silver.fact_transaction WHERE NOT is_quarantined""")
