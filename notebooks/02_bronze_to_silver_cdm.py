# Databricks notebook source
# Bronze (Parquet per hospital) -> Silver Common Data Model (Delta). Reference code: runtime values (paths, catalogs, endpoints, secrets) come from pipeline/job configuration.
import sys
sys.path.append("../src")  # when run from a Databricks Repo, or package src/lib as a wheel
from pyspark.sql import functions as F
from lib.quality import RULES_PATIENT, RULES_TXN, quarantine_flag_sql, quarantine_reason_sql
from lib.scd2 import scd2_merge

BRONZE = spark.conf.get("bronze_path", "/Volumes/rcm/bronze")
CAT = spark.conf.get("catalog", "rcm")

# Column mapping per hospital -> CDM names. New hospital = add a mapping, not a new pipeline.
MAP = {
    "A": {"patients": {"PatientID": "patient_id", "FirstName": "first_name", "LastName": "last_name", "SSN": "ssn",
                       "PhoneNumber": "phone", "Gender": "gender", "DOB": "dob", "Address": "address", "ModifiedDate": "source_modified_date"},
          "transactions": {"TransactionID": "transaction_id", "EncounterID": "encounter_id", "PatientID": "patient_id",
                           "ServiceDate": "service_date", "Amount": "amount", "PaidAmount": "paid_amount", "ClaimID": "claim_id",
                           "PayorID": "payor_id", "ProcedureCode": "procedure_code", "ICDCode": "icd_code"}},
    "B": {"patients": {"patient_id": "patient_id", "firstname": "first_name", "lastname": "last_name", "ssn": "ssn",
                       "phonenumber": "phone", "gender": "gender", "dob": "dob", "address": "address", "updated_date": "source_modified_date"},
          "transactions": {"transaction_id": "transaction_id", "encounter_id": "encounter_id", "patient_id": "patient_id",
                           "service_date": "service_date", "amount": "amount", "paid_amount": "paid_amount", "claim_id": "claim_id",
                           "payor_id": "payor_id", "procedure_code": "procedure_code", "icd_code": "icd_code"}},
}


def to_cdm(table, natural_key):
    parts = []
    for h, maps in MAP.items():
        df = spark.read.parquet(f"{BRONZE}/hospital_{h.lower()}/{table}")
        df = df.select([F.col(s).alias(t) for s, t in maps[table].items()]).withColumn("hospital_id", F.lit(h))
        parts.append(df)
    u = parts[0].unionByName(parts[1])
    # deterministic, collision-safe key (hospital A id 1 != hospital B id 1)
    return u.withColumn(f"{natural_key.replace('_id', '')}_key",
                        F.sha2(F.concat_ws("||", "hospital_id", F.col(natural_key).cast("string")), 256))


# ---- patients: PII handling + quarantine + SCD2
p = to_cdm("patients", "patient_id")
p = (p.withColumn("ssn_hash", F.sha2(F.concat_ws("|", F.lit(dbutils.secrets.get(spark.conf.get("secret_scope", "rcm-kv"), "ssn-salt")), "ssn"), 256))  # salt comes from a Key Vault-backed secret scope
       .drop("ssn")                                                  # never carry raw SSN into Silver
       .withColumn("is_quarantined", F.expr(quarantine_flag_sql(RULES_PATIENT)))
       .withColumn("quarantine_reason", F.expr(quarantine_reason_sql(RULES_PATIENT))))
scd2_merge(spark, p, f"{CAT}.silver.dim_patient", key="patient_key",
           tracked_cols=["first_name", "last_name", "phone", "gender", "dob", "address", "is_quarantined"])

# ---- transactions: idempotent MERGE (re-runs don't duplicate)
t = to_cdm("transactions", "transaction_id")
t = (t.withColumn("patient_key", F.sha2(F.concat_ws("||", "hospital_id", F.col("patient_id").cast("string")), 256))  # FK to dim_patient
       .withColumn("outstanding", F.round(F.col("amount") - F.col("paid_amount"), 2))
       .withColumn("is_quarantined", F.expr(quarantine_flag_sql(RULES_TXN)))
       .withColumn("quarantine_reason", F.expr(quarantine_reason_sql(RULES_TXN))))
tbl = f"{CAT}.silver.fact_transaction"
if not spark.catalog.tableExists(tbl):
    t.write.format("delta").saveAsTable(tbl)
else:
    t.createOrReplaceTempView("_t")
    spark.sql(f"MERGE INTO {tbl} x USING _t s ON x.transaction_key = s.transaction_key "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

# ---- DQ metrics (observability): one row per run per table
spark.sql(f"""INSERT INTO {CAT}.audit.dq_metrics
  SELECT current_timestamp(), 'fact_transaction', count(*), count_if(is_quarantined) FROM {tbl}""")
