# Databricks notebook source
# Reference data from public APIs (NPI Registry, ICD-10-CM via NLM Clinical Tables). Requires outbound internet:
# fine on a standard Azure workspace; restricted on Databricks Free Edition (use the CSV fallback below).
import requests, time
from pyspark.sql import functions as F

BRONZE = spark.conf.get("bronze_path", "/Volumes/rcm/bronze/reference")


def fetch_icd(terms=("hypertension", "diabetes", "asthma", "chest pain", "knee")):
    rows = []
    for t in terms:
        r = requests.get("https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search",
                         params={"sf": "code,name", "terms": t, "maxList": 20}, timeout=30)
        r.raise_for_status()
        rows += [(c, n) for c, n in r.json()[3]]
        time.sleep(0.5)  # be polite to a public API
    return rows


def fetch_npi(npis):
    rows = []
    for n in npis:
        r = requests.get("https://npiregistry.cms.hhs.gov/api/", params={"version": "2.1", "number": n}, timeout=30)
        if r.ok and r.json().get("results"):
            b = r.json()["results"][0]["basic"]
            rows.append((n, b.get("first_name"), b.get("last_name"), b.get("credential")))
        time.sleep(0.5)
    return rows


icd = spark.createDataFrame(fetch_icd(), ["icd_code", "icd_desc"]).dropDuplicates(["icd_code"]).withColumn("_ingested_at", F.current_timestamp())
icd.write.mode("overwrite").parquet(f"{BRONZE}/icd")
# Synthetic providers use fake NPIs, so the NPI call only demonstrates the integration pattern on real NPIs you supply.
