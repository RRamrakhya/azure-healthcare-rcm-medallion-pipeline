"""Synthetic EMR + claims data for two hospitals with DIFFERENT schemas and OVERLAPPING IDs
(the reason a Common Data Model and collision-safe surrogate keys are needed). No real PHI: all values are fabricated."""
import argparse
import csv
import os
import random
from datetime import date, timedelta

FIRST = ["Asha", "Ben", "Chen", "Dara", "Eli", "Farah", "Gus", "Hana", "Ivan", "Jo"]
LAST = ["Alvarez", "Brooks", "Chua", "Dunn", "Evans", "Fox", "Garcia", "Holt", "Ito", "Jain"]
SPECIALTIES = ["Cardiology", "Orthopedics", "Pediatrics", "Oncology", "General"]
DEPTS = ["Cardiology", "Orthopedics", "Pediatrics", "Oncology", "Emergency"]
CPT = [("99213", "Office visit est. patient", 120.0), ("99285", "ED visit high severity", 650.0),
       ("93000", "Electrocardiogram", 85.0), ("73721", "MRI knee", 900.0), ("36415", "Venipuncture", 15.0)]
ICD = ["I10", "E11.9", "M17.11", "J45.909", "R07.9"]
PAYORS = ["PAY-AETNA", "PAY-BCBS", "PAY-MEDICARE", "PAY-SELF"]


def _w(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _rand_date(rng, a=date(2023, 1, 1), b=date(2024, 12, 31)):
    return a + timedelta(days=rng.randint(0, (b - a).days))


def hospital(rng, kind, n_pat=60, n_prov=8, n_enc=150):
    """kind 'A' uses PascalCase columns; kind 'B' uses snake_case/lowercase + different names. IDs start at 1 in both."""
    A = kind == "A"
    pat = [{("PatientID" if A else "patient_id"): i + 1,
            ("FirstName" if A else "firstname"): rng.choice(FIRST),
            ("LastName" if A else "lastname"): rng.choice(LAST),
            ("SSN" if A else "ssn"): f"9{rng.randint(10, 99)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}",
            ("PhoneNumber" if A else "phonenumber"): f"555-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}",
            ("Gender" if A else "gender"): rng.choice(["M", "F"]),
            ("DOB" if A else "dob"): _rand_date(rng, date(1940, 1, 1), date(2015, 12, 31)).isoformat(),
            ("Address" if A else "address"): f"{rng.randint(1, 999)} Test St",
            ("ModifiedDate" if A else "updated_date"): _rand_date(rng).isoformat()} for i in range(n_pat)]
    # inject data-quality problems for the quarantine logic to catch
    for i in rng.sample(range(n_pat), 3):
        pat[i][("FirstName" if A else "firstname")] = ""
    prov = [{("ProviderID" if A else "providerid"): i + 1, ("FirstName" if A else "firstname"): rng.choice(FIRST),
             ("LastName" if A else "lastname"): rng.choice(LAST),
             ("Specialization" if A else "specialization"): rng.choice(SPECIALTIES),
             ("DeptID" if A else "deptid"): rng.randint(1, len(DEPTS)),
             ("NPI" if A else "npi"): str(rng.randint(10**9, 10**10 - 1))} for i in range(n_prov)]
    dept = [{("DeptID" if A else "deptid"): i + 1, ("Name" if A else "name"): n} for i, n in enumerate(DEPTS)]
    enc, tx = [], []
    for i in range(n_enc):
        pid, prid = rng.randint(1, n_pat), rng.randint(1, n_prov)
        d = _rand_date(rng)
        proc = rng.choice(CPT)
        enc.append({("EncounterID" if A else "encounter_id"): i + 1, ("PatientID" if A else "patient_id"): pid,
                    ("ProviderID" if A else "provider_id"): prid, ("EncounterDate" if A else "encounter_date"): d.isoformat(),
                    ("ProcedureCode" if A else "procedure_code"): proc[0]})
        amt = proc[2]
        paid = rng.choice([0.0, amt * 0.8, amt, amt * 0.5])
        tx.append({("TransactionID" if A else "transaction_id"): i + 1, ("EncounterID" if A else "encounter_id"): i + 1,
                   ("PatientID" if A else "patient_id"): pid, ("ServiceDate" if A else "service_date"): d.isoformat(),
                   ("Amount" if A else "amount"): amt, ("PaidAmount" if A else "paid_amount"): round(paid, 2),
                   ("ClaimID" if A else "claim_id"): f"CLM-{kind}-{i + 1:05d}",
                   ("PayorID" if A else "payor_id"): rng.choice(PAYORS),
                   ("ProcedureCode" if A else "procedure_code"): proc[0], ("ICDCode" if A else "icd_code"): rng.choice(ICD)})
    return {"patients": pat, "providers": prov, "departments": dept, "encounters": enc, "transactions": tx}


def claims(rng, tx_rows, key):
    out = []
    for t in tx_rows:
        status = rng.choices(["Paid", "Denied", "Pending"], [70, 12, 18])[0]
        out.append({"ClaimID": t[key], "PayorID": t["PayorID" if "PayorID" in t else "payor_id"], "ClaimStatus": status,
                    "DenialReason": rng.choice(["Missing auth", "Not covered", "Coding error"]) if status == "Denied" else ""})
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=11)
    a = ap.parse_args(argv)
    rng = random.Random(a.seed)
    for kind in ("A", "B"):
        data = hospital(rng, kind)
        for name, rows in data.items():
            _w(os.path.join(a.out, f"hospital_{kind.lower()}", f"{name}.csv"), rows)
        key = "ClaimID" if kind == "A" else "claim_id"
        _w(os.path.join(a.out, "landing", f"claims_{kind.lower()}.csv"), claims(rng, data["transactions"], key))
    _w(os.path.join(a.out, "landing", "cpt_codes.csv"),
       [{"cpt_code": c, "description": d, "base_charge": p} for c, d, p in CPT])
    print("generated under", a.out)


if __name__ == "__main__":
    main()
