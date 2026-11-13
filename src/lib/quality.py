"""Declarative data-quality rules rendered two ways: Spark SQL (pipeline) and Python (unit tests / local checks).
One definition -> no drift between what's tested and what runs."""
import re

RULES_PATIENT = [
    {"name": "patient_id_present", "col": "patient_id", "check": "not_null"},
    {"name": "first_name_present", "col": "first_name", "check": "not_blank"},
    {"name": "last_name_present", "col": "last_name", "check": "not_blank"},
    {"name": "gender_valid", "col": "gender", "check": "in_set", "values": ["M", "F", "U"]},
]
RULES_TXN = [
    {"name": "amount_positive", "col": "amount", "check": "positive"},
    {"name": "paid_not_negative", "col": "paid_amount", "check": "non_negative"},
    {"name": "claim_id_present", "col": "claim_id", "check": "not_blank"},
]


def sql_pass(rule):
    c, k = rule["col"], rule["check"]
    return {"not_null": f"{c} IS NOT NULL", "not_blank": f"{c} IS NOT NULL AND trim({c}) <> ''",
            "positive": f"{c} > 0", "non_negative": f"{c} >= 0",
            "in_set": f"{c} IN ({', '.join(repr(v) for v in rule.get('values', []))})"}[k]


def py_pass(rule, row):
    v, k = row.get(rule["col"]), rule["check"]
    if k == "not_null":
        return v is not None
    if k == "not_blank":
        return v is not None and str(v).strip() != ""
    if v is None:
        return False
    if k == "positive":
        return v > 0
    if k == "non_negative":
        return v >= 0
    if k == "in_set":
        return v in rule["values"]
    raise ValueError(k)


def quarantine_flag_sql(rules):
    """CASE expression -> boolean is_quarantined (nulls in a rule count as failure)."""
    allp = " AND ".join(f"coalesce({sql_pass(r)}, false)" for r in rules)
    return f"CASE WHEN {allp} THEN false ELSE true END"


def quarantine_reason_sql(rules):
    parts = ", ".join(f"CASE WHEN coalesce({sql_pass(r)}, false) THEN NULL ELSE '{r['name']}' END" for r in rules)
    return f"nullif(concat_ws(',', {parts}), '')"


def evaluate(rules, row):
    failed = [r["name"] for r in rules if not py_pass(r, row)]
    return {"is_quarantined": bool(failed), "reasons": failed}


def valid_rule_names(rules):
    return all(re.fullmatch(r"[a-z_]+", r["name"]) for r in rules)
