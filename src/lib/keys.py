"""Deterministic surrogate keys. Spark equivalent:  sha2(concat_ws('||', hospital_id, natural_id), 256)
Why not append a suffix or use max()+1? Hash keys are stable across reruns/backfills and safe under concurrent writers."""
import hashlib


def surrogate_key(hospital_id: str, natural_id) -> str:
    return hashlib.sha256("||".join([str(hospital_id), str(natural_id)]).encode("utf-8")).hexdigest()
