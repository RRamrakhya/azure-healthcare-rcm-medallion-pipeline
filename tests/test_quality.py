import unittest

from src.lib import quality as q


class TestQuality(unittest.TestCase):
    def test_python_and_sql_renderers_cover_same_rules(self):
        sql = q.quarantine_reason_sql(q.RULES_PATIENT)
        for r in q.RULES_PATIENT:
            self.assertIn(r["name"], sql)
        self.assertTrue(q.valid_rule_names(q.RULES_PATIENT + q.RULES_TXN))

    def test_blank_and_null_names_are_quarantined(self):
        base = {"patient_id": 1, "first_name": "Ann", "last_name": "Lee", "gender": "F"}
        self.assertFalse(q.evaluate(q.RULES_PATIENT, base)["is_quarantined"])
        self.assertEqual(q.evaluate(q.RULES_PATIENT, {**base, "first_name": " "})["reasons"], ["first_name_present"])
        self.assertTrue(q.evaluate(q.RULES_PATIENT, {**base, "last_name": None})["is_quarantined"])

    def test_multiple_reasons_and_numeric_rules(self):
        r = q.evaluate(q.RULES_TXN, {"amount": 0, "paid_amount": -1, "claim_id": ""})
        self.assertEqual(r["reasons"], ["amount_positive", "paid_not_negative", "claim_id_present"])
        self.assertFalse(q.evaluate(q.RULES_TXN, {"amount": 10, "paid_amount": 0, "claim_id": "C1"})["is_quarantined"])

    def test_sql_flag_is_null_safe(self):
        self.assertIn("coalesce(", q.quarantine_flag_sql(q.RULES_TXN))


if __name__ == "__main__":
    unittest.main()
