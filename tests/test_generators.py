import csv
import os
import tempfile
import unittest

from src.generators import generate_emr_data as g
from src.lib.keys import surrogate_key


class TestGenerators(unittest.TestCase):
    def test_hospitals_have_different_schemas_and_overlapping_ids(self):
        with tempfile.TemporaryDirectory() as d:
            g.main(["--out", d])
            a = list(csv.DictReader(open(os.path.join(d, "hospital_a", "patients.csv"))))
            b = list(csv.DictReader(open(os.path.join(d, "hospital_b", "patients.csv"))))
        self.assertNotEqual(set(a[0]), set(b[0]))
        self.assertEqual(a[0]["PatientID"], b[0]["patient_id"])  # the clash a CDM must resolve

    def test_surrogate_keys_do_not_clash_and_are_deterministic(self):
        self.assertNotEqual(surrogate_key("A", 1), surrogate_key("B", 1))
        self.assertEqual(surrogate_key("A", 1), surrogate_key("A", 1))
        self.assertEqual(len(surrogate_key("A", 1)), 64)

    def test_seeded_dirty_rows_exist(self):
        import random
        data = g.hospital(random.Random(1), "A")
        self.assertEqual(sum(1 for p in data["patients"] if p["FirstName"] == ""), 3)


if __name__ == "__main__":
    unittest.main()
