"""Tests for csv_generator module."""

import csv
import os
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_config_file
from core.csv_generator import generate_all_csvs


class TestCSVGenerator(unittest.TestCase):
    """Test CSV data generation."""

    @classmethod
    def setUpClass(cls):
        """Load config and generate CSVs once for all tests."""
        cls.sample_data = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = tempfile.mkdtemp(prefix="fabric_test_")
        cls.output_dir = Path(cls.tmpdir)
        cls.csv_paths = generate_all_csvs(cls.sample_data, cls.output_dir, seed=42)

    def test_all_tables_generated(self):
        """All tables from config should produce CSV files."""
        domains = self.sample_data["sampleData"]["domains"]
        expected_tables = set()
        for domain in domains:
            for table in domain["tables"]:
                expected_tables.add(table["name"])
        self.assertEqual(set(self.csv_paths.keys()), expected_tables)

    def test_csv_files_exist(self):
        """All generated CSV files should exist on disk."""
        for name, path in self.csv_paths.items():
            self.assertTrue(path.is_file(), f"Missing: {path}")

    def test_csv_headers_match_config(self):
        """CSV headers should match column definitions."""
        domains = self.sample_data["sampleData"]["domains"]
        for domain in domains:
            for table in domain["tables"]:
                path = self.csv_paths[table["name"]]
                with open(path, encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                expected_cols = [col["name"] for col in table["columns"]]
                self.assertEqual(headers, expected_cols,
                                 f"Header mismatch for {table['name']}")

    def test_row_counts(self):
        """Generated CSVs should have the expected number of rows."""
        domains = self.sample_data["sampleData"]["domains"]
        for domain in domains:
            for table in domain["tables"]:
                path = self.csv_paths[table["name"]]
                with open(path, encoding="utf-8") as f:
                    row_count = sum(1 for _ in f) - 1  # minus header
                self.assertEqual(row_count, table["rowCount"],
                                 f"Row count mismatch for {table['name']}: "
                                 f"expected {table['rowCount']}, got {row_count}")

    def test_no_empty_cells_in_pk(self):
        """Primary key columns should have no empty values."""
        domains = self.sample_data["sampleData"]["domains"]
        for domain in domains:
            for table in domain["tables"]:
                pk_cols = [col["name"] for col in table["columns"] if col.get("primaryKey")]
                if not pk_cols:
                    continue
                path = self.csv_paths[table["name"]]
                with open(path, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        for pk in pk_cols:
                            self.assertTrue(
                                row[pk].strip() != "",
                                f"Empty PK '{pk}' at row {i} in {table['name']}"
                            )

    def test_reproducible_with_same_seed(self):
        """Same seed should produce identical output."""
        tmpdir2 = Path(tempfile.mkdtemp(prefix="fabric_test_2_"))
        paths2 = generate_all_csvs(self.sample_data, tmpdir2, seed=42)

        for name in self.csv_paths:
            with open(self.csv_paths[name], encoding="utf-8") as f1:
                content1 = f1.read()
            with open(paths2[name], encoding="utf-8") as f2:
                content2 = f2.read()
            self.assertEqual(content1, content2,
                             f"Non-deterministic output for {name}")

    def test_different_seed_different_output(self):
        """Different seed should produce different data."""
        tmpdir3 = Path(tempfile.mkdtemp(prefix="fabric_test_3_"))
        paths3 = generate_all_csvs(self.sample_data, tmpdir3, seed=99)

        # At least one fact table should differ
        name = "FactOrders"
        with open(self.csv_paths[name], encoding="utf-8") as f1:
            content1 = f1.read()
        with open(paths3[name], encoding="utf-8") as f2:
            content2 = f2.read()
        self.assertNotEqual(content1, content2)

    def test_file_count(self):
        """Horizon Books should generate 17 CSV files."""
        self.assertEqual(len(self.csv_paths), 17)

    def test_fk_values_valid(self):
        """Foreign keys should reference existing PK values."""
        # Load DimAccounts PKs
        dim_accounts_path = self.csv_paths["DimAccounts"]
        with open(dim_accounts_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            valid_account_ids = {row["AccountID"] for row in reader}

        # Check FactFinancialTransactions.AccountID
        fact_path = self.csv_paths["FactFinancialTransactions"]
        with open(fact_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.assertIn(
                    row["AccountID"], valid_account_ids,
                    f"Invalid FK AccountID={row['AccountID']} in FactFinancialTransactions"
                )


class TestCSVDomainFolders(unittest.TestCase):
    """Test that CSVs are organized into domain folders."""

    @classmethod
    def setUpClass(cls):
        cls.sample_data = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_folders_"))
        cls.csv_paths = generate_all_csvs(cls.sample_data, cls.tmpdir, seed=42)

    def test_finance_folder(self):
        sample_dir = self.tmpdir / "SampleData" / "Finance"
        self.assertTrue(sample_dir.is_dir())
        csv_files = list(sample_dir.glob("*.csv"))
        self.assertEqual(len(csv_files), 4)

    def test_hr_folder(self):
        sample_dir = self.tmpdir / "SampleData" / "HR"
        self.assertTrue(sample_dir.is_dir())
        csv_files = list(sample_dir.glob("*.csv"))
        self.assertEqual(len(csv_files), 5)

    def test_operations_folder(self):
        sample_dir = self.tmpdir / "SampleData" / "Operations"
        self.assertTrue(sample_dir.is_dir())
        csv_files = list(sample_dir.glob("*.csv"))
        self.assertEqual(len(csv_files), 8)


if __name__ == "__main__":
    unittest.main()
