"""CSV sample data generator.

Generates realistic CSV files from sample-data.json definitions.
Supports multiple data generation methods (sequence, random, date ranges, FK lookup)
while maintaining referential integrity across tables.
"""

import csv
import math
import os
import random
import string
from datetime import date, datetime, timedelta
from pathlib import Path


def generate_all_csvs(sample_data_config: dict, output_dir: Path,
                      seed: int = 42) -> dict[str, Path]:
    """Generate all CSV files defined in sample-data.json.

    Args:
        sample_data_config: Parsed sample-data.json content.
        output_dir: Base output directory (CSVs go to output_dir/SampleData/<domain>/).
        seed: Random seed for reproducibility.

    Returns:
        Dict mapping table name → generated CSV path.
    """
    random.seed(seed)
    sample_dir = output_dir / "SampleData"
    domains = sample_data_config.get("sampleData", {}).get("domains", [])

    # Phase 1: Generate dimension tables first (no FK dependencies)
    # Phase 2: Generate fact tables (may reference dimension PKs)
    generated_data = {}  # table_name → list of row dicts
    generated_paths = {}

    # Separate dims from facts based on whether columns have foreignKey refs
    dim_tables = []
    fact_tables = []

    for domain in domains:
        for table in domain.get("tables", []):
            has_fk = any(
                col.get("foreignKey") for col in table.get("columns", [])
            )
            entry = (domain.get("folder", domain["name"]), table)
            if has_fk:
                fact_tables.append(entry)
            else:
                dim_tables.append(entry)

    # Generate dims first
    for folder, table in dim_tables:
        rows = _generate_table_rows(table, generated_data)
        generated_data[table["name"]] = rows
        path = _write_csv(sample_dir / folder, table, rows)
        generated_paths[table["name"]] = path

    # Then facts (can reference dim PKs)
    for folder, table in fact_tables:
        rows = _generate_table_rows(table, generated_data)
        generated_data[table["name"]] = rows
        path = _write_csv(sample_dir / folder, table, rows)
        generated_paths[table["name"]] = path

    return generated_paths


def _write_csv(folder: Path, table: dict, rows: list[dict]) -> Path:
    """Write rows to a CSV file."""
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / table["fileName"]
    columns = [col["name"] for col in table["columns"]]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return path


def _generate_table_rows(table: dict, generated_data: dict) -> list[dict]:
    """Generate row_count rows for a table definition."""
    row_count = table.get("rowCount", 10)
    columns = table.get("columns", [])
    rows = []

    for i in range(row_count):
        row = {}
        for col in columns:
            row[col["name"]] = _generate_value(col, i, row_count, generated_data)
        rows.append(row)

    return rows


def _generate_value(col: dict, row_index: int, total_rows: int,
                    generated_data: dict):
    """Generate a single column value based on its generator config."""
    gen = col.get("generator", {})
    method = gen.get("method", "")
    params = gen.get("params", {})
    col_type = col.get("type", "string")

    # Foreign key lookup — pick from referenced table's PK values
    if col.get("foreignKey"):
        fk = col["foreignKey"]
        ref_table = fk["table"]
        ref_col = fk["column"]
        ref_rows = generated_data.get(ref_table, [])
        if ref_rows:
            chosen = random.choice(ref_rows)
            return chosen.get(ref_col, "")
        return ""

    if method == "sequence":
        start = params.get("start", 1)
        prefix = params.get("prefix", "")
        pad = params.get("pad", 0)
        val = start + row_index
        if prefix or pad:
            return f"{prefix}{str(val).zfill(pad)}"
        return val

    elif method == "random_int":
        low = params.get("min", 1)
        high = params.get("max", 1000)
        return random.randint(low, high)

    elif method == "random_float":
        low = params.get("min", 0.0)
        high = params.get("max", 1000.0)
        decimals = params.get("decimals", 2)
        return round(random.uniform(low, high), decimals)

    elif method == "random_choice":
        choices = params.get("values", ["A", "B", "C"])
        weights = params.get("weights")
        if weights and len(weights) == len(choices):
            return random.choices(choices, weights=weights, k=1)[0]
        return random.choice(choices)

    elif method == "random_date":
        start_str = params.get("start", "2024-01-01")
        end_str = params.get("end", "2026-12-31")
        start_dt = date.fromisoformat(start_str)
        end_dt = date.fromisoformat(end_str)
        delta = (end_dt - start_dt).days
        if delta <= 0:
            return start_str
        rand_days = random.randint(0, delta)
        return (start_dt + timedelta(days=rand_days)).isoformat()

    elif method == "uuid":
        # Simple pseudo-UUID (no uuid module dependency needed for this format)
        hex_chars = string.hexdigits[:16]
        parts = [
            "".join(random.choices(hex_chars, k=8)),
            "".join(random.choices(hex_chars, k=4)),
            "".join(random.choices(hex_chars, k=4)),
            "".join(random.choices(hex_chars, k=4)),
            "".join(random.choices(hex_chars, k=12)),
        ]
        return "-".join(parts)

    elif method == "formula":
        # Simple expression referencing other columns in the same row
        # Not supported at generation time (would need row context)
        return ""

    elif method == "faker":
        # Lightweight faker — domain-specific data from params
        faker_type = params.get("type", "name")
        return _fake_value(faker_type, row_index, params)

    # Default fallback by column type
    if col_type == "int":
        return row_index + 1
    elif col_type == "float" or col_type == "decimal":
        return round(random.uniform(0, 1000), 2)
    elif col_type == "date":
        base = date(2024, 1, 1) + timedelta(days=row_index)
        return base.isoformat()
    elif col_type == "boolean":
        return random.choice(["true", "false"])
    else:
        return f"{col.get('name', 'val')}_{row_index + 1}"


# ── Lightweight faker functions (no external dependency) ────────────────

_FIRST_NAMES = [
    "James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Sophia", "Mason",
    "Isabella", "Ethan", "Mia", "Lucas", "Charlotte", "Benjamin", "Amelia",
    "Alexander", "Harper", "Daniel", "Evelyn", "Henry", "Abigail", "Sebastian",
    "Emily", "Jack", "Elizabeth", "Aiden", "Sofia", "Owen", "Ella", "Samuel",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Taylor", "Anderson", "Thomas", "Jackson",
    "White", "Harris", "Martin", "Thompson", "Moore", "Young", "Allen", "King",
    "Wright", "Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Clark",
]

_CITIES = [
    "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto",
    "Mumbai", "São Paulo", "Mexico City", "Chicago", "Denver", "Seattle",
    "San Francisco", "Boston", "Detroit", "Stuttgart", "Frankfurt", "Guadalajara", "Pune",
]

_COUNTRIES = [
    "United States", "United Kingdom", "Japan", "France", "Germany",
    "Australia", "Canada", "India", "Brazil", "Mexico",
]

_COMPANY_SUFFIXES = ["Inc.", "LLC", "Corp.", "Ltd.", "GmbH", "S.A.", "Co."]


def _fake_value(faker_type: str, index: int, params: dict):
    """Generate a fake value of the given type."""
    if faker_type == "name":
        return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"
    elif faker_type == "first_name":
        return random.choice(_FIRST_NAMES)
    elif faker_type == "last_name":
        return random.choice(_LAST_NAMES)
    elif faker_type == "email":
        first = random.choice(_FIRST_NAMES).lower()
        last = random.choice(_LAST_NAMES).lower()
        domains = params.get("domains", ["example.com", "company.org"])
        return f"{first}.{last}@{random.choice(domains)}"
    elif faker_type == "city":
        return random.choice(_CITIES)
    elif faker_type == "country":
        return random.choice(_COUNTRIES)
    elif faker_type == "company":
        name = f"{random.choice(_LAST_NAMES)} {random.choice(_COMPANY_SUFFIXES)}"
        return name
    elif faker_type == "phone":
        return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
    elif faker_type == "text":
        max_len = params.get("max_length", 50)
        words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
                 "adipiscing", "elit", "sed", "do", "eiusmod", "tempor"]
        text = " ".join(random.choices(words, k=random.randint(3, 8)))
        return text[:max_len]
    elif faker_type == "address":
        num = random.randint(1, 9999)
        street = f"{random.choice(_LAST_NAMES)} {random.choice(['St', 'Ave', 'Blvd', 'Dr', 'Ln'])}"
        return f"{num} {street}"
    else:
        return f"{faker_type}_{index + 1}"
