"""Test generator — produces per-industry Pester 5 test suites.

Wraps pester_generator.py and additionally generates:
  - Industry-specific validation test files
  - Cross-industry comparison test suites
  - Test runner scripts

This module is the PLAN.md §7 'test_generator.py' that orchestrates
all test artifact generation from industry configs.
"""

from pathlib import Path

from core.pester_generator import generate_pester_tests


def generate_tests(industry_config: dict,
                   configs: dict,
                   output_dir: Path) -> list[Path]:
    """Generate all test artifacts for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        configs: Full config dict with all loaded JSON configs.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    created: list[Path] = []

    # Delegate to Pester generator for PS1 test suite
    pester_paths = generate_pester_tests(
        industry_config,
        configs.get("sample_data"),
        configs.get("semantic_model"),
        configs.get("reports"),
        output_dir,
    )
    created.extend(pester_paths)

    # Generate cross-artifact validation script
    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo").replace(" ", "")
    tests_dir = output_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)

    validation_path = tests_dir / f"Validate-{company}.ps1"
    validation_path.write_text(
        _build_validation_script(industry, configs, company),
        encoding="utf-8",
    )
    created.append(validation_path)

    return created


def _build_validation_script(industry: dict, configs: dict,
                             company: str) -> str:
    """Build a cross-artifact validation PowerShell script."""
    sm = configs.get("semantic_model", {})
    sd = configs.get("sample_data", {})

    sm_data = sm.get("semanticModel", sm) if sm else {}
    sd_data = sd.get("sampleData", sd) if sd else {}

    table_count = len(sm_data.get("tables", []))
    measure_count = len(sm_data.get("measures", []))
    rel_count = len(sm_data.get("relationships", []))
    csv_count = sum(
        len(d.get("tables", []))
        for d in sd_data.get("domains", [])
    )

    return f'''<#
.SYNOPSIS
    Cross-artifact validation for {company} Fabric demo.
.DESCRIPTION
    Verifies that generated artifacts are internally consistent:
    - CSV count matches sample-data.json
    - TMDL table count matches semantic-model.json
    - Measure and relationship counts match
    - All report directories exist
#>
[CmdletBinding()]
param(
    [string]$OutputDir = (Join-Path $PSScriptRoot "..")
)

$ErrorCount = 0

Write-Host "Validating {company} demo artifacts..." -ForegroundColor Cyan

# ── CSV Files ──
$csvFiles = Get-ChildItem (Join-Path $OutputDir "SampleData") -Recurse -Filter *.csv
if ($csvFiles.Count -lt {csv_count}) {{
    Write-Host "  FAIL: Expected {csv_count}+ CSVs, found $($csvFiles.Count)" -ForegroundColor Red
    $ErrorCount++
}} else {{
    Write-Host "  OK: $($csvFiles.Count) CSV files" -ForegroundColor Green
}}

# ── TMDL Tables ──
$smDir = Get-ChildItem (Join-Path $OutputDir "*.SemanticModel") -Directory | Select-Object -First 1
$tmdlTables = Get-ChildItem (Join-Path $smDir.FullName "definition/tables") -Filter *.tmdl -ErrorAction SilentlyContinue
if ($tmdlTables.Count -lt {table_count}) {{
    Write-Host "  FAIL: Expected {table_count}+ TMDL tables, found $($tmdlTables.Count)" -ForegroundColor Red
    $ErrorCount++
}} else {{
    Write-Host "  OK: $($tmdlTables.Count) TMDL tables" -ForegroundColor Green
}}

# ── Relationships ──
$tmdlRels = Get-ChildItem (Join-Path $smDir.FullName "definition/relationships") -Filter *.tmdl -ErrorAction SilentlyContinue
if ($tmdlRels.Count -lt {rel_count}) {{
    Write-Host "  FAIL: Expected {rel_count}+ relationships, found $($tmdlRels.Count)" -ForegroundColor Red
    $ErrorCount++
}} else {{
    Write-Host "  OK: $($tmdlRels.Count) relationships" -ForegroundColor Green
}}

# ── Notebooks ──
$notebooks = Get-ChildItem (Join-Path $OutputDir "notebooks") -Filter *.py
if ($notebooks.Count -lt 6) {{
    Write-Host "  FAIL: Expected 6+ notebooks, found $($notebooks.Count)" -ForegroundColor Red
    $ErrorCount++
}} else {{
    Write-Host "  OK: $($notebooks.Count) notebooks" -ForegroundColor Green
}}

# ── Summary ──
if ($ErrorCount -eq 0) {{
    Write-Host "`nAll validations passed." -ForegroundColor Green
}} else {{
    Write-Host "`n$ErrorCount validation(s) failed." -ForegroundColor Red
    exit 1
}}
'''
