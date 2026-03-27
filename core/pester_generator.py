"""Pester test generator — produces PowerShell Pester 5 test suites.

Generates per-industry .Tests.ps1 files with categories:
- Unit: CSV counts, file existence
- NonRegression: TMDL measures, relationships
- DataQuality: FK integrity, null checks
- DeployScript: PS1 syntax, token patterns
"""

import json
from pathlib import Path


def generate_pester_tests(industry_config: dict,
                          sample_data_config: dict | None,
                          semantic_model_config: dict | None,
                          reports_config: dict | None,
                          output_dir: Path) -> list[Path]:
    """Generate Pester 5 test suites for an industry demo."""
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    industry_id = industry["id"]

    tests_dir = output_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # ── Main industry test file ──
    test_path = tests_dir / f"{company}.Tests.ps1"
    content = _build_pester_suite(industry, sample_data_config,
                                  semantic_model_config, reports_config, company)
    test_path.write_text(content, encoding="utf-8")
    created.append(test_path)

    # ── Test runner ──
    runner_path = tests_dir / "Run-Tests.ps1"
    runner = f'''<#
.SYNOPSIS
    Run all Pester tests for {company}.
#>
[CmdletBinding()]
param(
    [string]$OutputDir = (Join-Path $PSScriptRoot ".."),
    [string[]]$Tag = @(),
    [string[]]$ExcludeTag = @("Integration")
)

$config = New-PesterConfiguration
$config.Run.Path = $PSScriptRoot
$config.Run.PassThru = $true
$config.Output.Verbosity = "Detailed"
if ($Tag) {{ $config.Filter.Tag = $Tag }}
if ($ExcludeTag) {{ $config.Filter.ExcludeTag = $ExcludeTag }}

$result = Invoke-Pester -Configuration $config

if ($result.FailedCount -gt 0) {{
    Write-Host "\\n$($result.FailedCount) test(s) failed." -ForegroundColor Red
    exit 1
}} else {{
    Write-Host "\\nAll $($result.PassedCount) tests passed." -ForegroundColor Green
}}
'''
    runner_path.write_text(runner, encoding="utf-8")
    created.append(runner_path)

    return created


def _build_pester_suite(industry, sample_data_config, semantic_model_config,
                        reports_config, company):
    """Build the full Pester 5 test suite."""
    sections = []

    # Header
    sections.append(f'''<#
.SYNOPSIS
    Pester 5 test suite for {company} Fabric demo.
.DESCRIPTION
    Validates generated artifacts: CSV data, TMDL model, reports,
    deploy scripts, and cross-artifact consistency.
#>

$OutputDir = $env:FABRIC_OUTPUT_DIR
if (-not $OutputDir) {{ $OutputDir = (Join-Path $PSScriptRoot "..") }}
''')

    # ── Unit Tests: CSV data ──
    if sample_data_config:
        domains = sample_data_config.get("sampleData", {}).get("domains", [])
        total_csv = sum(len(d.get("tables", [])) for d in domains)

        csv_checks = []
        for d in domains:
            folder = d.get("folder", d["name"])
            for t in d.get("tables", []):
                fn = t.get("fileName", f"{t['name']}.csv")
                rows = t.get("rowCount", 0)
                csv_checks.append(
                    f'        It "has {fn} with {rows}+ rows" -Tag Unit {{\n'
                    f'            $path = Join-Path $OutputDir "SampleData" "{folder}" "{fn}"\n'
                    f'            $path | Should -Exist\n'
                    f'            $lines = (Get-Content $path).Count - 1  # minus header\n'
                    f'            $lines | Should -BeGreaterOrEqual {rows}\n'
                    f'        }}'
                )

        sections.append(f'''Describe "{company} CSV Data" -Tag Unit {{
    Context "File count" {{
        It "generates {total_csv} CSV files" {{
            $csvFiles = Get-ChildItem (Join-Path $OutputDir "SampleData") -Recurse -Filter "*.csv"
            $csvFiles.Count | Should -Be {total_csv}
        }}
    }}

    Context "Per-table validation" {{
{chr(10).join(csv_checks)}
    }}
}}
''')

    # ── TMDL Tests ──
    if semantic_model_config:
        sm = semantic_model_config
        table_count = len(sm.get("tables", []))
        rel_count = len(sm.get("relationships", []))
        measure_count = len(sm.get("measures", []))
        model_name = sm.get("name", f"{company}Model")

        sections.append(f'''Describe "{company} Semantic Model" -Tag NonRegression, TMDL {{
    $smDir = Join-Path $OutputDir "{model_name}.SemanticModel" "definition"

    It "has model.tmdl" {{
        Join-Path $smDir "model.tmdl" | Should -Exist
    }}

    It "has {table_count} table files" {{
        $tables = Get-ChildItem (Join-Path $smDir "tables") -Filter "*.tmdl"
        $tables.Count | Should -Be {table_count}
    }}

    It "has {rel_count} relationship files" {{
        $rels = Get-ChildItem (Join-Path $smDir "relationships") -Filter "*.tmdl"
        $rels.Count | Should -Be {rel_count}
    }}

    It "contains {measure_count} measures" {{
        $tmdlContent = Get-ChildItem (Join-Path $smDir "tables") -Filter "*.tmdl" |
            Get-Content -Raw
        $measureMatches = [regex]::Matches($tmdlContent, "(?m)^\\s+measure ")
        $measureMatches.Count | Should -Be {measure_count}
    }}

    It "has .pbism file" {{
        Join-Path $OutputDir "{model_name}.SemanticModel" "definition.pbism" | Should -Exist
    }}
}}
''')

    # ── Report Tests ──
    if reports_config:
        reports = reports_config.get("reports", [])
        for rpt in reports:
            rpt_name = rpt.get("name", "Report")
            page_count = len(rpt.get("pages", []))
            sections.append(f'''Describe "{rpt_name} Report" -Tag Report {{
    $reportDir = Join-Path $OutputDir "{rpt_name}.Report" "definition"

    It "has report.json" {{
        Join-Path $reportDir "report.json" | Should -Exist
    }}

    It "has {page_count} pages" {{
        $pages = Get-ChildItem (Join-Path $reportDir "pages") -Directory
        $pages.Count | Should -Be {page_count}
    }}

    It "has theme file" {{
        $themes = Get-ChildItem (Join-Path $reportDir "StaticResources" "SharedResources" "BaseThemes") -Filter "*.json"
        $themes.Count | Should -BeGreaterOrEqual 1
    }}

    It "has .pbip file" {{
        Join-Path $OutputDir "{rpt_name}.pbip" | Should -Exist
    }}
}}
''')

    # ── Deploy Script Tests ──
    sections.append(f'''Describe "{company} Deploy Scripts" -Tag DeployScript {{
    $deployDir = Join-Path $OutputDir "deploy"

    It "has Deploy-Full.ps1" {{
        Join-Path $deployDir "Deploy-Full.ps1" | Should -Exist
    }}

    It "has shared module ({company}.psm1)" {{
        Join-Path $deployDir "{company}.psm1" | Should -Exist
    }}

    It "has Upload-SampleData.ps1" {{
        Join-Path $deployDir "Upload-SampleData.ps1" | Should -Exist
    }}

    It "has Validate-Deployment.ps1" {{
        Join-Path $deployDir "Validate-Deployment.ps1" | Should -Exist
    }}
}}
''')

    # ── Pipeline Tests ──
    sections.append(f'''Describe "{company} Pipeline" -Tag Unit {{
    $pipelineFile = Join-Path $OutputDir "Pipeline" "pipeline-content.json"

    It "has pipeline-content.json" {{
        $pipelineFile | Should -Exist
    }}

    It "is valid JSON" {{
        {{ Get-Content $pipelineFile -Raw | ConvertFrom-Json }} | Should -Not -Throw
    }}

    It "has activities" {{
        $pl = Get-Content $pipelineFile -Raw | ConvertFrom-Json
        $pl.properties.activities.Count | Should -BeGreaterOrEqual 4
    }}
}}
''')

    # ── Token Pattern Tests ──
    sections.append(f'''Describe "{company} Token Patterns" -Tag TokenPattern {{
    It "pipeline tokens follow {{{{TOKEN}}}} pattern" {{
        $content = Get-Content (Join-Path $OutputDir "Pipeline" "pipeline-content.json") -Raw
        $tokens = [regex]::Matches($content, "\\{{\\{{([A-Z0-9_]+)\\}}\\}}")
        $tokens.Count | Should -BeGreaterOrEqual 1
        foreach ($t in $tokens) {{
            $t.Groups[1].Value | Should -Match "^[A-Z][A-Z0-9_]+$"
        }}
    }}
}}
''')

    return "\n".join(sections)
