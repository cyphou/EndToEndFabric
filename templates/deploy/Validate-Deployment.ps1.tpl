# {{company_name}} — Validate-Deployment.ps1
# Validates that all artifacts are deployed correctly
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId
)

Import-Module "$PSScriptRoot/{{company_pascal}}.psm1" -Force

$token = Get-FabricToken
$pass = 0
$fail = 0

Write-Host "=== Validating {{company_name}} Deployment ==="

# Check workspace items
$items = Get-FabricItems -WorkspaceId $WorkspaceId -Token $token

# Verify Lakehouses
foreach ($lh in @("{{bronze_lh}}", "{{silver_lh}}", "{{gold_lh}}")) {
    if ($items | Where-Object { $_.displayName -eq $lh -and $_.type -eq "Lakehouse" }) {
        Write-Host "  [PASS] Lakehouse: $lh"
        $pass++
    } else {
        Write-Host "  [FAIL] Lakehouse: $lh"
        $fail++
    }
}

# Verify Semantic Model
if ($items | Where-Object { $_.type -eq "SemanticModel" }) {
    Write-Host "  [PASS] Semantic Model exists"
    $pass++
} else {
    Write-Host "  [FAIL] Semantic Model missing"
    $fail++
}

# Verify Reports
$reports = $items | Where-Object { $_.type -eq "Report" }
Write-Host "  Reports found: $($reports.Count)"

Write-Host "`n=== Results: $pass passed, $fail failed ==="
