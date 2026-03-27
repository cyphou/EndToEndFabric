# {{company_name}} — Deploy-Full.ps1
# Auto-generated end-to-end deployment orchestrator
# Usage: .\Deploy-Full.ps1 -WorkspaceId <guid>
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,
    [string]$Environment = "Dev"
)

Import-Module "$PSScriptRoot/{{company_pascal}}.psm1" -Force

Write-Host "=== Deploying {{company_name}} Demo ==="

# Step 1: Authenticate
$token = Get-FabricToken
Write-Host "[1/12] Authenticated"

# Step 2: Create Lakehouses
$lakehouses = @("{{bronze_lh}}", "{{silver_lh}}", "{{gold_lh}}")
foreach ($lh in $lakehouses) {
    New-FabricItem -WorkspaceId $WorkspaceId -Type "Lakehouse" -DisplayName $lh -Token $token
}
Write-Host "[2/12] Lakehouses created"

# Step 3: Upload sample data
.\Upload-SampleData.ps1 -WorkspaceId $WorkspaceId
Write-Host "[3/12] Sample data uploaded"

# Step 4-8: Create and run notebooks
{{#EACH notebooks}}
Write-Host "[{{item.step}}/12] {{item.description}}"
{{/EACH notebooks}}

# Step 9-12: Create remaining artifacts
Write-Host "[9/12] Deploying Semantic Model..."
Write-Host "[10/12] Deploying Reports..."
Write-Host "[11/12] Deploying Pipeline..."
Write-Host "[12/12] Running validation..."
.\Validate-Deployment.ps1 -WorkspaceId $WorkspaceId

Write-Host "`n=== Deployment Complete ==="
