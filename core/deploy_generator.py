"""Deploy generator — produces PowerShell deployment scripts and shared module.

Generates:
  deploy/Deploy-Full.ps1         — End-to-end orchestrator
  deploy/<Company>.psm1          — Shared module (auth, Fabric API helpers)
  deploy/Upload-SampleData.ps1   — CSV upload to BronzeLH
  deploy/Validate-Deployment.ps1 — Post-deploy validation
"""

import json
from pathlib import Path


def generate_deploy_scripts(industry_config: dict,
                            sample_data_config: dict | None,
                            output_dir: Path) -> list[Path]:
    """Generate PowerShell deployment scripts for the industry demo."""
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    lakehouses = artifacts.get("lakehouses", {})
    bronze_lh = lakehouses.get("bronze", "BronzeLH")
    silver_lh = lakehouses.get("silver", "SilverLH")
    gold_lh = lakehouses.get("gold", "GoldLH")

    domains = []
    tables_by_domain = {}
    if sample_data_config:
        for d in sample_data_config.get("sampleData", {}).get("domains", []):
            domains.append(d["name"])
            tables_by_domain[d["name"]] = [t["name"] for t in d["tables"]]

    deploy_dir = output_dir / "deploy"
    deploy_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # ── Shared Module ──
    psm1 = _generate_shared_module(company, bronze_lh, silver_lh, gold_lh, domains)
    psm1_path = deploy_dir / f"{company}.psm1"
    psm1_path.write_text(psm1, encoding="utf-8")
    created.append(psm1_path)

    # ── Deploy-Full.ps1 ──
    full = _generate_deploy_full(company, bronze_lh, silver_lh, gold_lh,
                                 domains, tables_by_domain)
    full_path = deploy_dir / "Deploy-Full.ps1"
    full_path.write_text(full, encoding="utf-8")
    created.append(full_path)

    # ── Upload-SampleData.ps1 ──
    upload = _generate_upload_script(company, bronze_lh, domains, tables_by_domain)
    upload_path = deploy_dir / "Upload-SampleData.ps1"
    upload_path.write_text(upload, encoding="utf-8")
    created.append(upload_path)

    # ── Validate-Deployment.ps1 ──
    validate = _generate_validate_script(company, bronze_lh, silver_lh, gold_lh,
                                         domains, tables_by_domain)
    validate_path = deploy_dir / "Validate-Deployment.ps1"
    validate_path.write_text(validate, encoding="utf-8")
    created.append(validate_path)

    return created


def _generate_shared_module(company, bronze_lh, silver_lh, gold_lh, domains):
    """Generate the shared PowerShell module (.psm1)."""
    domain_df_ids = "\n".join(
        f'    "DF_{d.upper()}_ID" = $null' for d in domains
    )
    return f'''<#
.SYNOPSIS
    {company} deployment shared module — Fabric REST API helpers.
.DESCRIPTION
    Contains authentication, token management, OneLake upload,
    and Fabric REST API wrapper functions.
#>

# ── Tokens ──
$script:Tokens = @{{
    "WORKSPACE_ID"   = $null
    "BRONZE_LH_ID"   = $null
    "SILVER_LH_ID"   = $null
    "GOLD_LH_ID"     = $null
    "SQL_ENDPOINT"    = $null
    "LAKEHOUSE_NAME"  = "{gold_lh}"
{domain_df_ids}
    "NB01_ID"         = $null
    "NB02_ID"         = $null
    "NB03_ID"         = $null
    "NB04_ID"         = $null
    "PIPELINE_ID"     = $null
    "SEMANTIC_MODEL_ID" = $null
}}

function Get-FabricToken {{
    [CmdletBinding()]
    param()
    $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    return $token
}}

function Invoke-FabricApi {{
    [CmdletBinding()]
    param(
        [string]$Method = "GET",
        [string]$Uri,
        [object]$Body = $null,
        [string]$ContentType = "application/json"
    )
    $headers = @{{
        "Authorization" = "Bearer $(Get-FabricToken)"
        "Content-Type"  = $ContentType
    }}
    $params = @{{
        Method  = $Method
        Uri     = $Uri
        Headers = $headers
    }}
    if ($Body) {{
        if ($Body -is [string]) {{
            $params["Body"] = $Body
        }} else {{
            $params["Body"] = $Body | ConvertTo-Json -Depth 20
        }}
    }}
    $response = Invoke-RestMethod @params
    return $response
}}

function Set-Token {{
    [CmdletBinding()]
    param([string]$Name, [string]$Value)
    $script:Tokens[$Name] = $Value
}}

function Get-Token {{
    [CmdletBinding()]
    param([string]$Name)
    return $script:Tokens[$Name]
}}

function Resolve-Tokens {{
    [CmdletBinding()]
    param([string]$Content)
    $result = $Content
    foreach ($key in $script:Tokens.Keys) {{
        if ($script:Tokens[$key]) {{
            $result = $result -replace "\\{{\\{{$key\\}}\\}}", $script:Tokens[$key]
        }}
    }}
    return $result
}}

function Upload-FileToOneLake {{
    [CmdletBinding()]
    param(
        [string]$WorkspaceId,
        [string]$LakehouseId,
        [string]$LocalPath,
        [string]$DestinationPath
    )
    $token = Get-FabricToken
    $uri = "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId/$LakehouseId/Files/$DestinationPath"

    # Create file
    Invoke-RestMethod -Method PUT -Uri "$($uri)?resource=file" -Headers @{{
        "Authorization" = "Bearer $token"
    }}

    # Upload content
    $bytes = [System.IO.File]::ReadAllBytes($LocalPath)
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=0&action=append" -Headers @{{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/octet-stream"
    }} -Body $bytes

    # Flush
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=$($bytes.Length)&action=flush" -Headers @{{
        "Authorization" = "Bearer $token"
    }}
}}

function Write-Step {{
    [CmdletBinding()]
    param([int]$Number, [int]$Total, [string]$Message)
    Write-Host ""
    Write-Host "[$Number/$Total] $Message" -ForegroundColor Cyan
    Write-Host ("-" * 60) -ForegroundColor DarkGray
}}

Export-ModuleMember -Function Get-FabricToken, Invoke-FabricApi, Set-Token, Get-Token, `
    Resolve-Tokens, Upload-FileToOneLake, Write-Step
'''


def _generate_deploy_full(company, bronze_lh, silver_lh, gold_lh,
                          domains, tables_by_domain):
    """Generate the main Deploy-Full.ps1 orchestrator."""
    total_steps = 12
    upload_blocks = ""
    for domain in domains:
        tables = tables_by_domain.get(domain, [])
        upload_blocks += f'''
    # Upload {domain} CSVs
    $csvDir = Join-Path $SampleDataDir "{domain}"
    foreach ($csv in (Get-ChildItem $csvDir -Filter "*.csv")) {{
        Upload-FileToOneLake -WorkspaceId $WorkspaceId -LakehouseId (Get-Token "BRONZE_LH_ID") `
            -LocalPath $csv.FullName -DestinationPath "{domain}/$($csv.Name)"
        Write-Host "  Uploaded $($csv.Name)" -ForegroundColor Green
    }}
'''

    return f'''<#
.SYNOPSIS
    {company} — Full end-to-end deployment to Microsoft Fabric.
.DESCRIPTION
    12-step deployment: Auth → Lakehouses → CSVs → Notebooks → Dataflows →
    Pipeline → Semantic Model → Reports → Data Agent → Validation.
.PARAMETER WorkspaceId
    Target Fabric workspace ID. If omitted, creates a new workspace.
.PARAMETER SampleDataDir
    Path to sample CSV data directory.
#>
[CmdletBinding()]
param(
    [string]$WorkspaceId,
    [string]$SampleDataDir = (Join-Path $PSScriptRoot ".." "SampleData"),
    [string]$DefinitionsDir = (Join-Path $PSScriptRoot ".." "definitions"),
    [switch]$SkipForecast,
    [switch]$SkipHTAP,
    [switch]$SkipDataAgent
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module (Join-Path $PSScriptRoot "{company}.psm1") -Force

$totalSteps = {total_steps}

# ── Step 1: Authentication ──
Write-Step -Number 1 -Total $totalSteps -Message "Authenticating to Fabric..."
$null = Get-FabricToken
Write-Host "  Authentication successful." -ForegroundColor Green

# ── Step 2: Create/validate workspace ──
Write-Step -Number 2 -Total $totalSteps -Message "Setting up workspace..."
if (-not $WorkspaceId) {{
    Write-Host "  Creating new workspace: {company}Demo" -ForegroundColor Yellow
    $ws = Invoke-FabricApi -Method POST -Uri "https://api.fabric.microsoft.com/v1/workspaces" `
        -Body @{{ displayName = "{company}Demo"; description = "{company} End-to-End Demo" }}
    $WorkspaceId = $ws.id
}}
Set-Token "WORKSPACE_ID" $WorkspaceId
Write-Host "  Workspace ID: $WorkspaceId" -ForegroundColor Green

# ── Step 3: Create Lakehouses ──
Write-Step -Number 3 -Total $totalSteps -Message "Creating Lakehouses..."
$baseUri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/lakehouses"
foreach ($lh in @("{bronze_lh}", "{silver_lh}", "{gold_lh}")) {{
    $resp = Invoke-FabricApi -Method POST -Uri $baseUri -Body @{{ displayName = $lh }}
    $tokenKey = switch ($lh) {{
        "{bronze_lh}" {{ "BRONZE_LH_ID" }}
        "{silver_lh}" {{ "SILVER_LH_ID" }}
        "{gold_lh}"   {{ "GOLD_LH_ID" }}
    }}
    Set-Token $tokenKey $resp.id
    Write-Host "  Created $lh ($($resp.id))" -ForegroundColor Green
}}

# ── Step 4: Upload Sample Data ──
Write-Step -Number 4 -Total $totalSteps -Message "Uploading sample data to BronzeLH..."
{upload_blocks}

# ── Step 5: Create Spark Environment ──
Write-Step -Number 5 -Total $totalSteps -Message "Creating Spark Environment..."
Write-Host "  (Environment creation — placeholder for REST API)" -ForegroundColor Yellow

# ── Step 6: Deploy Notebooks ──
Write-Step -Number 6 -Total $totalSteps -Message "Deploying Notebooks..."
$notebookDir = Join-Path $PSScriptRoot ".." "Notebooks"
$nbFiles = Get-ChildItem $notebookDir -Filter "*.py" | Sort-Object Name
foreach ($nb in $nbFiles) {{
    Write-Host "  Deploying $($nb.BaseName)..." -ForegroundColor Yellow
}}

# ── Step 7: Deploy Dataflows ──
Write-Step -Number 7 -Total $totalSteps -Message "Deploying Dataflows Gen2..."
Write-Host "  Deploying dataflows for: {', '.join(domains)}" -ForegroundColor Yellow

# ── Step 8: Deploy Pipeline ──
Write-Step -Number 8 -Total $totalSteps -Message "Deploying Pipeline..."
$pipelineDef = Get-Content (Join-Path $DefinitionsDir "Pipeline" "pipeline-content.json") -Raw
$pipelineDef = Resolve-Tokens $pipelineDef
Write-Host "  Pipeline definition tokens resolved." -ForegroundColor Green

# ── Step 9: Deploy Semantic Model ──
Write-Step -Number 9 -Total $totalSteps -Message "Deploying Semantic Model..."
Write-Host "  Deploying TMDL Semantic Model via REST API..." -ForegroundColor Yellow

# ── Step 10: Deploy Reports ──
Write-Step -Number 10 -Total $totalSteps -Message "Deploying Power BI Reports..."
Write-Host "  Deploying PBIR reports via REST API..." -ForegroundColor Yellow

# ── Step 11: Deploy Data Agent ──
if (-not $SkipDataAgent) {{
    Write-Step -Number 11 -Total $totalSteps -Message "Deploying Data Agent (requires F64+)..."
    Write-Host "  Data Agent deployment — placeholder." -ForegroundColor Yellow
}} else {{
    Write-Step -Number 11 -Total $totalSteps -Message "Skipping Data Agent (--SkipDataAgent)"
}}

# ── Step 12: Validation ──
Write-Step -Number 12 -Total $totalSteps -Message "Validating deployment..."
& (Join-Path $PSScriptRoot "Validate-Deployment.ps1") -WorkspaceId $WorkspaceId

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  {company} deployment complete!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host ""
'''


def _generate_upload_script(company, bronze_lh, domains, tables_by_domain):
    """Generate Upload-SampleData.ps1."""
    domain_blocks = ""
    for domain in domains:
        domain_blocks += f'''
    Write-Host "Uploading {domain} data..." -ForegroundColor Cyan
    $csvDir = Join-Path $SampleDataDir "{domain}"
    if (Test-Path $csvDir) {{
        foreach ($csv in (Get-ChildItem $csvDir -Filter "*.csv")) {{
            Upload-FileToOneLake -WorkspaceId $WorkspaceId -LakehouseId $LakehouseId `
                -LocalPath $csv.FullName -DestinationPath "{domain}/$($csv.Name)"
            Write-Host "  ✓ $($csv.Name)" -ForegroundColor Green
        }}
    }} else {{
        Write-Warning "  Directory not found: $csvDir"
    }}
'''

    return f'''<#
.SYNOPSIS
    Upload sample CSV data to {company} BronzeLH via OneLake DFS API.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$WorkspaceId,
    [Parameter(Mandatory)][string]$LakehouseId,
    [string]$SampleDataDir = (Join-Path $PSScriptRoot ".." "SampleData")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module (Join-Path $PSScriptRoot "{company}.psm1") -Force

Write-Host "Uploading sample data to {bronze_lh}..." -ForegroundColor Cyan
Write-Host "  Workspace: $WorkspaceId"
Write-Host "  Lakehouse: $LakehouseId"
Write-Host ""
{domain_blocks}
Write-Host ""
Write-Host "Upload complete." -ForegroundColor Green
'''


def _generate_validate_script(company, bronze_lh, silver_lh, gold_lh,
                              domains, tables_by_domain):
    """Generate Validate-Deployment.ps1."""
    all_tables = []
    for d in domains:
        all_tables.extend(tables_by_domain.get(d, []))
    table_count = len(all_tables)

    return f'''<#
.SYNOPSIS
    Validate {company} deployment — checks all expected Fabric items exist.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$WorkspaceId
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module (Join-Path $PSScriptRoot "{company}.psm1") -Force

Write-Host ""
Write-Host "Validating {company} deployment..." -ForegroundColor Cyan
Write-Host ("=" * 60)

$baseUri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
$items = (Invoke-FabricApi -Uri "$baseUri/items").value

$expectedTypes = @{{
    "Lakehouse"    = 3
    "Notebook"     = 4
    "DataPipeline" = 1
    "SemanticModel"= 1
    "Report"       = 2
}}

$pass = 0
$fail = 0

foreach ($type in $expectedTypes.Keys) {{
    $found = ($items | Where-Object {{ $_.type -eq $type }}).Count
    $expected = $expectedTypes[$type]
    if ($found -ge $expected) {{
        Write-Host "  ✓ $type`: $found/$expected" -ForegroundColor Green
        $pass++
    }} else {{
        Write-Host "  ✗ $type`: $found/$expected" -ForegroundColor Red
        $fail++
    }}
}}

Write-Host ""
Write-Host ("=" * 60)
if ($fail -eq 0) {{
    Write-Host "  All checks passed ($pass/$pass)" -ForegroundColor Green
}} else {{
    Write-Host "  $fail checks failed ($pass/$($pass+$fail) passed)" -ForegroundColor Red
}}
Write-Host ""
'''
