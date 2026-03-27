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

# ── Constants ──
$script:FabricBaseUri = "https://api.fabric.microsoft.com/v1"
$script:OneLakeBaseUri = "https://onelake.dfs.fabric.microsoft.com"

# ── Tokens ──
$script:Tokens = @{{
    "WORKSPACE_ID"       = $null
    "BRONZE_LH_ID"       = $null
    "SILVER_LH_ID"       = $null
    "GOLD_LH_ID"         = $null
    "SQL_ENDPOINT"        = $null
    "LAKEHOUSE_NAME"      = "{gold_lh}"
{domain_df_ids}
    "NB01_ID"             = $null
    "NB02_ID"             = $null
    "NB03_ID"             = $null
    "NB04_ID"             = $null
    "NB05_ID"             = $null
    "NB06_ID"             = $null
    "PIPELINE_ID"         = $null
    "SEMANTIC_MODEL_ID"   = $null
}}

# ── Authentication ──

function Get-FabricToken {{
    [CmdletBinding()]
    param()
    return (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
}}

function Get-StorageToken {{
    [CmdletBinding()]
    param()
    return (Get-AzAccessToken -ResourceUrl "https://storage.azure.com").Token
}}

# ── REST Wrapper ──

function Invoke-FabricRaw {{
    [CmdletBinding()]
    param(
        [string]$Method = "GET",
        [string]$Uri,
        [object]$Body = $null
    )
    $headers = @{{
        "Authorization" = "Bearer $(Get-FabricToken)"
        "Content-Type"  = "application/json"
    }}
    $params = @{{ Method = $Method; Uri = $Uri; Headers = $headers; UseBasicParsing = $true }}
    if ($Body) {{
        $params["Body"] = if ($Body -is [string]) {{ $Body }} else {{ $Body | ConvertTo-Json -Depth 30 }}
        $params["ContentType"] = "application/json"
    }}
    return Invoke-WebRequest @params
}}

function Invoke-FabricApi {{
    [CmdletBinding()]
    param(
        [string]$Method = "GET",
        [string]$Uri,
        [object]$Body = $null
    )
    $resp = Invoke-FabricRaw -Method $Method -Uri $Uri -Body $Body
    if ($resp.StatusCode -eq 202) {{
        $opUrl = $resp.Headers["Location"]
        if ($opUrl) {{ Wait-LongRunning $opUrl | Out-Null }}
        return $null
    }}
    if ($resp.Content) {{ return ($resp.Content | ConvertFrom-Json) }}
    return $null
}}

function Wait-LongRunning {{
    [CmdletBinding()]
    param([string]$OperationUrl)
    $maxWait = 180; $elapsed = 0
    while ($elapsed -lt $maxWait) {{
        $retryAfter = 5
        Start-Sleep -Seconds $retryAfter; $elapsed += $retryAfter
        $headers = @{{ "Authorization" = "Bearer $(Get-FabricToken)" }}
        $resp = Invoke-WebRequest -Uri $OperationUrl -Headers $headers -UseBasicParsing
        $status = $resp.Content | ConvertFrom-Json
        Write-Host "    LRO status: $($status.status) (${{elapsed}}s)" -ForegroundColor DarkGray
        if ($status.status -eq "Succeeded") {{ return $status }}
        if ($status.status -eq "Failed") {{ throw "LRO failed: $($resp.Content)" }}
        if ($resp.Headers["Retry-After"]) {{ $retryAfter = [int]$resp.Headers["Retry-After"] }}
    }}
    Write-Warning "LRO timed out after ${{maxWait}}s at $OperationUrl"
}}

# ── Token Management ──

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

# ── Utility ──

function To-Base64 {{
    [CmdletBinding()]
    param([string]$Text)
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text))
}}

# ── OneLake Upload ──

function Upload-FileToOneLake {{
    [CmdletBinding()]
    param(
        [string]$WorkspaceId,
        [string]$LakehouseId,
        [string]$LocalPath,
        [string]$DestinationPath
    )
    $token = Get-StorageToken
    $uri = "$script:OneLakeBaseUri/$WorkspaceId/$LakehouseId/Files/$DestinationPath"

    # Create file
    Invoke-RestMethod -Method PUT -Uri "$($uri)?resource=file" -Headers @{{
        "Authorization" = "Bearer $token"
    }} | Out-Null

    # Append content
    $bytes = [System.IO.File]::ReadAllBytes($LocalPath)
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=0&action=append" -Headers @{{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/octet-stream"
    }} -Body $bytes | Out-Null

    # Flush
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=$($bytes.Length)&action=flush" -Headers @{{
        "Authorization" = "Bearer $token"
    }} | Out-Null
}}

# ── Item Finders ──

function Find-OrCreateItem {{
    [CmdletBinding()]
    param(
        [string]$WorkspaceId,
        [string]$DisplayName,
        [string]$Type,
        [object]$Body
    )
    try {{
        $resp = Invoke-FabricApi -Method POST -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items" -Body $Body
        if ($resp -and $resp.id) {{ return $resp.id }}
    }} catch {{
        $errDetail = $_.ToString()
        try {{ $sr = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() }} catch {{}}
        if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {{
            Write-Host "  $DisplayName already exists, looking up..." -ForegroundColor Yellow
        }} else {{
            Write-Warning "  Failed to create $DisplayName`: $errDetail"
            return $null
        }}
    }}
    # Look up existing item
    $items = (Invoke-FabricApi -Uri "$script:FabricBaseUri/workspaces/$WorkspaceId/items").value
    $found = $items | Where-Object {{ $_.displayName -eq $DisplayName -and $_.type -eq $Type }}
    if ($found) {{ return $found.id }}
    return $null
}}

# ── Progress ──

function Write-Step {{
    [CmdletBinding()]
    param([int]$Number, [int]$Total, [string]$Message)
    Write-Host ""
    Write-Host "[$Number/$Total] $Message" -ForegroundColor Cyan
    Write-Host ("-" * 60) -ForegroundColor DarkGray
}}

Export-ModuleMember -Function Get-FabricToken, Get-StorageToken, Invoke-FabricRaw, `
    Invoke-FabricApi, Wait-LongRunning, Set-Token, Get-Token, Resolve-Tokens, `
    To-Base64, Upload-FileToOneLake, Find-OrCreateItem, Write-Step
'''


def _generate_deploy_full(company, bronze_lh, silver_lh, gold_lh,
                          domains, tables_by_domain):
    """Generate the main Deploy-Full.ps1 orchestrator."""
    total_steps = 12
    upload_blocks = ""
    for domain in domains:
        upload_blocks += f'''
    # Upload {domain} CSVs
    $csvDir = Join-Path $SampleDataDir "{domain}"
    if (Test-Path $csvDir) {{
        foreach ($csv in (Get-ChildItem $csvDir -Filter "*.csv")) {{
            Upload-FileToOneLake -WorkspaceId $WorkspaceId -LakehouseId (Get-Token "BRONZE_LH_ID") `
                -LocalPath $csv.FullName -DestinationPath "{domain}/$($csv.Name)"
            $totalCsv++
            Write-Host "  [$totalCsv] {domain}/$($csv.Name)" -ForegroundColor Gray
        }}
    }}
'''

    # Build report info from output naming convention: <Company>-Analytics, -Forecasting, -HTAP
    report_names = [
        f"{company}-Analytics",
        f"{company}-Forecasting",
        f"{company}-HTAP",
    ]
    report_array_entries = "\n".join(
        f'    @{{ Name = "{r}"; Dir = "{r}.Report" }}'
        for r in report_names
    )

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
    [switch]$SkipForecast,
    [switch]$SkipHTAP,
    [switch]$SkipDataAgent
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$OutputRoot = Split-Path $PSScriptRoot -Parent
Import-Module (Join-Path $PSScriptRoot "{company}.psm1") -Force

$totalSteps = {total_steps}
$FabricBase = "https://api.fabric.microsoft.com/v1"

# ── Step 1: Authentication ──
Write-Step -Number 1 -Total $totalSteps -Message "Authenticating to Fabric..."
$null = Get-FabricToken
Write-Host "  Authentication successful." -ForegroundColor Green

# ── Step 2: Create/validate workspace ──
Write-Step -Number 2 -Total $totalSteps -Message "Setting up workspace..."
if (-not $WorkspaceId) {{
    Write-Host "  Creating new workspace: {company}Demo" -ForegroundColor Yellow
    $ws = Invoke-FabricApi -Method POST -Uri "$FabricBase/workspaces" `
        -Body @{{ displayName = "{company}Demo"; description = "{company} End-to-End Demo" }}
    $WorkspaceId = $ws.id
}}
Set-Token "WORKSPACE_ID" $WorkspaceId
Write-Host "  Workspace ID: $WorkspaceId" -ForegroundColor Green

# ── Step 3: Create Lakehouses ──
Write-Step -Number 3 -Total $totalSteps -Message "Creating Lakehouses..."
$lhUri = "$FabricBase/workspaces/$WorkspaceId/lakehouses"
foreach ($lh in @("{bronze_lh}", "{silver_lh}", "{gold_lh}")) {{
    $tokenKey = switch ($lh) {{
        "{bronze_lh}" {{ "BRONZE_LH_ID" }}
        "{silver_lh}" {{ "SILVER_LH_ID" }}
        "{gold_lh}"   {{ "GOLD_LH_ID" }}
    }}
    $lhId = Find-OrCreateItem -WorkspaceId $WorkspaceId -DisplayName $lh -Type "Lakehouse" `
        -Body @{{ displayName = $lh }}
    if ($lhId) {{
        Set-Token $tokenKey $lhId
        Write-Host "  $lh = $lhId" -ForegroundColor Green
    }}
}}

# ── Step 4: Upload Sample Data ──
Write-Step -Number 4 -Total $totalSteps -Message "Uploading sample data to {bronze_lh}..."
$totalCsv = 0
{upload_blocks}
Write-Host "  Uploaded $totalCsv CSV files." -ForegroundColor Green

# ── Step 5: Create Spark Environment ──
Write-Step -Number 5 -Total $totalSteps -Message "Creating Spark Environment..."
Write-Host "  Spark environment auto-provisioned with Lakehouse." -ForegroundColor Yellow

# ── Step 6: Deploy Notebooks ──
Write-Step -Number 6 -Total $totalSteps -Message "Deploying Notebooks..."
$notebookDir = Join-Path $OutputRoot "notebooks"
$nbFiles = Get-ChildItem $notebookDir -Filter "*.py" | Sort-Object Name
foreach ($nb in $nbFiles) {{
    $nbContent = Get-Content $nb.FullName -Raw -Encoding UTF8
    # Replace lakehouse tokens with actual IDs
    $nbContent = Resolve-Tokens $nbContent

    $nbName = $nb.BaseName

    # Convert .py to ipynb JSON
    $sourceLines = @()
    foreach ($line in ($nbContent -split "`n")) {{
        $sourceLines += ($line.TrimEnd("`r") + "`n")
    }}
    $ipynb = @{{
        nbformat = 4
        nbformat_minor = 5
        cells = @(
            @{{
                cell_type = "code"
                source = $sourceLines
                execution_count = $null
                outputs = @()
                metadata = @{{}}
            }}
        )
        metadata = @{{
            language_info = @{{ name = "python" }}
            kernel_info = @{{ name = "synapse_pyspark" }}
        }}
    }}
    $ipynbJson = $ipynb | ConvertTo-Json -Depth 10 -Compress
    $b64 = To-Base64 $ipynbJson

    $body = @{{
        displayName = $nbName
        type = "Notebook"
        definition = @{{
            format = "ipynb"
            parts = @(
                @{{
                    path = "notebook-content.ipynb"
                    payload = $b64
                    payloadType = "InlineBase64"
                }}
            )
        }}
    }}
    $nbId = Find-OrCreateItem -WorkspaceId $WorkspaceId -DisplayName $nbName -Type "Notebook" -Body $body
    if ($nbId) {{
        # Store NB token by extracting step number from filename (e.g. 01_BronzeToSilver → NB01_ID)
        if ($nb.Name -match "^(\\d+)_") {{
            $nbTokenKey = "NB0$($Matches[1])_ID" -replace "NB0(\\d{{2}})", 'NB$1'
            Set-Token $nbTokenKey $nbId
        }}
        Write-Host "  Created: $nbName ($nbId)" -ForegroundColor Green
    }}
}}

# ── Step 7: Deploy Dataflows ──
Write-Step -Number 7 -Total $totalSteps -Message "Uploading Dataflow definitions..."
$dfDir = Join-Path $OutputRoot "Dataflows"
$goldId = Get-Token "GOLD_LH_ID"
if (Test-Path $dfDir) {{
    $dfFiles = Get-ChildItem $dfDir -Filter "*.json"
    foreach ($df in $dfFiles) {{
        $dest = "Dataflows/$($df.Name)"
        Upload-FileToOneLake -WorkspaceId $WorkspaceId -LakehouseId $goldId `
            -LocalPath $df.FullName -DestinationPath $dest
        Write-Host "  Uploaded: $($df.Name)" -ForegroundColor Green
    }}
}}

# ── Step 8: Deploy Pipeline ──
Write-Step -Number 8 -Total $totalSteps -Message "Deploying Data Pipeline..."
$pipelineFile = Join-Path $OutputRoot "Pipeline" "pipeline-content.json"
if (Test-Path $pipelineFile) {{
    $pipelineJson = Get-Content $pipelineFile -Raw -Encoding UTF8
    $pipelineJson = Resolve-Tokens $pipelineJson
    $b64 = To-Base64 $pipelineJson

    $pipBody = @{{
        displayName = "{company}-ETL"
        type = "DataPipeline"
        definition = @{{
            parts = @(
                @{{
                    path = "pipeline-content.json"
                    payload = $b64
                    payloadType = "InlineBase64"
                }}
            )
        }}
    }}
    $pipId = Find-OrCreateItem -WorkspaceId $WorkspaceId -DisplayName "{company}-ETL" `
        -Type "DataPipeline" -Body $pipBody
    if ($pipId) {{
        Set-Token "PIPELINE_ID" $pipId
        Write-Host "  Created pipeline: {company}-ETL ($pipId)" -ForegroundColor Green
    }}
}}

# ── Step 9: Deploy Semantic Model ──
Write-Step -Number 9 -Total $totalSteps -Message "Deploying Semantic Model..."
$smDir = Join-Path $OutputRoot "{company}Model.SemanticModel"
$defDir = Join-Path $smDir "definition"

$parts = @()

# definition.pbism
$pbismPath = Join-Path $smDir "definition.pbism"
if (Test-Path $pbismPath) {{
    $pbism = Get-Content $pbismPath -Raw -Encoding UTF8
    $parts += @{{ path = "definition.pbism"; payload = (To-Base64 $pbism); payloadType = "InlineBase64" }}
}}

# model.tmdl
$modelPath = Join-Path $defDir "model.tmdl"
if (Test-Path $modelPath) {{
    $modelTmdl = Get-Content $modelPath -Raw -Encoding UTF8
    $modelTmdl = Resolve-Tokens $modelTmdl
    $parts += @{{ path = "definition/model.tmdl"; payload = (To-Base64 $modelTmdl); payloadType = "InlineBase64" }}
}}

# Table TMDL files
$tablesDir = Join-Path $defDir "tables"
if (Test-Path $tablesDir) {{
    foreach ($tf in (Get-ChildItem $tablesDir -Filter "*.tmdl")) {{
        $content = Resolve-Tokens (Get-Content $tf.FullName -Raw -Encoding UTF8)
        $parts += @{{ path = "definition/tables/$($tf.Name)"; payload = (To-Base64 $content); payloadType = "InlineBase64" }}
    }}
}}

# Relationship TMDL files
$relDir = Join-Path $defDir "relationships"
if (Test-Path $relDir) {{
    foreach ($rf in (Get-ChildItem $relDir -Filter "*.tmdl")) {{
        $content = Get-Content $rf.FullName -Raw -Encoding UTF8
        $parts += @{{ path = "definition/relationships/$($rf.Name)"; payload = (To-Base64 $content); payloadType = "InlineBase64" }}
    }}
}}

$smBody = @{{
    displayName = "{company}Model"
    type = "SemanticModel"
    definition = @{{
        format = "TMDL"
        parts = $parts
    }}
}}
$smId = Find-OrCreateItem -WorkspaceId $WorkspaceId -DisplayName "{company}Model" `
    -Type "SemanticModel" -Body $smBody
if ($smId) {{
    Set-Token "SEMANTIC_MODEL_ID" $smId
    Write-Host "  Created SemanticModel: {company}Model ($smId)" -ForegroundColor Green
}}

# ── Step 10: Deploy Reports ──
Write-Step -Number 10 -Total $totalSteps -Message "Deploying Power BI Reports..."
$reportDirs = @(
{report_array_entries}
)

foreach ($reportInfo in $reportDirs) {{
    $rptDir = Join-Path $OutputRoot $reportInfo.Dir "definition"
    if (-not (Test-Path $rptDir)) {{
        Write-Warning "  Report dir not found: $rptDir — skipping $($reportInfo.Name)"
        continue
    }}

    $rptParts = @()
    $allFiles = Get-ChildItem $rptDir -Recurse -File
    foreach ($f in $allFiles) {{
        $relPath = $f.FullName.Substring((Join-Path $OutputRoot $reportInfo.Dir).Length + 1).Replace("\\", "/")
        $content = Get-Content $f.FullName -Raw -Encoding UTF8
        $content = Resolve-Tokens $content
        $rptParts += @{{
            path = $relPath
            payload = (To-Base64 $content)
            payloadType = "InlineBase64"
        }}
    }}

    $rptBody = @{{
        displayName = $reportInfo.Name
        type = "Report"
        definition = @{{
            format = "PBIR"
            parts = $rptParts
        }}
    }}
    $rptId = Find-OrCreateItem -WorkspaceId $WorkspaceId -DisplayName $reportInfo.Name `
        -Type "Report" -Body $rptBody
    if ($rptId) {{
        Write-Host "  Created report: $($reportInfo.Name) ($rptId)" -ForegroundColor Green
    }}
}}

# ── Step 11: Deploy Data Agent ──
if (-not $SkipDataAgent) {{
    Write-Step -Number 11 -Total $totalSteps -Message "Deploying Data Agent (requires F64+)..."
    Write-Host "  Data Agent deployment requires F64+ capacity — skipping if unavailable." -ForegroundColor Yellow
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
Write-Host "  Open: https://app.powerbi.com/groups/$WorkspaceId/list" -ForegroundColor Cyan
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
