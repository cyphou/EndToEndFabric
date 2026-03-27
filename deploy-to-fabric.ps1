<#
.SYNOPSIS
    Deploy an industry demo to a Fabric workspace via REST API.
.DESCRIPTION
    Steps: Lakehouses → CSV upload → Notebooks → Semantic Model → Reports → Pipeline → Dataflows
.PARAMETER WorkspaceId
    Target Fabric workspace GUID.
.PARAMETER Industry
    Industry folder name under output/ (e.g. contoso-energy, horizon-books). Defaults to contoso-energy.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,
    [string]$Industry = "contoso-energy"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$OutputRoot = Join-Path (Join-Path $PSScriptRoot "output") $Industry
if (-not (Test-Path $OutputRoot)) {
    throw "Industry output directory not found: $OutputRoot. Run 'python generate.py -i $Industry' first."
}

# Derive company prefix from industry.json
$industryJsonPath = Join-Path (Join-Path $PSScriptRoot "industries") $Industry "industry.json"
$industryConfig = Get-Content $industryJsonPath -Raw | ConvertFrom-Json
$Company = $industryConfig.industry.name  # e.g. "ContosoEnergy"

$FabricBase = "https://api.fabric.microsoft.com/v1"
$OneLakeBase = "https://onelake.dfs.fabric.microsoft.com"

# ── Helpers ──

function Get-Headers {
    $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    return @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
}

function Get-StorageHeaders {
    $token = (Get-AzAccessToken -ResourceUrl "https://storage.azure.com").Token
    return @{ "Authorization" = "Bearer $token" }
}

function Invoke-FabricRaw {
    param([string]$Method = "GET", [string]$Uri, [object]$Body = $null)
    $h = Get-Headers
    $p = @{ Method = $Method; Uri = $Uri; Headers = $h; UseBasicParsing = $true }
    if ($Body) { $p["Body"] = ($Body | ConvertTo-Json -Depth 30); $p["ContentType"] = "application/json" }
    return Invoke-WebRequest @p
}

function Invoke-Fabric {
    param([string]$Method = "GET", [string]$Uri, [object]$Body = $null)
    $resp = Invoke-FabricRaw -Method $Method -Uri $Uri -Body $Body
    if ($resp.StatusCode -eq 202) {
        # Long-running operation — poll until done
        $opUrl = $resp.Headers["Location"]
        if ($opUrl) {
            $result = Wait-LongRunning $opUrl
            return $result
        }
        return $null
    }
    if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) }
    return $null
}

function Wait-LongRunning {
    param([string]$OperationUrl)
    $maxWait = 120; $elapsed = 0
    while ($elapsed -lt $maxWait) {
        $retryAfter = 5
        Start-Sleep -Seconds $retryAfter; $elapsed += $retryAfter
        $h = Get-Headers
        $resp = Invoke-WebRequest -Uri $OperationUrl -Headers $h -UseBasicParsing
        $status = $resp.Content | ConvertFrom-Json
        Write-Host "    LRO status: $($status.status) (${elapsed}s)" -ForegroundColor DarkGray
        if ($status.status -eq "Succeeded") { return $status }
        if ($status.status -eq "Failed") { throw "LRO failed: $($resp.Content)" }
        if ($resp.Headers["Retry-After"]) { $retryAfter = [int]$resp.Headers["Retry-After"] }
    }
    Write-Warning "LRO timed out after ${maxWait}s at $OperationUrl"
}

function To-Base64 { param([string]$Text) return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text)) }

function Write-Step { param([int]$N, [int]$T, [string]$Msg) Write-Host "`n[$N/$T] $Msg" -ForegroundColor Cyan; Write-Host ("-" * 60) -ForegroundColor DarkGray }

$totalSteps = 8
$tokens = @{}

# ======================================================================
# Step 1: Create Lakehouses
# ======================================================================
Write-Step -N 1 -T $totalSteps -Msg "Creating Lakehouses..."
$lhUri = "$FabricBase/workspaces/$WorkspaceId/lakehouses"
foreach ($lh in @("BronzeLH", "SilverLH", "GoldLH")) {
    try {
        $resp = Invoke-Fabric -Method POST -Uri $lhUri -Body @{ displayName = $lh }
        if ($resp -and $resp.id) {
            $tokens[$lh] = $resp.id
            Write-Host "  Created $lh = $($resp.id)" -ForegroundColor Green
        }
    } catch {
        $errDetail = $_.ToString()
        # Try to read error body from WebException
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errDetail = $sr.ReadToEnd(); $sr.Close()
        } catch {}
        if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
            Write-Host "  $lh already exists, looking up..." -ForegroundColor Yellow
        } else {
            Write-Warning "  Failed to create $lh : $errDetail"
        }
    }
    # Always look up the ID if we don't have it
    if (-not $tokens.ContainsKey($lh) -or -not $tokens[$lh]) {
        $all = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/lakehouses").value
        $existing = $all | Where-Object { $_.displayName -eq $lh }
        if ($existing) {
            $tokens[$lh] = $existing.id
            Write-Host "  Found $lh = $($existing.id)" -ForegroundColor Green
        }
    }
}

# ======================================================================
# Step 2: Upload Sample CSVs to BronzeLH
# ======================================================================
Write-Step -N 2 -T $totalSteps -Msg "Uploading sample data to BronzeLH..."
$bronzeId = $tokens["BronzeLH"]
$sampleDir = Join-Path $OutputRoot "SampleData"
$domains = Get-ChildItem $sampleDir -Directory
$totalCsv = 0
foreach ($domain in $domains) {
    $csvFiles = Get-ChildItem $domain.FullName -Filter "*.csv"
    foreach ($csv in $csvFiles) {
        $dest = "$($domain.Name)/$($csv.Name)"
        $uri = "$OneLakeBase/$WorkspaceId/$bronzeId/Files/$dest"
        $sh = Get-StorageHeaders

        # Create
        Invoke-RestMethod -Method PUT -Uri "$($uri)?resource=file" -Headers $sh | Out-Null
        # Append
        $bytes = [System.IO.File]::ReadAllBytes($csv.FullName)
        $sh2 = Get-StorageHeaders; $sh2["Content-Type"] = "application/octet-stream"
        Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=0&action=append" -Headers $sh2 -Body $bytes | Out-Null
        # Flush
        Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=$($bytes.Length)&action=flush" -Headers (Get-StorageHeaders) | Out-Null
        $totalCsv++
        Write-Host "  [$totalCsv] $dest ($([math]::Round($bytes.Length/1KB,1)) KB)" -ForegroundColor Gray
    }
}
Write-Host "  Uploaded $totalCsv CSV files to BronzeLH." -ForegroundColor Green

# ======================================================================
# Step 3: Deploy Notebooks
# ======================================================================
Write-Step -N 3 -T $totalSteps -Msg "Deploying Notebooks..."
$nbDir = Join-Path $OutputRoot "notebooks"
$nbFiles = Get-ChildItem $nbDir -Filter "*.py" | Sort-Object Name
$nbTokens = @{}
foreach ($nb in $nbFiles) {
    $nbContent = Get-Content $nb.FullName -Raw -Encoding UTF8
    # Replace lakehouse tokens with actual IDs
    $nbContent = $nbContent -replace "\{\{BRONZE_LH_ID\}\}", $tokens["BronzeLH"]
    $nbContent = $nbContent -replace "\{\{SILVER_LH_ID\}\}", $tokens["SilverLH"]
    $nbContent = $nbContent -replace "\{\{GOLD_LH_ID\}\}", $tokens["GoldLH"]
    $nbContent = $nbContent -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId

    $nbName = $nb.BaseName

    # Convert .py content to ipynb JSON structure
    # Split into cells on "# COMMAND ----------" or "# %%", else one big cell
    $lines = $nbContent -split "`n"
    $sourceLines = @()
    foreach ($line in $lines) {
        $sourceLines += ($line.TrimEnd("`r") + "`n")
    }
    $ipynb = @{
        nbformat = 4
        nbformat_minor = 5
        cells = @(
            @{
                cell_type = "code"
                source = $sourceLines
                execution_count = $null
                outputs = @()
                metadata = @{}
            }
        )
        metadata = @{
            language_info = @{ name = "python" }
            kernel_info = @{ name = "synapse_pyspark" }
        }
    }
    $ipynbJson = $ipynb | ConvertTo-Json -Depth 10 -Compress
    $b64 = To-Base64 $ipynbJson

    $body = @{
        displayName = $nbName
        type = "Notebook"
        definition = @{
            format = "ipynb"
            parts = @(
                @{
                    path = "notebook-content.ipynb"
                    payload = $b64
                    payloadType = "InlineBase64"
                }
            )
        }
    }
    try {
        Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/notebooks" -Body $body | Out-Null
        Write-Host "  Created notebook: $nbName" -ForegroundColor Green
    } catch {
        $errDetail = $_.ToString()
        try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
        if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
            Write-Host "  $nbName already exists, skipping." -ForegroundColor Yellow
        } else {
            Write-Warning "  Failed to create $nbName : $errDetail"
        }
    }
}
# Look up all notebook IDs by name
$allItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
foreach ($nb in $nbFiles) {
    $found = $allItems | Where-Object { $_.displayName -eq $nb.BaseName -and $_.type -eq "Notebook" }
    if ($found) { $nbTokens[$nb.BaseName] = $found.id }
}
Write-Host "  Notebooks deployed: $($nbTokens.Count)" -ForegroundColor Green

# ======================================================================
# Step 4: Deploy Semantic Model (TMDL)
# ======================================================================
Write-Step -N 4 -T $totalSteps -Msg "Deploying Semantic Model..."
$smDir = Join-Path $OutputRoot "${Company}Model.SemanticModel"
$defDir = Join-Path $smDir "definition"

# Gather all TMDL files into definition parts
$parts = @()

# definition.pbism → .platform
$pbism = Get-Content (Join-Path $smDir "definition.pbism") -Raw -Encoding UTF8
$parts += @{ path = "definition.pbism"; payload = (To-Base64 $pbism); payloadType = "InlineBase64" }

# model.tmdl
$modelTmdl = Get-Content (Join-Path $defDir "model.tmdl") -Raw -Encoding UTF8
# Replace tokens in model
$modelTmdl = $modelTmdl -replace "\{\{GOLD_LH_ID\}\}", $tokens["GoldLH"]
$modelTmdl = $modelTmdl -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
$parts += @{ path = "definition/model.tmdl"; payload = (To-Base64 $modelTmdl); payloadType = "InlineBase64" }

# Table TMDL files
$tableFiles = Get-ChildItem (Join-Path $defDir "tables") -Filter "*.tmdl"
foreach ($tf in $tableFiles) {
    $content = Get-Content $tf.FullName -Raw -Encoding UTF8
    $content = $content -replace "\{\{GOLD_LH_ID\}\}", $tokens["GoldLH"]
    $content = $content -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
    $parts += @{ path = "definition/tables/$($tf.Name)"; payload = (To-Base64 $content); payloadType = "InlineBase64" }
}

# Relationship TMDL files
$relDir = Join-Path $defDir "relationships"
if (Test-Path $relDir) {
    $relFiles = Get-ChildItem $relDir -Filter "*.tmdl"
    foreach ($rf in $relFiles) {
        $content = Get-Content $rf.FullName -Raw -Encoding UTF8
        $parts += @{ path = "definition/relationships/$($rf.Name)"; payload = (To-Base64 $content); payloadType = "InlineBase64" }
    }
}

$smBody = @{
    displayName = "${Company}Model"
    type = "SemanticModel"
    definition = @{
        format = "TMDL"
        parts = $parts
    }
}

try {
    Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Body $smBody | Out-Null
    Write-Host "  Created SemanticModel: ${Company}Model" -ForegroundColor Green
} catch {
    $errDetail = $_.ToString()
    try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
    if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
        Write-Host "  SemanticModel already exists." -ForegroundColor Yellow
    } else {
        Write-Warning "  SemanticModel deploy failed: $errDetail"
    }
}
# Look up SM ID
$allItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
$smId = ($allItems | Where-Object { $_.displayName -eq "${Company}Model" -and $_.type -eq "SemanticModel" }).id
if ($smId) { Write-Host "  SemanticModel ID: $smId" -ForegroundColor Green }
$tokens["SEMANTIC_MODEL_ID"] = $smId

# ======================================================================
# Step 5: Deploy Reports (PBIR)
# ======================================================================
Write-Step -N 5 -T $totalSteps -Msg "Deploying Power BI Reports..."

$reportDirs = @(
    @{ Name = "${Company}-Analytics"; Dir = "${Company}-Analytics.Report" }
    @{ Name = "${Company}-Forecasting"; Dir = "${Company}-Forecasting.Report" }
    @{ Name = "${Company}-HTAP"; Dir = "${Company}-HTAP.Report" }
)

foreach ($reportInfo in $reportDirs) {
    $rptDir = Join-Path (Join-Path $OutputRoot $reportInfo.Dir) "definition"
    if (-not (Test-Path $rptDir)) {
        Write-Warning "  Report dir not found: $rptDir"
        continue
    }

    $rptParts = @()

    # Recursively collect all files under definition/
    $allFiles = Get-ChildItem $rptDir -Recurse -File
    foreach ($f in $allFiles) {
        $relPath = $f.FullName.Substring((Join-Path $OutputRoot $reportInfo.Dir).Length + 1).Replace("\", "/")
        $content = Get-Content $f.FullName -Raw -Encoding UTF8
        # Replace SM reference if needed
        if ($smId) {
            $content = $content -replace "\{\{SEMANTIC_MODEL_ID\}\}", $smId
        }
        $content = $content -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
        $rptParts += @{
            path = $relPath
            payload = (To-Base64 $content)
            payloadType = "InlineBase64"
        }
    }

    $rptBody = @{
        displayName = $reportInfo.Name
        type = "Report"
        definition = @{
            format = "PBIR"
            parts = $rptParts
        }
    }

    try {
        Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Body $rptBody | Out-Null
        Write-Host "  Created report: $($reportInfo.Name)" -ForegroundColor Green
    } catch {
        $errDetail = $_.ToString()
        try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
        if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
            Write-Host "  $($reportInfo.Name) already exists, skipping." -ForegroundColor Yellow
        } else {
            Write-Warning "  Report deploy failed ($($reportInfo.Name)): $errDetail"
        }
    }
}

# ======================================================================
# Step 6: Deploy Pipeline
# ======================================================================
Write-Step -N 6 -T $totalSteps -Msg "Deploying Data Pipeline..."
$pipelineJson = Get-Content (Join-Path (Join-Path $OutputRoot "Pipeline") "pipeline-content.json") -Raw -Encoding UTF8
# Resolve all known tokens
$pipelineJson = $pipelineJson -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
foreach ($key in $tokens.Keys) {
    $pipelineJson = $pipelineJson -replace "\{\{$key\}\}", $tokens[$key]
}
foreach ($key in $nbTokens.Keys) {
    $tokenName = $key -replace "^(\d+)_", 'NB0$1_ID'
    $pipelineJson = $pipelineJson -replace "\{\{$tokenName\}\}", $nbTokens[$key]
}

$pipBody = @{
    displayName = "${Company}-ETL"
    type = "DataPipeline"
    definition = @{
        parts = @(
            @{
                path = "pipeline-content.json"
                payload = (To-Base64 $pipelineJson)
                payloadType = "InlineBase64"
            }
        )
    }
}

try {
    Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Body $pipBody | Out-Null
    Write-Host "  Created pipeline: ${Company}-ETL" -ForegroundColor Green
} catch {
    $errDetail = $_.ToString()
    try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
    if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
        Write-Host "  Pipeline already exists, skipping." -ForegroundColor Yellow
    } else {
        Write-Warning "  Pipeline deploy failed: $errDetail"
    }
}

# ======================================================================
# Step 7: Deploy Dataflow definitions (metadata only — actual Power Query runs in Fabric)
# ======================================================================
Write-Step -N 7 -T $totalSteps -Msg "Uploading Dataflow definitions to GoldLH/Files/Dataflows..."
$dfDir = Join-Path $OutputRoot "Dataflows"
$goldId = $tokens["GoldLH"]
$dfFiles = Get-ChildItem $dfDir -Filter "*.json"
foreach ($df in $dfFiles) {
    $dest = "Dataflows/$($df.Name)"
    $uri = "$OneLakeBase/$WorkspaceId/$goldId/Files/$dest"
    $sh = Get-StorageHeaders
    Invoke-RestMethod -Method PUT -Uri "$($uri)?resource=file" -Headers $sh | Out-Null
    $bytes = [System.IO.File]::ReadAllBytes($df.FullName)
    $sh2 = Get-StorageHeaders; $sh2["Content-Type"] = "application/octet-stream"
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=0&action=append" -Headers $sh2 -Body $bytes | Out-Null
    Invoke-RestMethod -Method PATCH -Uri "$($uri)?position=$($bytes.Length)&action=flush" -Headers (Get-StorageHeaders) | Out-Null
    Write-Host "  Uploaded $($df.Name)" -ForegroundColor Green
}

# ======================================================================
# Step 8: Summary
# ======================================================================
Write-Step -N 8 -T $totalSteps -Msg "Deployment Summary"

$finalItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  $Company deployment complete!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace: $WorkspaceId" -ForegroundColor White
Write-Host "  Items deployed:" -ForegroundColor White
$finalItems | Group-Object type | ForEach-Object { Write-Host "    $($_.Name): $($_.Count)" -ForegroundColor Gray }
Write-Host ""
Write-Host "  Open: https://app.powerbi.com/groups/$WorkspaceId/list" -ForegroundColor Cyan
Write-Host ""
