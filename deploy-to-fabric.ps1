<#
.SYNOPSIS
    Deploy an industry demo to a Fabric workspace via REST API.
.DESCRIPTION
    Steps: Lakehouses → CSV upload → Notebooks → Semantic Model → Reports → Pipeline → Dataflows
.PARAMETER WorkspaceId
    Target Fabric workspace GUID.
.PARAMETER Industry
    Industry folder name under output/ (e.g. contoso-energy, horizon-books). Defaults to contoso-energy.
.PARAMETER Clean
    Delete all existing items in the workspace before deploying.
.PARAMETER TriggerPipeline
    After deployment, trigger the ETL pipeline run and wait for it to complete.
.PARAMETER Autoplay
    After the pipeline succeeds (or standalone), export PNG screenshots of every report page,
    run basic visual-overlap and empty-data checks, then open the screenshot folder.
.PARAMETER SkipDeploy
    Skip all deployment steps (1-10). Only trigger the pipeline (-TriggerPipeline) and/or
    take screenshots (-Autoplay). Requires the workspace to already be deployed.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,
    [string]$Industry = "contoso-energy",
    [switch]$Clean,
    [switch]$TriggerPipeline,
    [switch]$Autoplay,
    [switch]$SkipDeploy
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$OutputRoot = Join-Path (Join-Path $PSScriptRoot "output") $Industry
if (-not (Test-Path $OutputRoot)) {
    throw "Industry output directory not found: $OutputRoot. Run 'python generate.py -i $Industry' first."
}

# Derive company prefix from industry.json
$industryJsonPath = Join-Path (Join-Path (Join-Path $PSScriptRoot "industries") $Industry) "industry.json"
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
    param([string]$OperationUrl, [int]$MaxWait = 300)
    $elapsed = 0
    while ($elapsed -lt $MaxWait) {
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
    Write-Warning "LRO timed out after ${MaxWait}s at $OperationUrl"
}

function To-Base64 { param([string]$Text) return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text)) }

function Write-Step { param([int]$N, [int]$T, [string]$Msg) Write-Host "`n[$N/$T] $Msg" -ForegroundColor Cyan; Write-Host ("-" * 60) -ForegroundColor DarkGray }

# ── Power BI Export-to-File screenshot helper ──────────────────────────
function Invoke-ReportScreenshots {
    param(
        [string]$WsId,
        [string]$ReportName,
        [string]$ReportId,
        [string]$OutputDir
    )
    $pbiBase = "https://api.powerbi.com/v1.0/myorg"
    $pbiToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
    $pbiH = @{ "Authorization" = "Bearer $pbiToken" }

    # Enumerate pages from local output (page.json) — fast, no extra API call
    $localReportDir = Join-Path (Join-Path (Join-Path $PSScriptRoot "output") $Industry) "${ReportName}.Report"
    $pagesDir = Join-Path $localReportDir "definition\pages"
    $pages = @()
    if (Test-Path $pagesDir) {
        foreach ($d in (Get-ChildItem $pagesDir -Directory)) {
            $pjson = Join-Path $d.FullName "page.json"
            if (Test-Path $pjson) {
                $pdata = Get-Content $pjson -Raw | ConvertFrom-Json
                $visProp = $pdata.PSObject.Properties['visibility']
                if ($null -eq $visProp -or $visProp.Value -ne 1) {   # 1 = hidden
                    $pages += [PSCustomObject]@{ Name = $d.Name; DisplayName = $pdata.displayName }
                }
            }
        }
    }
    if ($pages.Count -eq 0) {
        Write-Host "    No visible pages found for $ReportName" -ForegroundColor DarkGray
        return @()
    }

    # Build paginated export request (PNG per page)
    $pagePayloads = $pages | ForEach-Object { @{ pageName = $_.Name } }
    $exportBody = @{
        format = "PNG"
        powerBIReportConfiguration = @{
            pages = $pagePayloads
        }
    } | ConvertTo-Json -Depth 6

    try {
        $startResp = Invoke-WebRequest -Method POST `
            -Uri "$pbiBase/groups/$WsId/reports/$ReportId/ExportTo" `
            -Headers $pbiH -Body $exportBody -ContentType "application/json" -UseBasicParsing
        $exportId = ($startResp.Content | ConvertFrom-Json).id
    } catch {
        Write-Warning "    ExportTo failed for $ReportName : $_"
        return @()
    }

    # Poll export status (up to 3 min)
    $elapsed = 0
    $exportStatus = $null
    while ($elapsed -lt 180) {
        Start-Sleep -Seconds 5; $elapsed += 5
        $pbiToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
        $pbiH = @{ "Authorization" = "Bearer $pbiToken" }
        $poll = Invoke-RestMethod -Uri "$pbiBase/groups/$WsId/reports/$ReportId/exports/$exportId" -Headers $pbiH
        if ($poll.status -in @("Succeeded","Failed")) { $exportStatus = $poll.status; break }
    }
    if ($exportStatus -ne "Succeeded") {
        Write-Warning "    Export did not succeed for $ReportName (status=$exportStatus)"
        return @()
    }

    # Download the ZIP / multi-page PNG archive
    $zipPath = Join-Path $OutputDir "${ReportName}_export.zip"
    $pbiToken = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
    $pbiH = @{ "Authorization" = "Bearer $pbiToken" }
    Invoke-WebRequest -Uri "$pbiBase/groups/$WsId/reports/$ReportId/exports/$exportId/file" `
        -Headers $pbiH -OutFile $zipPath -UseBasicParsing

    # Unzip — wipe the extract dir first so stale named PNGs from prior runs
    # don't mix with the newly-extracted hash-named files.
    $extractDir = Join-Path $OutputDir $ReportName
    if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force
    Remove-Item $zipPath -Force

    # Rename extracted PNGs to page display name
    $pngFiles = Get-ChildItem $extractDir -Filter "*.png" | Sort-Object Name
    $results = @()
    for ($i = 0; $i -lt $pngFiles.Count; $i++) {
        $pageLabel = if ($i -lt $pages.Count) { $pages[$i].DisplayName -replace '[\\/:*?"<>|]','_' } else { "Page$($i+1)" }
        $target = Join-Path $extractDir "${pageLabel}.png"
        Rename-Item $pngFiles[$i].FullName $target -Force
        $results += $target
    }
    return $results
}

# ── Visual-overlap and empty-data checker (pixel-based) ──────────────────
function Test-ReportScreenshot {
    param([string]$PngPath, [string]$PageLabel)
    Add-Type -AssemblyName System.Drawing
    $bmp = [System.Drawing.Bitmap]::FromFile($PngPath)
    $w = $bmp.Width; $h = $bmp.Height
    $totalPx = $w * $h

    # Count white/near-white pixels (background) vs. coloured pixels (data ink)
    $whitePx = 0; $colourPx = 0
    $sampleStep = [math]::Max(1, [int]($totalPx / 50000))   # sample ~50k pixels for speed
    for ($y = 0; $y -lt $h; $y += $sampleStep) {
        for ($x = 0; $x -lt $w; $x += $sampleStep) {
            $c = $bmp.GetPixel($x, $y)
            if ($c.R -gt 240 -and $c.G -gt 240 -and $c.B -gt 240) { $whitePx++ } else { $colourPx++ }
        }
    }
    $bmp.Dispose()
    $inkRatio = if (($whitePx + $colourPx) -gt 0) { [math]::Round($colourPx / ($whitePx + $colourPx) * 100, 1) } else { 0 }

    # --- Overlap detection: look at a 10×10 grid of regions; flag regions
    #     that contain near-identical solid blocks (simplified heuristic)
    $bmp2 = [System.Drawing.Bitmap]::FromFile($PngPath)
    $gridSize = 10; $regionW = [int]($w / $gridSize); $regionH = [int]($h / $gridSize)
    $overlapWarning = $false
    if ($regionW -gt 0 -and $regionH -gt 0) {
        $regionColors = @{}
        for ($row = 0; $row -lt $gridSize; $row++) {
            for ($col = 0; $col -lt $gridSize; $col++) {
                $cx = $col * $regionW + [int]($regionW / 2)
                $cy = $row * $regionH + [int]($regionH / 2)
                if ($cx -lt $w -and $cy -lt $h) {
                    $col_rgb = $bmp2.GetPixel($cx,$cy)
                    $key = "$($col_rgb.R).$($col_rgb.G).$($col_rgb.B)"
                    if ($regionColors[$key]) { $regionColors[$key]++ } else { $regionColors[$key] = 1 }
                }
            }
        }
        # If more than 60% of grid regions share one exact colour it may be a full-bleed blank/overlap
        $maxCount = ($regionColors.Values | Measure-Object -Maximum).Maximum
        if ($maxCount -gt [int]($gridSize * $gridSize * 0.65)) { $overlapWarning = $true }
    }
    $bmp2.Dispose()

    $status = "OK"
    $notes  = "ink=$inkRatio%"
    if ($inkRatio -lt 3) { $status = "WARN"; $notes += " [possible empty/no-data]" }
    if ($overlapWarning) { $status = "WARN"; $notes += " [possible visual overlap or blank fill]" }

    return [PSCustomObject]@{ Page=$PageLabel; Status=$status; Notes=$notes; Path=$PngPath }
}

$totalSteps = if ($SkipDeploy) { 0 } else { 10 }
if ($TriggerPipeline) { $totalSteps++ }
if ($Autoplay) { $totalSteps++ }
$tokens = @{}
$script:_pipelineSucceeded = $false
$finalItems = $null

if ($SkipDeploy) {
    Write-Host "`n[SKIP DEPLOY] Loading existing workspace state..." -ForegroundColor Yellow
    Write-Host ("-" * 60) -ForegroundColor DarkGray
    $finalItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
    Write-Host "  Found $($finalItems.Count) items in workspace." -ForegroundColor Gray
}

# ======================================================================
# Step 0 (optional): Clean workspace
# ======================================================================
if (-not $SkipDeploy -and $Clean) {
    Write-Host "`n[CLEAN] Deleting all items in workspace $WorkspaceId ..." -ForegroundColor Red
    Write-Host ("-" * 60) -ForegroundColor DarkGray
    $allItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
    # Delete in dependency order: Reports first, then Pipelines, Notebooks, SemanticModels, Lakehouses last
    $deleteOrder = @("Report", "DataPipeline", "UserDataFunction", "Notebook", "SemanticModel", "Eventhouse", "KQLDatabase", "SQLDatabase", "Lakehouse")
    foreach ($itemType in $deleteOrder) {
        $items = $allItems | Where-Object { $_.type -eq $itemType }
        foreach ($item in $items) {
            try {
                Invoke-FabricRaw -Method DELETE -Uri "$FabricBase/workspaces/$WorkspaceId/items/$($item.id)" | Out-Null
                Write-Host "  Deleted $($item.type): $($item.displayName)" -ForegroundColor DarkYellow
            } catch {
                Write-Warning "  Could not delete $($item.displayName): $_"
            }
        }
    }
    # Delete remaining items not in the explicit order (SQLEndpoints are auto-deleted with lakehouses)
    $allItems2 = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
    foreach ($item in $allItems2) {
        try {
            Invoke-FabricRaw -Method DELETE -Uri "$FabricBase/workspaces/$WorkspaceId/items/$($item.id)" | Out-Null
            Write-Host "  Deleted $($item.type): $($item.displayName)" -ForegroundColor DarkYellow
        } catch {
            # Some items can't be deleted directly (e.g. SQL Endpoints), skip them
        }
    }
    Write-Host "  Workspace cleaned." -ForegroundColor Green
    # Also delete folders
    try {
        $folders = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/folders").value
        foreach ($f in $folders) {
            try {
                Invoke-FabricRaw -Method DELETE -Uri "$FabricBase/workspaces/$WorkspaceId/folders/$($f.id)" | Out-Null
                Write-Host "  Deleted folder: $($f.displayName)" -ForegroundColor DarkYellow
            } catch {}
        }
    } catch {}
    Write-Host "  Waiting 30s for backend to release item names..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 30
}

# ======================================================================
# Create workspace folders for organization
# ======================================================================
if (-not $SkipDeploy) {
Write-Host "`n[FOLDERS] Organizing workspace into folders..." -ForegroundColor Cyan
Write-Host ("-" * 60) -ForegroundColor DarkGray

$folderIds = @{}
$folderUri = "$FabricBase/workspaces/$WorkspaceId/folders"
foreach ($folderName in @("01 Data", "02 Transform", "03 Analytics", "04 Writeback")) {
    try {
        # Check if folder exists
        $existingFolders = (Invoke-Fabric -Uri $folderUri).value
        $existing = $existingFolders | Where-Object { $_.displayName -eq $folderName }
        if ($existing) {
            $folderIds[$folderName] = $existing.id
            Write-Host "  Folder exists: $folderName = $($existing.id)" -ForegroundColor Yellow
        } else {
            $resp = Invoke-Fabric -Method POST -Uri $folderUri -Body @{ displayName = $folderName }
            if ($resp -and $resp.id) {
                $folderIds[$folderName] = $resp.id
                Write-Host "  Created folder: $folderName = $($resp.id)" -ForegroundColor Green
            }
        }
    } catch {
        Write-Warning "  Could not create folder $folderName - items will be at workspace root."
    }
}

function Move-ToFolder {
    param([string]$ItemId, [string]$FolderName)
    if ($folderIds.ContainsKey($FolderName) -and $folderIds[$FolderName]) {
        for ($retry = 1; $retry -le 3; $retry++) {
            try {
                Invoke-FabricRaw -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items/$ItemId/move" -Body @{ targetFolderId = $folderIds[$FolderName] } | Out-Null
                return $true
            } catch {
                $errDetail = $_.ToString()
                try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
                if ($errDetail -match "429|TooManyRequests|Throttl" -or [string]::IsNullOrWhiteSpace($errDetail)) {
                    $wait = $retry * 10
                    Write-Host "    Throttled, waiting ${wait}s (attempt $retry/3)..." -ForegroundColor DarkGray
                    Start-Sleep -Seconds $wait
                } else {
                    Write-Host "    Move failed: $errDetail" -ForegroundColor DarkYellow
                    return $false
                }
            }
        }
        Write-Host "    Move failed after 3 retries" -ForegroundColor DarkYellow
        return $false
    }
    return $false
}

# ======================================================================
# Step 1: Create Lakehouses
# ======================================================================
Write-Step -N 1 -T $totalSteps -Msg "Creating Lakehouses..."
$lhUri = "$FabricBase/workspaces/$WorkspaceId/lakehouses"
foreach ($lh in @("BronzeLH", "SilverLH", "GoldLH")) {
    $created = $false
    # SilverLH and GoldLH need schema support so sub-folder Delta writes (Tables/schema/table)
    # are registered as SQL endpoint schemas.  BronzeLH uses flat staging tables.
    $lhBody = if ($lh -in @("SilverLH", "GoldLH")) {
        @{ displayName = $lh; creationPayload = @{ enableSchemas = $true } }
    } else {
        @{ displayName = $lh }
    }
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        try {
            $resp = Invoke-Fabric -Method POST -Uri $lhUri -Body $lhBody
            if ($resp -and $resp.id) {
                $tokens[$lh] = $resp.id
                Write-Host "  Created $lh = $($resp.id)" -ForegroundColor Green
                $created = $true
            }
            break
        } catch {
            $errDetail = $_.ToString()
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $errDetail = $sr.ReadToEnd(); $sr.Close()
            } catch {}
            if ($errDetail -match "NotAvailableYet|isRetriable") {
                Write-Host "  $lh name not yet available, retrying in 15s ($attempt/6)..." -ForegroundColor DarkGray
                Start-Sleep -Seconds 15
            } elseif ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
                Write-Host "  $lh already exists, looking up..." -ForegroundColor Yellow
                break
            } else {
                Write-Warning "  Failed to create $lh : $errDetail"
                break
            }
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

# Create Fabric SQL Database for writeback (translytical)
$sqlDbName = "${Company}WritebackDB"
$sqlDbSetupSql = Join-Path (Join-Path (Join-Path $OutputRoot "Writeback") "sqldb") "setup_writeback.sql"
$sqlDbCreated = $false
if (Test-Path $sqlDbSetupSql) {
    Write-Host "  Creating SQL Database: $sqlDbName..." -ForegroundColor Cyan
    try {
        $existingItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
        $existingSqlDb = $existingItems | Where-Object { $_.displayName -eq $sqlDbName -and $_.type -eq "SQLDatabase" } | Select-Object -First 1
        if ($existingSqlDb) {
            $tokens["SQLDB_ID"] = $existingSqlDb.id
            Write-Host "  SQL Database exists: $sqlDbName = $($existingSqlDb.id)" -ForegroundColor Yellow
            $sqlDbCreated = $true
        } else {
            Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/sqlDatabases" -Body @{ displayName = $sqlDbName; description = "Writeback SQL Database for Power BI translytical scenarios" } | Out-Null
            # Look up by name after LRO completes
            $lookup = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value |
                Where-Object { $_.displayName -eq $sqlDbName -and $_.type -eq "SQLDatabase" } |
                Select-Object -First 1
            if ($lookup) {
                $tokens["SQLDB_ID"] = $lookup.id
                Write-Host "  Created SQL Database: $sqlDbName = $($lookup.id)" -ForegroundColor Green
                $sqlDbCreated = $true
            } else {
                Write-Warning "  SQL Database created but could not find it by name."
            }
        }
    } catch {
        $errDetail = $_.ToString()
        try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
        Write-Warning "  SQL Database creation failed: $errDetail"
    }

    # Get SQL Database properties (server FQDN and database name) for TMDL tokens
    if ($sqlDbCreated -and $tokens.ContainsKey("SQLDB_ID")) {
        try {
            $sqlDbProps = Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/sqlDatabases/$($tokens['SQLDB_ID'])"
            if ($sqlDbProps -and $sqlDbProps.properties) {
                $tokens["SQLDB_SERVER"] = $sqlDbProps.properties.serverFqdn
                $tokens["SQLDB_NAME"] = $sqlDbProps.properties.databaseName
                Write-Host "  SQL Server: $($tokens['SQLDB_SERVER'])" -ForegroundColor Gray
                Write-Host "  SQL Database: $($tokens['SQLDB_NAME'])" -ForegroundColor Gray
            }
        } catch {
            Write-Warning "  Could not retrieve SQL Database properties."
        }
    }
} else {
    Write-Host "  No writeback SQL setup found, skipping SQL Database." -ForegroundColor DarkGray
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
            trident = @{
                lakehouse = @{
                    default_lakehouse              = $tokens["BronzeLH"]
                    default_lakehouse_name         = "BronzeLH"
                    default_lakehouse_workspace_id = $WorkspaceId
                    known_lakehouses = @(
                        @{ id = $tokens["BronzeLH"] }
                        @{ id = $tokens["SilverLH"] }
                        @{ id = $tokens["GoldLH"]   }
                    )
                }
            }
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
# Step 3b: Execute SQL Database DDL for writeback
# ======================================================================
if ($sqlDbCreated -and (Test-Path $sqlDbSetupSql) -and $tokens.ContainsKey("SQLDB_SERVER")) {
    Write-Host "`n  Executing writeback DDL on SQL Database..." -ForegroundColor Cyan
    $sqlDdl = Get-Content $sqlDbSetupSql -Raw -Encoding UTF8
    # Split on GO statements
    $batches = $sqlDdl -split "(?m)^\s*GO\s*$" | Where-Object { $_.Trim() }
    $sqlToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
    $batchOk = 0; $batchFail = 0
    foreach ($batch in $batches) {
        $trimmed = $batch.Trim()
        if (-not $trimmed) { continue }
        try {
            Invoke-Sqlcmd -ServerInstance $tokens["SQLDB_SERVER"] -Database $tokens["SQLDB_NAME"] -AccessToken $sqlToken -Query $trimmed -ErrorAction Stop
            $batchOk++
        } catch {
            $batchFail++
            Write-Host "    DDL batch failed: $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
    }
    Write-Host "  SQL DDL executed: $batchOk OK, $batchFail failed" -ForegroundColor $(if ($batchFail -gt 0) { "Yellow" } else { "Green" })
} elseif ($sqlDbCreated) {
    Write-Host "`n  SQL Database created but properties not available yet." -ForegroundColor DarkGray
    Write-Host "  Run the 09_SQLDatabaseSetup notebook manually to create writeback schema." -ForegroundColor DarkGray
}

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
# Look up SQL endpoint connection string for DirectLake binding
if (-not $tokens.ContainsKey("SQL_ENDPOINT")) {
    $goldLhProps = Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/lakehouses/$($tokens['GoldLH'])"
    $tokens["SQL_ENDPOINT"] = $goldLhProps.properties.sqlEndpointProperties.connectionString
    $tokens["SQL_ENDPOINT_ID"] = $goldLhProps.properties.sqlEndpointProperties.id
    Write-Host "  SQL Endpoint: $($tokens['SQL_ENDPOINT'])" -ForegroundColor DarkGray
    Write-Host "  SQL Endpoint ID: $($tokens['SQL_ENDPOINT_ID'])" -ForegroundColor DarkGray
}
# Replace tokens in model
$modelTmdl = $modelTmdl -replace "\{\{GOLD_LH_ID\}\}", $tokens["GoldLH"]
$modelTmdl = $modelTmdl -replace "\{\{SQL_ENDPOINT\}\}", $tokens["SQL_ENDPOINT"]
$modelTmdl = $modelTmdl -replace "\{\{SQL_ENDPOINT_ID\}\}", $tokens["SQL_ENDPOINT_ID"]
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

# Check if SM already exists
$preItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
$existingSM = $preItems | Where-Object { $_.displayName -eq "${Company}Model" -and $_.type -eq "SemanticModel" } | Select-Object -First 1
if ($existingSM) {
    Write-Host "  SemanticModel already exists: $($existingSM.id)" -ForegroundColor Yellow
} else {
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
}
# Look up SM ID
$allItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
$smItem = $allItems | Where-Object { $_.displayName -eq "${Company}Model" -and $_.type -eq "SemanticModel" } | Select-Object -First 1
if ($smItem) {
    $smId = $smItem.id
    Write-Host "  SemanticModel ID: $smId" -ForegroundColor Green
    $tokens["SEMANTIC_MODEL_ID"] = $smId
} else {
    $smId = $null
    Write-Warning "  SemanticModel not found -- reports will skip SM binding."
}

# Deploy Writeback Semantic Model (DirectQuery to SQL Database) if it exists
$wbSmDir = Join-Path $OutputRoot "${Company}WritebackModel.SemanticModel"
if ((Test-Path $wbSmDir) -and $tokens.ContainsKey("SQLDB_SERVER")) {
    Write-Host "`n  Deploying Writeback SemanticModel (DirectQuery)..." -ForegroundColor Cyan
    $wbDefDir = Join-Path $wbSmDir "definition"
    $wbParts = @()

    # definition.pbism
    $wbPbism = Get-Content (Join-Path $wbSmDir "definition.pbism") -Raw -Encoding UTF8
    $wbParts += @{ path = "definition.pbism"; payload = (To-Base64 $wbPbism); payloadType = "InlineBase64" }

    # model.tmdl with token replacement
    $wbModelTmdl = Get-Content (Join-Path $wbDefDir "model.tmdl") -Raw -Encoding UTF8
    $wbModelTmdl = $wbModelTmdl -replace "\{\{SQLDB_SERVER\}\}", $tokens["SQLDB_SERVER"]
    $wbModelTmdl = $wbModelTmdl -replace "\{\{SQLDB_NAME\}\}", $tokens["SQLDB_NAME"]
    $wbParts += @{ path = "definition/model.tmdl"; payload = (To-Base64 $wbModelTmdl); payloadType = "InlineBase64" }

    # Writeback table TMDL files
    $wbTableDir = Join-Path $wbDefDir "tables"
    if (Test-Path $wbTableDir) {
        foreach ($tf in (Get-ChildItem $wbTableDir -Filter "*.tmdl")) {
            $content = Get-Content $tf.FullName -Raw -Encoding UTF8
            $wbParts += @{ path = "definition/tables/$($tf.Name)"; payload = (To-Base64 $content); payloadType = "InlineBase64" }
        }
    }

    $wbSmBody = @{
        displayName = "${Company}WritebackModel"
        type = "SemanticModel"
        definition = @{
            format = "TMDL"
            parts = $wbParts
        }
    }

    $wbSmId = $null
    $existingWbSM = $allItems | Where-Object { $_.displayName -eq "${Company}WritebackModel" -and $_.type -eq "SemanticModel" } | Select-Object -First 1
    if ($existingWbSM) {
        Write-Host "  WritebackModel already exists: $($existingWbSM.id)" -ForegroundColor Yellow
        $wbSmId = $existingWbSM.id
    } else {
        try {
            Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Body $wbSmBody | Out-Null
            # Look up created item by name (LRO result does not include id)
            $wbItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
            $wbSmItem = $wbItems | Where-Object { $_.displayName -eq "${Company}WritebackModel" -and $_.type -eq "SemanticModel" } | Select-Object -First 1
            if ($wbSmItem) {
                $wbSmId = $wbSmItem.id
                Write-Host "  Created WritebackModel: ${Company}WritebackModel" -ForegroundColor Green
            } else {
                Write-Warning "  WritebackModel created but ID not found in items list."
            }
        } catch {
            $errDetail = $_.ToString()
            try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
            Write-Warning "  WritebackModel deploy failed: $errDetail"
        }
    }
    if ($wbSmId) {
        $tokens["WRITEBACK_MODEL_ID"] = $wbSmId
        Write-Host "  WritebackModel ID: $wbSmId" -ForegroundColor Green
    }
}

# ======================================================================
# Step 5: Deploy User Data Function (writeback API bridge)
# ======================================================================
$udfDir = Join-Path $OutputRoot "UserDataFunction"
if ((Test-Path $udfDir) -and $sqlDbCreated -and $tokens.ContainsKey("SQLDB_ID")) {
    Write-Step -N 5 -T $totalSteps -Msg "Deploying User Data Function..."

    $udfName = "${Company}WritebackUDF"

    # Step A: Create empty UDF item
    $udfId = $null
    $existingUdf = $allItems | Where-Object { $_.displayName -eq $udfName -and $_.type -eq "UserDataFunction" } | Select-Object -First 1
    if ($existingUdf) {
        $udfId = $existingUdf.id
        Write-Host "  UDF already exists: $udfName = $udfId" -ForegroundColor Yellow
    } else {
        try {
            $createResp = Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/userDataFunctions" -Body @{
                displayName = $udfName
                description = "Writeback API bridge between Power BI and SQL Database"
            }
            # Look up by name
            $lookup = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value |
                Where-Object { $_.displayName -eq $udfName -and $_.type -eq "UserDataFunction" } |
                Select-Object -First 1
            if ($lookup) {
                $udfId = $lookup.id
                Write-Host "  Created UDF item: $udfName = $udfId" -ForegroundColor Green
            }
        } catch {
            $errDetail = $_.ToString()
            try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
            if ($errDetail -match "already exists|ItemDisplayNameAlreadyInUse|NameAlreadyExists") {
                Write-Host "  $udfName already exists, looking up..." -ForegroundColor Yellow
                $lookup = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value |
                    Where-Object { $_.displayName -eq $udfName -and $_.type -eq "UserDataFunction" } |
                    Select-Object -First 1
                if ($lookup) { $udfId = $lookup.id }
            } else {
                Write-Warning "  UDF item creation failed: $errDetail"
            }
        }
    }

    # Step B: Update definition with writeback functions
    if ($udfId) {
        try {
            Write-Host "  Updating UDF definition with writeback functions..." -ForegroundColor DarkGray

            $connectionAlias = "WritebackDB"

            # Load and resolve tokens in definition.json
            $udfDefJson = Get-Content (Join-Path $udfDir "definition.json") -Raw -Encoding UTF8
            $udfDefJson = $udfDefJson -replace "\{\{SQLDB_ID\}\}", $tokens["SQLDB_ID"]
            $udfDefJson = $udfDefJson -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId

            # Load and resolve tokens in functions.json
            $udfFuncJson = Get-Content (Join-Path (Join-Path $udfDir "resources") "functions.json") -Raw -Encoding UTF8
            $udfFuncJson = $udfFuncJson -replace "\{\{CONNECTION_ALIAS\}\}", $connectionAlias

            # Load function_app.py
            $udfAppPy = Get-Content (Join-Path $udfDir "function_app.py") -Raw -Encoding UTF8

            # Build update definition parts — Fabric API uses ".resources/" prefix (dot-prefixed)
            $updParts = @(
                @{ path = "definition.json"; payload = (To-Base64 $udfDefJson); payloadType = "InlineBase64" }
                @{ path = ".resources/functions.json"; payload = (To-Base64 $udfFuncJson); payloadType = "InlineBase64" }
                @{ path = "function_app.py"; payload = (To-Base64 $udfAppPy); payloadType = "InlineBase64" }
            )

            # Push definition update (do NOT use updateMetadata=True — it requires a .platform file)
            Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/userDataFunctions/$udfId/updateDefinition" -Body @{
                definition = @{ parts = $updParts }
            } | Out-Null
            Write-Host "  Updated UDF definition with writeback functions" -ForegroundColor Green
        } catch {
            $errDetail = $_.ToString()
            try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
            Write-Warning "  UDF definition update failed: $errDetail"
        }
    }
} else {
    Write-Step -N 5 -T $totalSteps -Msg "Skipping User Data Function (no SQL Database or UDF output)"
}

# ======================================================================
# Step 6: Deploy Reports (PBIR)
# ======================================================================
Write-Step -N 6 -T $totalSteps -Msg "Deploying Power BI Reports..."

$reportDirs = @(
    @{ Name = "${Company}-Analytics"; Dir = "${Company}-Analytics.Report" }
    @{ Name = "${Company}-Forecasting"; Dir = "${Company}-Forecasting.Report" }
    @{ Name = "${Company}-HTAP"; Dir = "${Company}-HTAP.Report" }
    @{ Name = "${Company}-Pipeline"; Dir = "${Company}-Pipeline.Report" }
    @{ Name = "${Company}-Writeback"; Dir = "${Company}-Writeback.Report" }
)

foreach ($reportInfo in $reportDirs) {
    $rptRoot = Join-Path $OutputRoot $reportInfo.Dir
    $rptDir = Join-Path $rptRoot "definition"
    if (-not (Test-Path $rptDir)) {
        Write-Warning "  Report dir not found: $rptDir"
        continue
    }

    $rptParts = @()

    # definition.pbir at .Report/ root
    $pbirFile = Join-Path $rptRoot "definition.pbir"
    if (Test-Path $pbirFile) {
        $content = Get-Content $pbirFile -Raw -Encoding UTF8
        if ($smId) { $content = $content -replace "\{\{SEMANTIC_MODEL_ID\}\}", $smId }
        if ($wbSmId) { $content = $content -replace "\{\{WRITEBACK_MODEL_ID\}\}", $wbSmId }
        $rptParts += @{ path = "definition.pbir"; payload = (To-Base64 $content); payloadType = "InlineBase64" }
    }

    # Recursively collect all files under definition/
    $allFiles = Get-ChildItem $rptDir -Recurse -File
    foreach ($f in $allFiles) {
        $relPath = $f.FullName.Substring($rptRoot.Length + 1).Replace("\", "/")
        $content = Get-Content $f.FullName -Raw -Encoding UTF8
        # Replace SM reference if needed
        if ($smId) {
            $content = $content -replace "\{\{SEMANTIC_MODEL_ID\}\}", $smId
        }
        if ($wbSmId) {
            $content = $content -replace "\{\{WRITEBACK_MODEL_ID\}\}", $wbSmId
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
# Step 7: Deploy Pipeline
# ======================================================================
Write-Step -N 7 -T $totalSteps -Msg "Deploying Data Pipeline..."
$pipelineJson = Get-Content (Join-Path (Join-Path $OutputRoot "Pipeline") "pipeline-content.json") -Raw -Encoding UTF8
# Resolve all known tokens
$pipelineJson = $pipelineJson -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
foreach ($key in $tokens.Keys) {
    $pipelineJson = $pipelineJson -replace "\{\{$key\}\}", $tokens[$key]
}
foreach ($key in $nbTokens.Keys) {
    $tokenName = $key -replace "^(\d+)_.*", 'NB$1_ID'
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
# Step 8: Deploy Dataflow Gen2 definitions
# ======================================================================
Write-Step -N 8 -T $totalSteps -Msg "Deploying Dataflow Gen2 definitions..."
$dfDir = Join-Path $OutputRoot "Dataflows"

# Upload JSON configs + .pq mashup files to GoldLH for reference and manual import
$goldId = $tokens["GoldLH"]
$dfAllFiles = Get-ChildItem $dfDir -File | Where-Object { $_.Extension -in @(".json", ".pq") }
foreach ($df in $dfAllFiles) {
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
Write-Host "  Dataflow definitions: $($dfAllFiles.Count) files (JSON configs + Power Query M)" -ForegroundColor Green
Write-Host "  Note: DataflowGen2 items can be imported in Fabric portal from GoldLH/Files/Dataflows/" -ForegroundColor DarkGray

# ======================================================================
# Step 9: Organize items into folders
# ======================================================================
Write-Step -N 9 -T $totalSteps -Msg "Organizing items into workspace folders..."

$finalAll = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
$movedCount = 0
$failCount = 0

# Data folder: Lakehouses (SQLEndpoints auto-move with parent)
foreach ($item in ($finalAll | Where-Object { $_.type -eq "Lakehouse" })) {
    Write-Host "  Moving $($item.type): $($item.displayName) -> 01 Data" -ForegroundColor DarkGray
    if (Move-ToFolder -ItemId $item.id -FolderName "01 Data") { $movedCount++ } else { $failCount++ }
    Start-Sleep -Seconds 1
}

# Transform folder: Notebooks + Pipelines + Dataflows
foreach ($item in ($finalAll | Where-Object { $_.type -in @("Notebook", "DataPipeline", "DataflowGen2", "Dataflow") })) {
    Write-Host "  Moving $($item.type): $($item.displayName) -> 02 Transform" -ForegroundColor DarkGray
    if (Move-ToFolder -ItemId $item.id -FolderName "02 Transform") { $movedCount++ } else { $failCount++ }
    Start-Sleep -Seconds 1
}

# Analytics folder: SemanticModel + Reports (except WritebackModel / Writeback report)
foreach ($item in ($finalAll | Where-Object { $_.type -in @("SemanticModel", "Report") -and $_.displayName -notlike "*WritebackModel*" -and $_.displayName -notlike "*-Writeback" })) {
    Write-Host "  Moving $($item.type): $($item.displayName) -> 03 Analytics" -ForegroundColor DarkGray
    if (Move-ToFolder -ItemId $item.id -FolderName "03 Analytics") { $movedCount++ } else { $failCount++ }
    Start-Sleep -Seconds 1
}

# Writeback folder: SQLDatabase + UserDataFunction + WritebackModel + Writeback report
foreach ($item in ($finalAll | Where-Object { $_.type -in @("SQLDatabase", "UserDataFunction") -or ($_.type -eq "SemanticModel" -and $_.displayName -like "*WritebackModel*") -or ($_.type -eq "Report" -and $_.displayName -like "*-Writeback") })) {
    Write-Host "  Moving $($item.type): $($item.displayName) -> 04 Writeback" -ForegroundColor DarkGray
    if (Move-ToFolder -ItemId $item.id -FolderName "04 Writeback") { $movedCount++ } else { $failCount++ }
    Start-Sleep -Seconds 1
}

Write-Host "  Moved $movedCount items into folders ($failCount failed)." -ForegroundColor $(if ($failCount -gt 0) { "Yellow" } else { "Green" })

# ======================================================================
# Step 10: Summary
# ======================================================================
Write-Step -N 10 -T $totalSteps -Msg "Deployment Summary"

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

} # end if (-not $SkipDeploy) — deploy block

# Ensure $finalItems is always populated before Steps 11/12
if (-not $finalItems) {
    $finalItems = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
}

# ======================================================================
# Step 11 (optional): Trigger ETL pipeline run
# ======================================================================
if ($TriggerPipeline) {
    $stepNum = if ($SkipDeploy) { 1 } else { 11 }
    Write-Step -N $stepNum -T $totalSteps -Msg "Triggering ETL Pipeline run..."

    $pipelineName = "${Company}-ETL"
    $pipelineItem = $finalItems | Where-Object { $_.displayName -eq $pipelineName -and $_.type -eq "DataPipeline" } | Select-Object -First 1

    if (-not $pipelineItem) {
        Write-Warning "  Pipeline '$pipelineName' not found in workspace -- cannot trigger."
    } else {
        $pipelineId = $pipelineItem.id
        Write-Host "  Pipeline: $pipelineName ($pipelineId)" -ForegroundColor Gray

        try {
            # POST to trigger a pipeline run (jobType=Pipeline)
            $runResp = Invoke-FabricRaw -Method POST `
                -Uri "$FabricBase/workspaces/$WorkspaceId/dataPipelines/$pipelineId/jobs/instances?jobType=Pipeline"

            # Fabric returns 202 with Location header pointing to the job instance
            $jobLocation = $runResp.Headers["Location"]
            if (-not $jobLocation) {
                # Some tenants return 200 with body
                $jobBody = $runResp.Content | ConvertFrom-Json
                $jobInstanceId = $jobBody.id
                $jobLocation = "$FabricBase/workspaces/$WorkspaceId/dataPipelines/$pipelineId/jobs/instances/$jobInstanceId"
            }

            Write-Host "  Pipeline run triggered. Polling for completion..." -ForegroundColor Green
            Write-Host "  Job URL: $jobLocation" -ForegroundColor DarkGray

            # Poll job status
            $maxPollMinutes = 60
            $pollInterval  = 15
            $elapsed       = 0
            $finalStatus   = $null

            while ($elapsed -lt ($maxPollMinutes * 60)) {
                Start-Sleep -Seconds $pollInterval
                $elapsed += $pollInterval

                $h        = Get-Headers
                $pollResp = Invoke-WebRequest -Uri $jobLocation -Headers $h -UseBasicParsing
                $jobState = $pollResp.Content | ConvertFrom-Json

                $status   = $jobState.status
                $elapsed_min = [math]::Round($elapsed / 60, 1)
                Write-Host "    Status: $status (${elapsed_min}m elapsed)" -ForegroundColor DarkGray

                if ($status -in @("Succeeded", "Completed")) {
                    $finalStatus = "Succeeded"
                    break
                } elseif ($status -in @("Failed", "Cancelled", "Deduped")) {
                    $finalStatus = $status
                    break
                }
            }

            if ($finalStatus -eq "Succeeded") {
                Write-Host "  Pipeline run SUCCEEDED." -ForegroundColor Green
                Write-Host "  Bronze -> Silver -> Gold data is now populated." -ForegroundColor Green
                if ($Autoplay) { $script:_pipelineSucceeded = $true }
            } elseif ($finalStatus) {
                Write-Warning "  Pipeline run ended with status: $finalStatus"
            } else {
                Write-Warning "  Pipeline run did not complete within ${maxPollMinutes} minutes. Check Fabric portal for status."
                Write-Host "  Job URL: $jobLocation" -ForegroundColor DarkGray
            }

        } catch {
            $errDetail = $_.ToString()
            try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
            Write-Warning "  Failed to trigger pipeline: $errDetail"
        }
    }
}

# ======================================================================
# Step 12 (optional): Autoplay — screenshot every report page & verify
# ======================================================================
if ($Autoplay -and (-not $TriggerPipeline -or $script:_pipelineSucceeded)) {
    $stepNum = if ($SkipDeploy -and -not $TriggerPipeline) { 1 } elseif ($SkipDeploy) { 2 } else { 12 }
    Write-Step -N $stepNum -T $totalSteps -Msg "Autoplay: exporting report screenshots..."

    $screenshotRoot = Join-Path $OutputRoot "screenshots"
    New-Item -ItemType Directory -Path $screenshotRoot -Force | Out-Null

    # Refresh the semantic model so reports render with freshly-loaded Gold data.
    # DirectLake models do NOT auto-refresh after a pipeline run, so exported
    # pages would otherwise come back as blank white images.
    $smItem = ($finalItems | Where-Object { $_.displayName -eq "${Company}Model" -and $_.type -eq "SemanticModel" } | Select-Object -First 1)
    if ($smItem) {
        Write-Host "  Refreshing semantic model: ${Company}Model..." -ForegroundColor Cyan
        try {
            # Power BI enhanced refresh (type=Full) — works for DirectLake models in Fabric.
            $pbiTok = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
            $pbiH   = @{ "Authorization" = "Bearer $pbiTok" }
            $refreshResp = Invoke-WebRequest -Method POST `
                -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/datasets/$($smItem.id)/refreshes" `
                -Headers $pbiH -Body '{"type":"Full"}' -ContentType "application/json" -UseBasicParsing
            $refreshUrl = $refreshResp.Headers["Location"]
            if ($refreshUrl) {
                # Poll until Completed / Failed (up to 5 min)
                $refreshDone = $false
                for ($ri = 0; $ri -lt 30; $ri++) {
                    Start-Sleep -Seconds 10
                    $pbiTok = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api").Token
                    $pbiH   = @{ "Authorization" = "Bearer $pbiTok" }
                    $rfStatus = (Invoke-RestMethod -Uri $refreshUrl -Headers $pbiH).status
                    Write-Host "  Refresh status: $rfStatus ($($ri*10 + 10)s)" -ForegroundColor DarkGray
                    if ($rfStatus -in @("Completed","Failed","Unknown")) {
                        if ($rfStatus -eq "Completed") { Write-Host "  Semantic model refresh completed." -ForegroundColor Green }
                        else { Write-Warning "  Refresh ended with status: $rfStatus — screenshots may still be blank." }
                        $refreshDone = $true; break
                    }
                }
                if (-not $refreshDone) { Write-Warning "  Refresh poll timed out — proceeding anyway." }
            }
        } catch {
            Write-Warning "  Could not refresh semantic model: $_ — proceeding anyway."
        }
    } else {
        Write-Warning "  Semantic model '${Company}Model' not found — screenshots may be blank."
    }

    # Collect all deployed reports — deduplicate by displayName (workspace may have
    # multiple items with same name from prior non-clean deploys).
    # Pick the last item per group — newest deploy overwrites oldest.
    $deployedReports = ($finalItems | Where-Object { $_.type -eq "Report" -and $_.displayName -like "${Company}-*" } |
        Group-Object displayName | ForEach-Object { $_.Group[-1] })

    $allResults = @()
    foreach ($rpt in $deployedReports) {
        Write-Host "  Exporting: $($rpt.displayName)" -ForegroundColor DarkGray
        $pngs = Invoke-ReportScreenshots -WsId $WorkspaceId -ReportName $rpt.displayName -ReportId $rpt.id -OutputDir $screenshotRoot
        foreach ($png in $pngs) {
            $label = [System.IO.Path]::GetFileNameWithoutExtension($png)
            $chk = Test-ReportScreenshot -PngPath $png -PageLabel "$($rpt.displayName) / $label"
            $colour = if ($chk.Status -eq "OK") { "Green" } else { "Yellow" }
            Write-Host "    [$($chk.Status)] $($chk.Page)  ($($chk.Notes))" -ForegroundColor $colour
            $allResults += $chk
        }
    }

    # Summary table
    Write-Host ""
    Write-Host "  Screenshot check summary:" -ForegroundColor Cyan
    $ok   = @($allResults | Where-Object { $_.Status -eq "OK" }).Count
    $warn = @($allResults | Where-Object { $_.Status -eq "WARN" }).Count
    Write-Host "    OK: $ok   WARN: $warn   Total: $($allResults.Count)" -ForegroundColor $(if ($warn -gt 0) { "Yellow" } else { "Green" })
    Write-Host "  Screenshots saved to: $screenshotRoot" -ForegroundColor Cyan

    # Open screenshot folder in Explorer
    Start-Process explorer.exe $screenshotRoot
}
