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
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,
    [string]$Industry = "contoso-energy",
    [switch]$Clean
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

$totalSteps = 10
$tokens = @{}

# ======================================================================
# Step 0 (optional): Clean workspace
# ======================================================================
if ($Clean) {
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
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        try {
            $resp = Invoke-Fabric -Method POST -Uri $lhUri -Body @{ displayName = $lh }
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

    $existingWbSM = $allItems | Where-Object { $_.displayName -eq "${Company}WritebackModel" -and $_.type -eq "SemanticModel" } | Select-Object -First 1
    if ($existingWbSM) {
        Write-Host "  WritebackModel already exists: $($existingWbSM.id)" -ForegroundColor Yellow
    } else {
        try {
            Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Body $wbSmBody | Out-Null
            Write-Host "  Created WritebackModel: ${Company}WritebackModel" -ForegroundColor Green
        } catch {
            $errDetail = $_.ToString()
            try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errDetail = $sr.ReadToEnd(); $sr.Close() } catch {}
            Write-Warning "  WritebackModel deploy failed: $errDetail"
        }
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

# Analytics folder: SemanticModel + Reports (except WritebackModel)
foreach ($item in ($finalAll | Where-Object { $_.type -in @("SemanticModel", "Report") -and $_.displayName -notlike "*WritebackModel*" })) {
    Write-Host "  Moving $($item.type): $($item.displayName) -> 03 Analytics" -ForegroundColor DarkGray
    if (Move-ToFolder -ItemId $item.id -FolderName "03 Analytics") { $movedCount++ } else { $failCount++ }
    Start-Sleep -Seconds 1
}

# Writeback folder: SQLDatabase + UserDataFunction + WritebackModel
foreach ($item in ($finalAll | Where-Object { $_.type -in @("SQLDatabase", "UserDataFunction") -or ($_.type -eq "SemanticModel" -and $_.displayName -like "*WritebackModel*") })) {
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
