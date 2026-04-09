# One-shot: update NB01 + pipeline definitions in Fabric then retrigger
param(
    [string]$WorkspaceId  = "acaeaa28-d3b3-4c64-b18f-56168c64b53e",
    [string]$BronzeLH_ID  = "a81adea9-278e-4022-b155-8432bdd362d1",
    [string]$SilverLH_ID  = "effd5d62-ea15-43d1-9853-38c4452f4369",
    [string]$GoldLH_ID    = "404823af-c239-4293-a191-18a91eb2edb0",
    [string]$PipelineId   = "ad60b274-c0d3-4b6e-82e1-b33061cd837c",
    [string]$OutputRoot   = "output\contoso-energy"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$FabricBase = "https://api.fabric.microsoft.com/v1"

function Get-FH {
    $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    return @{ "Authorization" = "Bearer $t"; "Content-Type" = "application/json" }
}
function To-B64([string]$T) { return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($T)) }

function Invoke-LRO([string]$Method, [string]$Uri, [string]$JsonBody) {
    $resp = Invoke-WebRequest -Method $Method -Uri $Uri -Headers (Get-FH) -Body $JsonBody -ContentType "application/json" -UseBasicParsing
    if ($resp.StatusCode -eq 202) {
        $opUrl = $resp.Headers["Location"]
        $elapsed = 0
        while ($elapsed -lt 300) {
            Start-Sleep -Seconds 10; $elapsed += 10
            $poll = Invoke-WebRequest -Uri $opUrl -Headers (Get-FH) -UseBasicParsing
            $s = ($poll.Content | ConvertFrom-Json).status
            Write-Host "    LRO: $s (${elapsed}s)" -ForegroundColor DarkGray
            if ($s -eq "Succeeded") { return }
            if ($s -in @("Failed","Cancelled")) { throw "LRO $s`: $($poll.Content)" }
        }
        throw "LRO timed out"
    }
}

Write-Host "[1/7] Fetching workspace items..." -ForegroundColor Cyan
$allItems = (Invoke-RestMethod -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Headers (Get-FH)).value
Write-Host "  Found $($allItems.Count) items"

# ── Helper: upload one notebook ────────────────────────────────────────────────
function Update-Notebook([string]$FileName, [string]$StepLabel) {
    Write-Host "`n[$StepLabel] Updating $FileName notebook definition..." -ForegroundColor Cyan
    $nbItem = $allItems | Where-Object { $_.displayName -eq $FileName -and $_.type -eq "Notebook" } | Select-Object -First 1
    if (-not $nbItem) { throw "$FileName not found in workspace" }

    $nbContent = Get-Content "$OutputRoot\notebooks\$FileName.py" -Raw -Encoding UTF8
    $nbContent = $nbContent -replace "\{\{BRONZE_LH_ID\}\}", $BronzeLH_ID
    $nbContent = $nbContent -replace "\{\{SILVER_LH_ID\}\}", $SilverLH_ID
    $nbContent = $nbContent -replace "\{\{GOLD_LH_ID\}\}",   $GoldLH_ID
    $nbContent = $nbContent -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId

    $lines = ($nbContent -split "`n") | ForEach-Object { $_.TrimEnd("`r") + "`n" }
    $ipynb = @{
        nbformat = 4; nbformat_minor = 5
        cells = @(@{
            cell_type = "code"; source = $lines
            execution_count = $null; outputs = @(); metadata = @{}
        })
        metadata = @{
            language_info = @{ name = "python" }
            kernel_info   = @{ name = "synapse_pyspark" }
            trident = @{
                lakehouse = @{
                    default_lakehouse              = $BronzeLH_ID
                    default_lakehouse_name         = "BronzeLH"
                    default_lakehouse_workspace_id = $WorkspaceId
                    known_lakehouses = @(
                        @{ id = $BronzeLH_ID }
                        @{ id = $SilverLH_ID }
                        @{ id = $GoldLH_ID   }
                    )
                }
            }
        }
    }
    $b64 = To-B64 ($ipynb | ConvertTo-Json -Depth 10 -Compress)
    $body = @{
        definition = @{
            format = "ipynb"
            parts  = @(@{ path = "notebook-content.ipynb"; payload = $b64; payloadType = "InlineBase64" })
        }
    } | ConvertTo-Json -Depth 10

    Invoke-LRO -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/notebooks/$($nbItem.id)/updateDefinition" -JsonBody $body
    Write-Host "  $FileName updated OK." -ForegroundColor Green
}

# ── Update NB01 through NB05 ─────────────────────────────────────────────────
Update-Notebook -FileName "01_BronzeToSilver" -StepLabel "2/7"
Update-Notebook -FileName "02_WebEnrichment"  -StepLabel "3/7"
Update-Notebook -FileName "03_SilverToGold"   -StepLabel "4/7"
Update-Notebook -FileName "04_Forecasting"    -StepLabel "5/7"
Update-Notebook -FileName "05_EventSimulator" -StepLabel "6/7"

# ── Update Pipeline ────────────────────────────────────────────────────────────
Write-Host "`n[7/7] Updating ContosoEnergy-ETL pipeline definition..." -ForegroundColor Cyan
$pipelineJson = Get-Content "$OutputRoot\Pipeline\pipeline-content.json" -Raw -Encoding UTF8
$pipelineJson = $pipelineJson -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
$pipelineJson = $pipelineJson -replace "\{\{BronzeLH\}\}", $BronzeLH_ID
$pipelineJson = $pipelineJson -replace "\{\{SilverLH\}\}", $SilverLH_ID
$pipelineJson = $pipelineJson -replace "\{\{GoldLH\}\}",   $GoldLH_ID

foreach ($nb in (Get-ChildItem "$OutputRoot\notebooks" -Filter "*.py" | Sort-Object Name)) {
    $nbName = $nb.BaseName
    $num    = [regex]::Match($nbName, '^\d+').Value.TrimStart('0')
    $token  = "NB0${num}_ID"
    $found  = $allItems | Where-Object { $_.displayName -eq $nbName -and $_.type -eq "Notebook" } | Select-Object -First 1
    if ($found) {
        $pipelineJson = $pipelineJson -replace "\{\{$token\}\}", $found.id
        Write-Host "  $token = $($found.id)" -ForegroundColor DarkGray
    } else {
        Write-Warning "  Notebook '$nbName' not found in workspace - token {{$token}} unresolved"
    }
}

# Check for unresolved tokens
$unresolved = [regex]::Matches($pipelineJson, '\{\{[^}]+\}\}') | ForEach-Object { $_.Value } | Sort-Object -Unique
if ($unresolved) { Write-Warning "  Unresolved tokens: $($unresolved -join ', ')" }

$pipBody = @{
    definition = @{
        parts = @(@{ path = "pipeline-content.json"; payload = (To-B64 $pipelineJson); payloadType = "InlineBase64" })
    }
} | ConvertTo-Json -Depth 10

Invoke-LRO -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/dataPipelines/$PipelineId/updateDefinition" -JsonBody $pipBody
Write-Host "  Pipeline updated OK." -ForegroundColor Green

Write-Host "`nAll items updated. Triggering pipeline run..." -ForegroundColor Cyan

# ── Trigger pipeline ───────────────────────────────────────────────────────────
$trigUri = "$FabricBase/workspaces/$WorkspaceId/dataPipelines/$PipelineId/jobs/instances?jobType=Pipeline"
$trigResp = Invoke-WebRequest -Method POST -Uri $trigUri -Headers (Get-FH) -Body "{}" -ContentType "application/json" -UseBasicParsing

$jobLocation = $trigResp.Headers["Location"]
if (-not $jobLocation) {
    $jobBody = $trigResp.Content | ConvertFrom-Json
    $jobLocation = "$FabricBase/workspaces/$WorkspaceId/dataPipelines/$PipelineId/jobs/instances/$($jobBody.id)"
}
Write-Host "Triggered. Polling: $jobLocation" -ForegroundColor Green

$maxMin = 90; $interval = 20; $elapsed = 0
while ($elapsed -lt ($maxMin * 60)) {
    Start-Sleep -Seconds $interval; $elapsed += $interval
    $poll = Invoke-WebRequest -Uri $jobLocation -Headers (Get-FH) -UseBasicParsing
    $state = ($poll.Content | ConvertFrom-Json).status
    $min = [math]::Round($elapsed / 60, 1)
    Write-Host "  Pipeline: $state (${min}m)" -ForegroundColor $(if ($state -eq "Succeeded") { "Green" } elseif ($state -in @("Failed","Cancelled")) { "Red" } else { "DarkGray" })
    if ($state -eq "Succeeded") { Write-Host "`nPipeline SUCCEEDED. Data fully loaded." -ForegroundColor Green; exit 0 }
    if ($state -in @("Failed","Cancelled","Deduped")) {
        $detail = ($poll.Content | ConvertFrom-Json).failureReason.message
        Write-Warning "Pipeline $state`: $detail"; exit 1
    }
}
Write-Warning "Pipeline did not finish within ${maxMin} min. Check: $jobLocation"
