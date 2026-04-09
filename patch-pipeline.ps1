param(
    [string]$WorkspaceId = "acaeaa28-d3b3-4c64-b18f-56168c64b53e",
    [string]$PipelineId  = "ee52364c-e4ab-4606-bc13-ba28607c53a9"
)

$FabricBase = "https://api.fabric.microsoft.com/v1"

function Get-FabricToken {
    return (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
}

function Invoke-Fabric {
    param([string]$Method = "GET", [string]$Uri, [object]$Body = $null)
    $h = @{ "Authorization" = "Bearer $(Get-FabricToken)"; "Content-Type" = "application/json" }
    $p = @{ Method = $Method; Uri = $Uri; Headers = $h; UseBasicParsing = $true }
    if ($Body) { $p["Body"] = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 30 } }
    $r = Invoke-WebRequest @p
    if ($r.Content) { return ($r.Content | ConvertFrom-Json) }
    return $null
}

function To-Base64([string]$s) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($s))
}

# Get real notebook IDs from workspace
Write-Host "Fetching notebook IDs from workspace..." -ForegroundColor Cyan
$items = (Invoke-Fabric -Uri "$FabricBase/workspaces/$WorkspaceId/items").value
$nbMap = @{}
foreach ($item in ($items | Where-Object { $_.type -eq "Notebook" })) {
    if ($item.displayName -match "^(\d{2})_") {
        $key = "NB$($Matches[1])_ID"
        $nbMap[$key] = $item.id
        Write-Host "  $key = $($item.id)  [$($item.displayName)]" -ForegroundColor DarkGray
    }
}

# Load and resolve pipeline JSON
$scriptDir = Split-Path $MyInvocation.MyCommand.Path -Parent
$pipelinePath = Join-Path $scriptDir "output\contoso-energy\Pipeline\pipeline-content.json"
$pj = Get-Content $pipelinePath -Raw -Encoding UTF8

$pj = $pj -replace "\{\{WORKSPACE_ID\}\}", $WorkspaceId
foreach ($key in $nbMap.Keys) {
    $pj = $pj -replace "\{\{$key\}\}", $nbMap[$key]
}

# Verify
$remaining = [regex]::Matches($pj, '\{\{[A-Z0-9_]+\}\}') | ForEach-Object { $_.Value } | Sort-Object -Unique
if ($remaining) {
    Write-Warning "Unresolved placeholders: $($remaining -join ', ')"
} else {
    Write-Host "All placeholders resolved." -ForegroundColor Green
}

# Update pipeline definition
Write-Host "Updating pipeline definition..." -ForegroundColor Cyan
$updateBody = @{
    definition = @{
        parts = @(
            @{
                path = "pipeline-content.json"
                payload = (To-Base64 $pj)
                payloadType = "InlineBase64"
            }
        )
    }
}

try {
    Invoke-Fabric -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items/$PipelineId/updateDefinition" -Body $updateBody | Out-Null
    Write-Host "Pipeline definition updated." -ForegroundColor Green
} catch {
    Write-Warning "updateDefinition failed: $_"
}

# Trigger new run
Write-Host "Triggering new pipeline run..." -ForegroundColor Cyan
$runHeaders = @{ "Authorization" = "Bearer $(Get-FabricToken)" }
$rawResp = Invoke-WebRequest -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items/$PipelineId/jobs/instances?jobType=Pipeline" -Headers $runHeaders -Body "{}" -ContentType "application/json" -UseBasicParsing
$runLocation = $rawResp.Headers["Location"]
Write-Host "Run location: $runLocation" -ForegroundColor DarkGray

# Poll
$start = Get-Date
$maxMin = 20
while ($true) {
    Start-Sleep -Seconds 15
    $elapsed = [math]::Round(((Get-Date) - $start).TotalMinutes, 1)
    $status = (Invoke-Fabric -Uri $runLocation).status
    Write-Host "  Status: $status ($($elapsed)m elapsed)"
    if ($status -in @("Succeeded", "Completed", "Failed", "Cancelled", "Deduped")) { break }
    if ($elapsed -gt $maxMin) { Write-Warning "Timeout after $maxMin min"; break }
}

if ($status -in @("Succeeded", "Completed")) {
    Write-Host "Pipeline run SUCCEEDED. Bronze -> Silver -> Gold data is now populated." -ForegroundColor Green
} else {
    Write-Warning "Pipeline run ended with status: $status"
}
