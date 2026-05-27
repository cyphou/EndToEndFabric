param(
    [string]$WorkspaceId = 'acaeaa28-d3b3-4c64-b18f-56168c64b53e',
    [string]$SmId = 'b6d8f433-8a31-428a-82ce-51789ba6050b',
    [string]$WbSmId = 'dcacf141-f6b9-483c-b8a6-511e74503a2f'
)

$ErrorActionPreference = 'Stop'
$OutputRoot = Join-Path $PSScriptRoot "output\contoso-energy"

function Get-FabricToken {
    (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
}

function To-Base64([string]$s) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($s))
}

$FabricBase = "https://api.fabric.microsoft.com/v1"

# Reports to fix
$reports = @(
    @{ Name='ContosoEnergy-Analytics';   Dir='ContosoEnergy-Analytics.Report';   ModelId=$SmId }
    @{ Name='ContosoEnergy-Forecasting'; Dir='ContosoEnergy-Forecasting.Report'; ModelId=$SmId }
    @{ Name='ContosoEnergy-HTAP';        Dir='ContosoEnergy-HTAP.Report';        ModelId=$SmId }
    @{ Name='ContosoEnergy-Pipeline';    Dir='ContosoEnergy-Pipeline.Report';    ModelId=$SmId }
    @{ Name='ContosoEnergy-Writeback';   Dir='ContosoEnergy-Writeback.Report';   ModelId=$WbSmId }
)

foreach ($rpt in $reports) {
    Write-Host "`n=== $($rpt.Name) ===" -ForegroundColor Cyan

    $rptRoot = Join-Path $OutputRoot $rpt.Dir
    $defDir  = Join-Path $rptRoot "definition"

    if (-not (Test-Path $defDir)) {
        Write-Warning "  Skipping - no definition dir: $defDir"
        continue
    }

    # Build parts array
    $parts = [System.Collections.ArrayList]::new()

    # definition.pbir
    $pbirPath = Join-Path $rptRoot "definition.pbir"
    if (Test-Path $pbirPath) {
        $c = [IO.File]::ReadAllText($pbirPath)
        $c = $c.Replace('{{SEMANTIC_MODEL_ID}}', $SmId)
        $c = $c.Replace('{{WRITEBACK_MODEL_ID}}', $WbSmId)
        $null = $parts.Add(@{ path = "definition.pbir"; payload = (To-Base64 $c); payloadType = "InlineBase64" })
    }

    # All files under definition/ - relative to .Report/ root (include definition/ prefix)
    # EXCEPT StaticResources/ which is a sibling of definition/ per PBIR spec
    foreach ($f in (Get-ChildItem $defDir -Recurse -File)) {
        $relPath = $f.FullName.Substring($rptRoot.Length + 1).Replace('\', '/')
        $content = [IO.File]::ReadAllText($f.FullName)
        $content = $content.Replace('{{SEMANTIC_MODEL_ID}}', $SmId)
        $content = $content.Replace('{{WRITEBACK_MODEL_ID}}', $WbSmId)
        $content = $content.Replace('{{WORKSPACE_ID}}', $WorkspaceId)

        # StaticResources must be at root level (sibling of definition/), not inside it
        if ($relPath -match '^definition/StaticResources/') {
            $relPath = $relPath.Substring('definition/'.Length)  # Strip 'definition/' prefix
        }

        $null = $parts.Add(@{ path = $relPath; payload = (To-Base64 $content); payloadType = "InlineBase64" })
    }

    Write-Host "  Parts: $($parts.Count) files"

    # Step 1: Delete existing report
    $tok = Get-FabricToken
    $hdrs = @{ Authorization = "Bearer $tok"; 'Content-Type' = 'application/json' }

    # Find existing report by name
    $existing = Invoke-RestMethod -Method GET -Uri "$FabricBase/workspaces/$WorkspaceId/reports" -Headers $hdrs
    $match = $existing.value | Where-Object { $_.displayName -eq $rpt.Name }

    if ($match) {
        Write-Host "  Deleting existing report $($match.id)..."
        Invoke-RestMethod -Method DELETE -Uri "$FabricBase/workspaces/$WorkspaceId/reports/$($match.id)" -Headers $hdrs
        Write-Host "  Deleted." -ForegroundColor Yellow
        Start-Sleep -Seconds 3
    }

    # Step 2: Create report with full definition
    $body = @{
        displayName = $rpt.Name
        type        = "Report"
        definition  = @{
            format = "PBIR"
            parts  = $parts
        }
    }

    $json = $body | ConvertTo-Json -Depth 10 -Compress
    Write-Host "  Creating report ($($json.Length) bytes)..."

    $tok = Get-FabricToken
    $hdrs = @{ Authorization = "Bearer $tok"; 'Content-Type' = 'application/json' }

    $resp = Invoke-WebRequest -Method POST -Uri "$FabricBase/workspaces/$WorkspaceId/items" -Headers $hdrs -Body $json -UseBasicParsing

    if ($resp.StatusCode -eq 201) {
        $created = $resp.Content | ConvertFrom-Json
        Write-Host "  Created: $($created.id)" -ForegroundColor Green
    }
    elseif ($resp.StatusCode -eq 202) {
        $loc = $resp.Headers['Location'] | Select-Object -First 1
        Write-Host "  Waiting for LRO..."
        do {
            Start-Sleep -Seconds 5
            $tok = Get-FabricToken
            $p = Invoke-RestMethod -Method GET -Uri $loc -Headers @{ Authorization = "Bearer $tok" }
            Write-Host "    Status: $($p.status)"
        } while ($p.status -notin @('Succeeded', 'Failed', 'Cancelled'))

        if ($p.status -eq 'Succeeded') {
            Write-Host "  Created OK" -ForegroundColor Green
        } else {
            Write-Host "  FAILED:" -ForegroundColor Red
            $p.error | ConvertTo-Json -Depth 5
        }
    }
}

# Step 3: Rebind reports to datasets
Write-Host "`n=== Rebinding reports ===" -ForegroundColor Cyan
$pbiTok = (Get-AzAccessToken -ResourceUrl 'https://analysis.windows.net/powerbi/api').Token
$pbiH = @{ Authorization = "Bearer $pbiTok"; 'Content-Type' = 'application/json' }

$allReports = Invoke-RestMethod -Method GET -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports" -Headers $pbiH
foreach ($r in $allReports.value) {
    $targetDs = if ($r.name -match 'Writeback') { $WbSmId } else { $SmId }
    Write-Host "  Rebinding $($r.name) -> $targetDs"
    try {
        Invoke-RestMethod -Method POST -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports/$($r.id)/Rebind" -Headers $pbiH -Body "{`"datasetId`":`"$targetDs`"}"
        Write-Host "    OK" -ForegroundColor Green
    } catch {
        Write-Host "    FAILED: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n=== Done! ===" -ForegroundColor Green
Write-Host "Try opening reports now. New report URLs:" -ForegroundColor Cyan
$allReports2 = Invoke-RestMethod -Method GET -Uri "https://api.powerbi.com/v1.0/myorg/groups/$WorkspaceId/reports" -Headers $pbiH
foreach ($r in $allReports2.value) {
    Write-Host "  $($r.name): https://app.powerbi.com/groups/$WorkspaceId/reports/$($r.id)?experience=power-bi"
}
