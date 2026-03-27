<#
.SYNOPSIS
    Shared OneLake DFS helpers — file upload/download via the OneLake REST API.
.DESCRIPTION
    Provides Upload-FileToOneLake, Upload-FolderToOneLake, and related utilities
    for pushing CSV sample data and notebook artifacts to OneLake.
    Import via: Import-Module (Join-Path $PSScriptRoot "OneLakeHelpers.psm1") -Force
#>

$script:OneLakeBaseUri = "https://onelake.dfs.fabric.microsoft.com"

function Upload-FileToOneLake {
    <#
    .SYNOPSIS Upload a single file to a Lakehouse via OneLake DFS API.
    .PARAMETER WorkspaceId  Fabric workspace GUID.
    .PARAMETER LakehouseId  Target Lakehouse GUID.
    .PARAMETER LocalPath    Full local path to the file.
    .PARAMETER DestinationPath  Relative path inside Files/ (e.g. "SampleData/Customers.csv").
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$LocalPath,
        [Parameter(Mandatory)][string]$DestinationPath
    )
    $token = (Get-AzAccessToken -ResourceUrl "https://storage.azure.com").Token
    $uri = "$script:OneLakeBaseUri/$WorkspaceId/$LakehouseId/Files/$DestinationPath"

    # Step 1: Create (PUT with resource=file)
    $createParams = @{
        Method  = "PUT"
        Uri     = "$uri`?resource=file"
        Headers = @{ "Authorization" = "Bearer $token" }
    }
    Invoke-RestMethod @createParams | Out-Null

    # Step 2: Append data
    $bytes = [System.IO.File]::ReadAllBytes($LocalPath)
    $appendParams = @{
        Method      = "PATCH"
        Uri         = "$uri`?action=append&position=0"
        Headers     = @{
            "Authorization" = "Bearer $token"
            "Content-Type"  = "application/octet-stream"
        }
        Body        = $bytes
    }
    Invoke-RestMethod @appendParams | Out-Null

    # Step 3: Flush
    $flushParams = @{
        Method  = "PATCH"
        Uri     = "$uri`?action=flush&position=$($bytes.Length)"
        Headers = @{ "Authorization" = "Bearer $token" }
    }
    Invoke-RestMethod @flushParams | Out-Null
}

function Upload-FolderToOneLake {
    <#
    .SYNOPSIS Upload all files in a local folder to a Lakehouse folder.
    .PARAMETER Filter  Glob filter for files (default "*.csv").
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$LocalFolder,
        [string]$DestinationFolder = "SampleData",
        [string]$Filter = "*.csv"
    )
    $files = Get-ChildItem -Path $LocalFolder -Filter $Filter -File
    $total = $files.Count
    $i = 0
    foreach ($file in $files) {
        $i++
        $dest = "$DestinationFolder/$($file.Name)"
        Write-Host "  [$i/$total] Uploading $($file.Name) → $dest" -ForegroundColor Gray
        Upload-FileToOneLake -WorkspaceId $WorkspaceId -LakehouseId $LakehouseId `
            -LocalPath $file.FullName -DestinationPath $dest
    }
    Write-Host "  Uploaded $total files to $DestinationFolder." -ForegroundColor Green
}

function Test-OneLakeFileExists {
    <#
    .SYNOPSIS Check if a file exists in OneLake. Returns $true/$false.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$FilePath
    )
    $token = (Get-AzAccessToken -ResourceUrl "https://storage.azure.com").Token
    $uri = "$script:OneLakeBaseUri/$WorkspaceId/$LakehouseId/Files/$FilePath"
    try {
        Invoke-RestMethod -Method HEAD -Uri $uri -Headers @{ "Authorization" = "Bearer $token" }
        return $true
    } catch {
        return $false
    }
}

Export-ModuleMember -Function Upload-FileToOneLake, Upload-FolderToOneLake, Test-OneLakeFileExists
