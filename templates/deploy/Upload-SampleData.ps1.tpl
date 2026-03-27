# {{company_name}} — Upload-SampleData.ps1
# Uploads CSV files to {{bronze_lh}} via OneLake DFS API
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceId,
    [string]$LakehouseName = "{{bronze_lh}}"
)

Import-Module "$PSScriptRoot/{{company_pascal}}.psm1" -Force

$token = Get-FabricToken
$csvDir = "$PSScriptRoot/../SampleData"

$files = Get-ChildItem -Path $csvDir -Filter "*.csv"
Write-Host "Uploading $($files.Count) CSV files to $LakehouseName..."

foreach ($file in $files) {
    $targetPath = "Tables/$($file.BaseName)/$($file.Name)"
    Upload-ToOneLake -WorkspaceId $WorkspaceId `
        -LakehouseName $LakehouseName `
        -LocalPath $file.FullName `
        -TargetPath $targetPath `
        -Token $token
    Write-Host "  Uploaded: $($file.Name)"
}

Write-Host "Upload complete: $($files.Count) files"
