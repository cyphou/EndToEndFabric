<#
.SYNOPSIS
    PowerShell wrapper for the Fabric End-to-End Demo Generator.

.DESCRIPTION
    Wraps generate.py with environment setup and parameter forwarding.

.PARAMETER Industry
    Industry ID to generate (e.g. 'contoso-energy', 'horizon-books').

.PARAMETER Output
    Output directory. Defaults to ./output/<Industry>.

.PARAMETER List
    List available industries and exit.

.PARAMETER SkipHtap
    Skip HTAP (Eventhouse/KQL) generation.

.PARAMETER SkipForecast
    Skip Forecasting & Planning generation.

.PARAMETER Seed
    Random seed for reproducible data generation. Default: 42.

.EXAMPLE
    .\generate.ps1 -List
    .\generate.ps1 -Industry horizon-books
    .\generate.ps1 -Industry contoso-energy -Output .\my-output -SkipHtap
#>
[CmdletBinding()]
param(
    [string]$Industry,
    [string]$Output,
    [switch]$List,
    [switch]$SkipHtap,
    [switch]$SkipForecast,
    [switch]$SkipDeploy,
    [int]$Seed = 42
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

# Find Python
$python = $null
foreach ($cmd in @('python3', 'python', 'py')) {
    $found = Get-Command $cmd -ErrorAction SilentlyContinue
    if ($found) {
        $python = $found.Source
        break
    }
}

if (-not $python) {
    Write-Error "Python 3.12+ not found. Install Python and ensure it is on PATH."
    exit 1
}

# Verify version
$ver = & $python --version 2>&1
if ($ver -notmatch '3\.(1[2-9]|[2-9]\d)') {
    Write-Warning "Python 3.12+ recommended. Found: $ver"
}

# Build argument list
$pyArgs = @("$ScriptDir\generate.py")

if ($List) {
    $pyArgs += '--list'
} elseif ($Industry) {
    $pyArgs += @('-i', $Industry)
    if ($Output)       { $pyArgs += @('-o', $Output) }
    if ($SkipHtap)     { $pyArgs += '--skip-htap' }
    if ($SkipForecast) { $pyArgs += '--skip-forecast' }
    if ($SkipDeploy)   { $pyArgs += '--skip-deploy' }
    $pyArgs += @('--seed', $Seed)
} else {
    Write-Host "Usage: .\generate.ps1 -Industry <id> | -List"
    Write-Host "Run '.\generate.ps1 -List' to see available industries."
    exit 1
}

& $python @pyArgs
exit $LASTEXITCODE
