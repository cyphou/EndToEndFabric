<#
.SYNOPSIS
    Shared Fabric REST API helpers — reusable across all industry demos.
.DESCRIPTION
    Contains authentication, token management, and Fabric REST API wrapper
    functions used by all per-industry deployment scripts.
    Import via: Import-Module (Join-Path $PSScriptRoot "FabricHelpers.psm1") -Force
#>

# ── Token Store ──
$script:TokenStore = @{}
$script:FabricBaseUri = "https://api.fabric.microsoft.com/v1"

# ── Authentication ──

function Get-FabricToken {
    <#
    .SYNOPSIS Obtain an OAuth2 bearer token for the Fabric API.
    #>
    [CmdletBinding()]
    param()
    $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
    return $token
}

# ── REST Wrapper ──

function Invoke-FabricApi {
    <#
    .SYNOPSIS Generic Fabric REST API caller with automatic auth header.
    #>
    [CmdletBinding()]
    param(
        [ValidateSet("GET","POST","PUT","PATCH","DELETE")]
        [string]$Method = "GET",
        [Parameter(Mandatory)][string]$Uri,
        [object]$Body = $null,
        [string]$ContentType = "application/json"
    )
    $headers = @{
        "Authorization" = "Bearer $(Get-FabricToken)"
        "Content-Type"  = $ContentType
    }
    $params = @{ Method = $Method; Uri = $Uri; Headers = $headers }
    if ($Body) {
        $params["Body"] = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 20 }
    }
    $response = Invoke-RestMethod @params
    return $response
}

# ── Token Management ──

function Set-Token {
    <#
    .SYNOPSIS Store a deployment token (e.g. WORKSPACE_ID) for later substitution.
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][string]$Value)
    $script:TokenStore[$Name] = $Value
}

function Get-Token {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name)
    return $script:TokenStore[$Name]
}

function Resolve-Tokens {
    <#
    .SYNOPSIS Replace all {{TOKEN}} placeholders in a string with stored values.
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Content)
    $result = $Content
    foreach ($key in $script:TokenStore.Keys) {
        if ($script:TokenStore[$key]) {
            $result = $result -replace "\{\{$key\}\}", $script:TokenStore[$key]
        }
    }
    return $result
}

# ── Workspace Helpers ──

function New-FabricWorkspace {
    <#
    .SYNOPSIS Create a new Fabric workspace and return its ID.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$DisplayName,
        [string]$Description = ""
    )
    $body = @{ displayName = $DisplayName; description = $Description }
    $ws = Invoke-FabricApi -Method POST -Uri "$script:FabricBaseUri/workspaces" -Body $body
    return $ws.id
}

function Get-FabricWorkspaceItems {
    <#
    .SYNOPSIS List all items in a workspace.
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$WorkspaceId)
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/items"
    return (Invoke-FabricApi -Uri $uri).value
}

# ── Lakehouse Helpers ──

function New-FabricLakehouse {
    <#
    .SYNOPSIS Create a Lakehouse in the target workspace and return its ID.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DisplayName
    )
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/lakehouses"
    $resp = Invoke-FabricApi -Method POST -Uri $uri -Body @{ displayName = $DisplayName }
    return $resp.id
}

# ── Item Creation Helpers ──

function New-FabricItem {
    <#
    .SYNOPSIS Create a generic Fabric item (Notebook, Dataflow, Pipeline, etc.).
    .PARAMETER Type
        Fabric item type: Notebook, DataPipeline, MLModel, SemanticModel, Report, etc.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DisplayName,
        [Parameter(Mandatory)][string]$Type,
        [object]$Definition = $null
    )
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/items"
    $body = @{ displayName = $DisplayName; type = $Type }
    if ($Definition) { $body["definition"] = $Definition }
    $resp = Invoke-FabricApi -Method POST -Uri $uri -Body $body
    return $resp
}

function Update-FabricItemDefinition {
    <#
    .SYNOPSIS Update the definition of an existing Fabric item.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemId,
        [Parameter(Mandatory)][object]$Definition
    )
    $uri = "$script:FabricBaseUri/workspaces/$WorkspaceId/items/$ItemId/updateDefinition"
    Invoke-FabricApi -Method POST -Uri $uri -Body @{ definition = $Definition }
}

# ── Progress ──

function Write-Step {
    <#
    .SYNOPSIS Write a consistent progress step banner.
    #>
    [CmdletBinding()]
    param(
        [int]$Number,
        [int]$Total,
        [string]$Message
    )
    Write-Host "`n[$Number/$Total] $Message" -ForegroundColor Cyan
}

Export-ModuleMember -Function Get-FabricToken, Invoke-FabricApi,
    Set-Token, Get-Token, Resolve-Tokens,
    New-FabricWorkspace, Get-FabricWorkspaceItems,
    New-FabricLakehouse, New-FabricItem, Update-FabricItemDefinition,
    Write-Step
