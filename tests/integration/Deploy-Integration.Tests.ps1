<#
.SYNOPSIS
    Live deployment integration tests for Fabric End-to-End demos.
.DESCRIPTION
    Validates that generated artifacts deploy correctly to a real Fabric workspace.
    Requires:
      - Az PowerShell module (authenticated)
      - A test Fabric workspace with capacity
      - Environment variable FABRIC_TEST_WORKSPACE_ID set

    Run: Invoke-Pester -Path tests/integration/Deploy-Integration.Tests.ps1 -Tag Integration
#>

BeforeAll {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    Import-Module (Join-Path $ProjectRoot "shared/deploy/FabricHelpers.psm1") -Force

    $WorkspaceId = $env:FABRIC_TEST_WORKSPACE_ID
    if (-not $WorkspaceId) {
        throw "Set FABRIC_TEST_WORKSPACE_ID to run integration tests"
    }

    # Helper: list items in workspace
    function Get-WorkspaceItems {
        param([string]$Type)
        $uri = "workspaces/$WorkspaceId/items"
        if ($Type) { $uri += "?type=$Type" }
        $result = Invoke-FabricApi -Method GET -Uri $uri
        return $result.value
    }

    # Helper: delete item by ID (cleanup)
    function Remove-WorkspaceItem {
        param([string]$ItemId)
        Invoke-FabricApi -Method DELETE -Uri "workspaces/$WorkspaceId/items/$ItemId"
    }
}

Describe "Workspace Connectivity" -Tag Integration {
    It "Can authenticate and reach the Fabric API" {
        $result = Invoke-FabricApi -Method GET -Uri "workspaces/$WorkspaceId"
        $result | Should -Not -BeNullOrEmpty
        $result.id | Should -Be $WorkspaceId
    }

    It "Workspace has capacity assigned" {
        $result = Invoke-FabricApi -Method GET -Uri "workspaces/$WorkspaceId"
        $result.capacityId | Should -Not -BeNullOrEmpty
    }
}

Describe "Per-Industry Deployment" -Tag Integration -ForEach @(
    @{ Industry = "horizon-books"; Label = "Horizon Books" }
    @{ Industry = "contoso-energy"; Label = "Contoso Energy" }
    @{ Industry = "northwind-hrfinance"; Label = "Northwind HR Finance" }
    @{ Industry = "fabrikam-manufacturing"; Label = "Fabrikam Manufacturing" }
) {
    BeforeAll {
        $OutputDir = Join-Path $ProjectRoot "output/$Industry"
        if (-not (Test-Path $OutputDir)) {
            # Generate if not already present
            python (Join-Path $ProjectRoot "generate.py") -i $Industry -o $OutputDir
        }
        $DeployDir = Join-Path $OutputDir "deploy"
    }

    It "Deploy directory exists for <Label>" {
        $DeployDir | Should -Exist
    }

    It "Deploy script exists for <Label>" {
        $deployScript = Get-ChildItem $DeployDir -Filter "Deploy-*.ps1" | Select-Object -First 1
        $deployScript | Should -Not -BeNullOrEmpty
    }

    Context "Lakehouse creation for <Label>" {
        It "Can create Bronze Lakehouse" {
            $body = @{
                displayName = "Test_BronzeLH_$Industry"
                type        = "Lakehouse"
            } | ConvertTo-Json
            $result = Invoke-FabricApi -Method POST -Uri "workspaces/$WorkspaceId/items" -Body $body
            $result.id | Should -Not -BeNullOrEmpty
            $script:BronzeLHId = $result.id
        }

        AfterAll {
            if ($script:BronzeLHId) {
                Remove-WorkspaceItem -ItemId $script:BronzeLHId
            }
        }
    }

    Context "Sample data upload for <Label>" {
        It "SampleData directory has CSV files" {
            $csvDir = Join-Path $OutputDir "SampleData"
            if (Test-Path $csvDir) {
                $csvFiles = Get-ChildItem $csvDir -Filter "*.csv"
                $csvFiles.Count | Should -BeGreaterThan 0
            }
        }
    }

    Context "Semantic model validation for <Label>" {
        It "SemanticModel directory exists" {
            $smDirs = Get-ChildItem $OutputDir -Directory -Filter "*.SemanticModel"
            $smDirs.Count | Should -BeGreaterOrEqual 1
        }

        It "TMDL model.tmdl file exists" {
            $smDir = Get-ChildItem $OutputDir -Directory -Filter "*.SemanticModel" | Select-Object -First 1
            $modelFile = Join-Path $smDir.FullName "definition/model.tmdl"
            $modelFile | Should -Exist
        }
    }

    Context "Report validation for <Label>" {
        It "At least one .pbip file exists" {
            $pbips = Get-ChildItem $OutputDir -Filter "*.pbip"
            $pbips.Count | Should -BeGreaterOrEqual 1
        }

        It "Report definition directories exist" {
            $reportDirs = Get-ChildItem $OutputDir -Directory -Filter "*.Report"
            $reportDirs.Count | Should -BeGreaterOrEqual 1
        }
    }
}

Describe "Cleanup" -Tag Integration {
    It "No test artifacts remain in workspace" {
        $items = Get-WorkspaceItems
        $testItems = $items | Where-Object { $_.displayName -like "Test_*" }
        $testItems.Count | Should -Be 0
    }
}
