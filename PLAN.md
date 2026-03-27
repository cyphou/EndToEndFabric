# Fabric End-to-End Industry Demo Generator

## Multi-Agent Architecture & Implementation Plan

> **Goal:** Build a reusable, multi-agent framework that generates complete Microsoft Fabric end-to-end demos for multiple industries — each with Medallion Lakehouse, Notebooks, Dataflows Gen2, Semantic Model (Direct Lake), Power BI Reports, Data Agent, Forecasting (Holt-Winters + MLflow), Planning (Fabric IQ), and **Transactional Analytics (HTAP)** — deployed in one command.

> **Reference:** Architecture inspired by [TableauToPowerBI](../TableauToPowerBI) (8+1 agent model) and [FullDemoFabricBookUseCase](../FullDemoFabricBookUseCase) (Horizon Books demo pattern).

---

## Table of Contents

1. [Industry Demos Overview](#1-industry-demos-overview)
2. [Multi-Agent Architecture (9+1 Agents)](#2-multi-agent-architecture-91-agents)
3. [Shared Framework (Core Engine)](#3-shared-framework-core-engine)
4. [Industry Demo Specifications](#4-industry-demo-specifications)
5. [Transactional Analytics (HTAP) Module](#5-transactional-analytics-htap-module)
6. [Pipeline Architecture](#6-pipeline-architecture)
7. [Project Structure](#7-project-structure)
8. [Agent Definitions & Ownership](#8-agent-definitions--ownership)
9. [Implementation Phases](#9-implementation-phases)
10. [Testing Strategy](#10-testing-strategy)
11. [CI/CD](#11-cicd)

---

## 1. Industry Demos Overview

Each demo follows the **Horizon Books blueprint** but with industry-specific data, business logic, KPIs, and analytics.

| Demo | Industry | Company Story | Key Domains |
|------|----------|---------------|-------------|
| **Horizon Books** | Publishing & Distribution | Mid-size book publisher, 6 imprints, global ops | Finance, Operations, HR, Inventory |
| **Contoso Energy** | Energy & Utilities | Regional energy provider, renewables + grid ops | Generation, Grid Operations, Customer Billing, Sustainability, Field Ops |
| **Northwind HR/Finance** | HR & Corporate Finance | Multi-subsidiary holding company | Workforce, Compensation, Recruitment, P&L, Treasury, Compliance |
| **Fabrikam Manufacturing** | Manufacturing & Industry | Automotive parts manufacturer, 4 plants | Production, Quality, Supply Chain, Maintenance, EHS |

### Common Fabric Artifacts Per Demo

| Artifact | Count | Description |
|----------|-------|-------------|
| Lakehouses | 3 | Bronze (raw), Silver (cleaned), Gold (star schema) — all schema-enabled |
| Notebooks | 6 | NB01: Bronze→Silver, NB02: Web Enrichment, NB03: Silver→Gold, NB04: Forecasting, NB05: Transactional Analytics, NB06: Diagnostic |
| Dataflows Gen2 | 3–5 | Domain-specific CSV-to-table ingestion |
| Data Pipeline | 1 | Linked orchestration (DF → NB01 → NB02 → NB03 → NB04 → NB05) |
| Spark Environment | 1 | Python deps + Spark config |
| Semantic Model | 1 | Direct Lake on Gold Lakehouse |
| Power BI Reports | 2–3 | Analytics (10+ pages), Forecasting (5 pages), HTAP Dashboard (3 pages) |
| Data Agent | 1 | AI Q&A on semantic model (F64+) |
| Eventhouses | 1 | Real-time event stream for HTAP scenarios |
| KQL Databases | 1 | Hot-path query layer for transactional analytics |

---

## 2. Multi-Agent Architecture (9+1 Agents)

Modeled after TableauToPowerBI's proven 8+1 agent pattern, adapted for Fabric demo generation.

```
┌─────────────────────────────────────────────────────────┐
│                    @SHARED CONSTRAINTS                   │
│   Hard rules, coding standards, file ownership model     │
└─────────────────────────────────────────────────────────┘
          ↓ inherited by all agents ↓
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│@ORCHESTRA│ │@DATA-ENG │ │@SEMANTIC │ │@REPORT   │
│  -TOR    │ │  INEER   │ │  -MODEL  │ │  -BUILDER│
│ CLI +    │ │ CSV +    │ │ TMDL +   │ │ PBIR +   │
│ Pipeline │ │ Notebook │ │ DAX meas │ │ Visuals  │
│ Config   │ │ Dataflow │ │ Relation │ │ Theme    │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│@FORECAST │ │@HTAP     │ │@DEPLOYER │ │@TESTER   │
│ Holt-    │ │ Eventhse │ │ PS1 +    │ │ Pester + │
│ Winters  │ │ KQL +    │ │ Fabric   │ │ Unit +   │
│ MLflow   │ │ RT Event │ │ REST API │ │ Integr   │
│ Planning │ │ HTAP     │ │ OneLake  │ │ NonReg   │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
                    ┌──────────┐
                    │@INDUSTRY │
                    │ Domain   │
                    │ Config   │
                    │ KPI defs │
                    │ Story    │
                    └──────────┘
```

### Agent Summary

| Agent | Role | Owns | Tools |
|-------|------|------|-------|
| **@orchestrator** | CLI, pipeline coordination, config loading | `generate.ps1`, `generate.py`, industry configs | read, edit, search, execute, todo, agent |
| **@data-engineer** | Sample data generation, notebooks, dataflows | `SampleData/`, `notebooks/`, `Dataflows/` | read, edit, search, execute, todo |
| **@semantic-model** | TMDL generation, DAX measures, relationships | `SemanticModel/`, `.SemanticModel/` definitions | read, edit, search, execute, todo |
| **@report-builder** | PBIR reports, visuals, themes, pages | `Reports/`, `.Report/` definitions | read, edit, search, execute, todo |
| **@forecaster** | Holt-Winters models, MLflow, Planning IQ | `Forecasting/`, `Planning/`, NB04, NB planning | read, edit, search, execute, todo |
| **@htap-engineer** | Eventhouse, KQL, real-time streams, HTAP | `Transactional/`, NB05, Eventhouse configs | read, edit, search, execute, todo |
| **@deployer** | PowerShell deployment, Fabric REST, OneLake | `deploy/`, `.psm1` helpers | read, edit, search, execute, todo |
| **@tester** | Pester tests, validation, data quality | `tests/` | read, edit, search, execute, todo |
| **@industry-designer** | Domain schemas, KPIs, business rules, story | `industries/*/` config files | read, edit, search, execute, todo |
| **@shared** | Cross-cutting constraints, conventions | `.github/agents/shared.instructions.md` | — (inherited) |

---

## 3. Shared Framework (Core Engine)

The framework is the **reusable scaffolding** that all demos share. Industry-specific content is injected via configuration.

### 3.1 Configuration-Driven Generation

```
industries/
├── horizon-books/          # Publishing & Distribution (existing)
│   ├── industry.json       # Master config (company story, domains, schemas)
│   ├── sample-data.json    # CSV file definitions, row counts, relationships
│   ├── semantic-model.json # Tables, columns, measures, relationships
│   ├── forecast-config.json
│   ├── planning-config.json
│   ├── htap-config.json    # Transactional analytics config
│   ├── reports.json        # Report pages, visuals, KPIs
│   ├── data-agent.json     # Agent system instructions
│   └── web-enrichment.json # API enrichment sources
├── contoso-energy/
│   ├── industry.json
│   ├── sample-data.json
│   ├── semantic-model.json
│   ├── forecast-config.json
│   ├── planning-config.json
│   ├── htap-config.json
│   ├── reports.json
│   ├── data-agent.json
│   └── web-enrichment.json
├── northwind-hrfinance/
│   └── ... (same structure)
└── fabrikam-manufacturing/
    └── ... (same structure)
```

### 3.2 Master Config Schema — `industry.json`

```json
{
  "industry": {
    "id": "contoso-energy",
    "name": "Contoso Energy",
    "displayName": "Contoso Energy & Utilities",
    "description": "Regional energy provider serving 2.4M customers across 5 states",
    "icon": "⚡",
    "domains": ["Generation", "Grid", "Billing", "Sustainability", "FieldOps"],
    "dataYears": ["FY2024", "FY2025", "FY2026"],
    "headquarters": "Denver, CO",
    "operations": ["Wind farms (3)", "Solar parks (2)", "Natural gas plants (2)", "Grid substations (47)"],
    "theme": {
      "primary": "#00843D",
      "secondary": "#FFB81C",
      "accent1": "#4A90D9",
      "accent2": "#E74C3C",
      "background": "#1A1A2E"
    }
  },
  "fabricArtifacts": {
    "workspacePrefix": "ContosoEnergy",
    "lakehouses": {
      "bronze": "BronzeLH",
      "silver": "SilverLH",
      "gold": "GoldLH"
    },
    "schemas": {
      "silver": ["generation", "grid", "billing", "sustainability", "fieldops", "web"],
      "gold": ["dim", "fact", "analytics", "planning"]
    },
    "notebooks": 6,
    "dataflows": 5,
    "reports": 3,
    "dataPipeline": "PL_ContosoEnergy_Orchestration"
  }
}
```

### 3.3 Core Generators (Shared Modules)

| Module | Purpose | Input | Output |
|--------|---------|-------|--------|
| `core/csv_generator.py` | Generate realistic sample CSV data | `sample-data.json` | `SampleData/**/*.csv` |
| `core/notebook_generator.py` | Templatize PySpark notebooks | `industry.json` + schemas | `notebooks/*.py` |
| `core/dataflow_generator.py` | Generate Dataflow Gen2 queries | `sample-data.json` | `Dataflows/` configs |
| `core/tmdl_generator.py` | Generate TMDL semantic model | `semantic-model.json` | `.SemanticModel/` |
| `core/report_generator.py` | Generate PBIR report pages | `reports.json` | `.Report/` |
| `core/forecast_generator.py` | Generate forecast notebooks | `forecast-config.json` | NB04 + forecast tables |
| `core/planning_generator.py` | Generate planning tables | `planning-config.json` | Planning notebooks |
| `core/htap_generator.py` | Generate HTAP artifacts | `htap-config.json` | Eventhouse + KQL + NB05 |
| `core/deploy_generator.py` | Generate PS1 deployment | `industry.json` | `deploy/*.ps1` |
| `core/agent_generator.py` | Generate Data Agent config | `data-agent.json` | `DataAgent/` |
| `core/test_generator.py` | Generate Pester test suite | All configs | `tests/*.Tests.ps1` |

---

## 4. Industry Demo Specifications

### 4.1 Contoso Energy — Energy & Utilities

**Company Story:** Contoso Energy operates a diversified energy portfolio across 5 states — wind farms, solar parks, natural gas plants, and 47 grid substations serving 2.4M residential and commercial customers. They are transitioning to 60% renewable by 2028.

#### Sample Data (Bronze — 20 CSV files)

| Domain | CSV Files | Key Tables |
|--------|-----------|------------|
| **Generation** | DimPowerPlants, DimEnergyAssets, FactGeneration, FactFuelConsumption | Plant capacity, hourly output (MWh), fuel usage |
| **Grid** | DimSubstations, DimTransmissionLines, FactGridEvents, FactLoadProfile | Load balancing, outage events, peak demand |
| **Billing** | DimCustomerAccounts, DimTariffPlans, FactBilling, FactPayments | 2.4M customer bills, payment history, arrears |
| **Sustainability** | DimCarbonSources, FactEmissions, FactRenewableCertificates | CO₂ tracking, RECs, carbon offset credits |
| **Field Ops** | DimFieldCrews, DimWorkOrders, FactFieldActivity | Crew dispatches, maintenance, inspections |

#### DAX Measures (110+)

| Domain | Measures | Key KPIs |
|--------|:---:|---------|
| Generation | 25 | Total MWh, Capacity Factor %, Renewable Mix %, Plant Efficiency |
| Grid | 20 | SAIDI, SAIFI, System Average Interruption Duration, Peak Load MW |
| Billing | 25 | Revenue per Customer, Avg Bill Amount, Collection Rate %, Bad Debt |
| Sustainability | 15 | Carbon Intensity (tCO₂/MWh), Renewable %, REC Balance, Scope 1/2/3 |
| Field Ops | 15 | MTTR (Mean Time to Repair), Work Orders Open, First-Time Fix Rate |
| Forecasting | 10 | Load Forecast Accuracy, Demand Prediction, Renewable Output Forecast |

#### Power BI Reports

**Report 1: Contoso Energy Analytics (12 pages)**
1. Executive Dashboard — KPI summary (MWh, Revenue, Carbon, SAIDI)
2. Generation Overview — Plant performance, capacity utilization
3. Renewable Energy — Solar/Wind output, capacity factor trends
4. Grid Operations — Load profile, outage map, SAIDI/SAIFI
5. Customer Billing — Revenue, arrears, tariff mix
6. Payment Analytics — Collection, aging, bad debt
7. Sustainability & ESG — Carbon intensity, REC certificates, targets
8. Field Operations — Crew utilization, work order backlog
9. Geographic Coverage — Service territory map, customer density
10. Workforce & Safety — Field crew metrics, incident rate
11. Financial P&L — Energy revenue vs operating costs
12. Budget vs Actual — CapEx/OpEx variance, investment ROI

**Report 2: Contoso Energy Forecasting (5 pages)**
1. Demand Forecast Overview
2. Load Forecasting by Region
3. Renewable Output Prediction
4. Revenue & Billing Forecast
5. Grid Maintenance Forecast

**Report 3: Contoso Energy HTAP Dashboard (3 pages)**
1. Real-Time Grid Monitoring — Live load, frequency, voltage
2. Event Stream Analytics — Grid fault detection, outage tracking
3. Operational Alerts — Threshold breaches, anomaly detection

#### Transactional Analytics (HTAP)

| Scenario | Event Source | Frequency | KQL Query Pattern |
|----------|-------------|-----------|-------------------|
| Grid telemetry | Smart meter readings | Every 15 sec | Sliding window avg, anomaly detect |
| Outage detection | SCADA fault signals | Event-driven | Pattern match → alert escalation |
| Billing transactions | Payment gateway | Real-time | Running totals, duplicate detection |
| Generation output | Turbine/panel sensors | Every 1 min | Rolling efficiency, deviation alerts |

#### Forecasting Models

| Model | Grain | Target | Horizon |
|-------|-------|--------|---------|
| Load Demand | Region × Hour | MWh | 7 days |
| Renewable Output | Plant × Day | MWh | 30 days |
| Revenue Billing | Tariff × Month | Revenue | 6 months |
| Grid Maintenance | Substation × Month | Work Orders | 6 months |
| Carbon Emissions | Source × Month | tCO₂ | 12 months |

---

### 4.2 Northwind HR/Finance — Corporate HR & Finance

**Company Story:** Northwind Holdings is a multi-subsidiary holding company with 3 business units (Retail, Services, Technology) operating in 12 countries. The CHRO and CFO need unified workforce analytics, compensation benchmarking, and consolidated financial reporting.

#### Sample Data (Bronze — 22 CSV files)

| Domain | CSV Files | Key Tables |
|--------|-----------|------------|
| **Workforce** | DimEmployees, DimDepartments, DimPositions, DimLocations, FactHeadcount | 2,500 employees, org hierarchy, tenure |
| **Compensation** | DimPayGrades, DimBenefitPlans, FactPayroll, FactBonuses, FactBenefitEnrollment | Salary bands, variable pay, benefits |
| **Recruitment** | DimJobPostings, DimCandidates, FactApplications, FactInterviews, FactOffers | Hiring funnel, time-to-fill, cost-per-hire |
| **Finance** | DimAccounts (CoA), DimCostCenters, DimProjects, FactGeneralLedger, FactBudget, FactCashFlow | GL transactions, budget, cash flow |
| **Compliance** | DimPolicies, FactTrainingCompletion, FactIncidents | Mandatory training, HR incidents, ethics |

#### DAX Measures (130+)

| Domain | Measures | Key KPIs |
|--------|:---:|---------|
| Workforce | 30 | Headcount, Turnover Rate, Avg Tenure, Span of Control, Diversity Index |
| Compensation | 25 | Compa-Ratio, Pay Equity Gap, Total Rewards Cost, Benefits Utilization |
| Recruitment | 20 | Time-to-Fill, Cost-per-Hire, Offer Acceptance Rate, Source Effectiveness |
| Finance | 35 | Net Income, EBITDA, Working Capital, Current Ratio, DSO, DPO |
| Compliance | 10 | Training Completion %, Incident Rate, Policy Adherence |
| Planning | 10 | Budget Variance, FTE Plan vs Actual, Compensation Budget Utilization |

#### Power BI Reports

**Report 1: Northwind HR & Finance Analytics (14 pages)**
1. Executive Summary — Unified KPI dashboard
2. Workforce Overview — Headcount, org chart, location map
3. Turnover & Retention — Voluntary/involuntary, by Department/Tenure
4. Diversity & Inclusion — Gender, ethnicity, age band analytics
5. Compensation Analysis — Compa-ratio, pay equity, total rewards
6. Benefits Dashboard — Enrollment, utilization, cost trends
7. Recruitment Funnel — Pipeline, time-to-fill, source ROI
8. Financial P&L — Consolidated income statement, subsidiary breakdown
9. Balance Sheet — Assets, liabilities, equity trends
10. Cash Flow — Operating/investing/financing activities
11. Budget vs Actual — Variance by cost center, project, subsidiary
12. Working Capital — DSO, DPO, inventory days, current ratio
13. Compliance & Training — Completion rates, incident tracking
14. Subsidiary Comparison — Cross-entity benchmarking

**Report 2: Northwind Forecasting (5 pages)**
1. Workforce Forecast — Headcount projection, attrition modeling
2. Compensation Forecast — Payroll growth, merit increase modeling
3. Revenue Forecast — By subsidiary and service line
4. Cash Flow Forecast — Working capital projection
5. Recruitment Demand — Open positions vs talent pipeline

**Report 3: Northwind HTAP Dashboard (3 pages)**
1. Real-Time Payroll Processing — Live pay run status, exceptions
2. Financial Transaction Stream — GL posting activity, reconciliation
3. Recruitment Activity — Application flow, interview scheduling

#### Transactional Analytics (HTAP)

| Scenario | Event Source | Frequency | KQL Query Pattern |
|----------|-------------|-----------|-------------------|
| Payroll processing | HRIS system | Batch + RT | Running totals, exception flagging |
| GL postings | ERP journal entries | Real-time | Accrual detection, period-close monitoring |
| Recruitment events | ATS webhook | Event-driven | Funnel velocity, bottleneck detection |
| Expense submissions | Expense system | Real-time | Policy violation, duplicate detection |

---

### 4.3 Fabrikam Manufacturing — Manufacturing & Industry

**Company Story:** Fabrikam Auto Parts manufactures precision components for the automotive industry across 4 plants (Detroit, Guadalajara, Stuttgart, Pune) with 1,200 SKUs and 180 suppliers.

#### Sample Data (Bronze — 25 CSV files)

| Domain | CSV Files | Key Tables |
|--------|-----------|------------|
| **Production** | DimProducts, DimProductionLines, DimShifts, FactProductionOrders, FactProductionOutput | 1,200 SKUs, production orders, output/hour |
| **Quality** | DimInspectionPoints, DimDefectTypes, FactQualityInspections, FactNonConformance | SPC, defect rates, NCR tracking |
| **Supply Chain** | DimSuppliers, DimMaterials, DimWarehouses, FactPurchaseOrders, FactGoodsReceipt, FactInventory | 180 suppliers, PO tracking, stock levels |
| **Maintenance** | DimEquipment, DimMaintenanceTypes, FactWorkOrders, FactDowntime | OEE, MTBF, MTTR, PM schedules |
| **EHS** | DimHazards, FactSafetyIncidents, FactEnvironmentalReadings | Safety incidents, air/water quality |

#### DAX Measures (120+)

| Domain | Measures | Key KPIs |
|--------|:---:|---------|
| Production | 30 | OEE, Throughput/hr, Yield %, Cycle Time, Schedule Adherence |
| Quality | 25 | DPPM, First Pass Yield, Cpk, NCR Count, Cost of Quality |
| Supply Chain | 25 | On-Time Delivery %, Lead Time, Inventory Turns, Supplier Score |
| Maintenance | 20 | OEE Breakdown (Availability × Performance × Quality), MTBF, MTTR |
| EHS | 10 | TRIR, Lost Time Rate, Near Miss Ratio, Environmental Compliance |
| Planning | 10 | Production Plan Adherence, Material Plan vs Actual, CapEx Utilization |

#### Power BI Reports

**Report 1: Fabrikam Manufacturing Analytics (12 pages)**
1. Executive Dashboard — OEE, revenue, quality, safety KPIs
2. Production Performance — Output by line/shift, schedule adherence
3. OEE Analysis — Availability × Performance × Quality waterfall
4. Quality Control — SPC charts, defect Pareto, Cpk trends
5. Supply Chain — Supplier scorecard, lead times, on-time delivery
6. Inventory Management — Stock levels, turnover, ABC analysis
7. Maintenance & Reliability — MTBF, PM compliance, downtime analysis
8. Plant Comparison — Cross-plant benchmarking (Detroit vs Stuttgart vs...)
9. Financial Overview — Cost of goods manufactured, margin analysis
10. Geographic Supply Map — Supplier locations, lead time by region
11. EHS Dashboard — Safety incidents, environmental metrics
12. Workforce & Shifts — Labor utilization, overtime, training

**Report 2: Fabrikam Forecasting (5 pages)**
1. Demand Forecast — Product demand by customer/region
2. Production Capacity — Planned vs available capacity
3. Material Requirements — Procurement forecast
4. Quality Trend — Defect rate projection
5. Maintenance Prediction — Equipment failure probability

**Report 3: Fabrikam HTAP Dashboard (3 pages)**
1. Real-Time Production Line — Live output, cycle times, scrap
2. Quality Event Stream — Inline inspection results, SPC alerts
3. Equipment Health Monitoring — Vibration, temperature, anomaly detection

#### Transactional Analytics (HTAP)

| Scenario | Event Source | Frequency | KQL Query Pattern |
|----------|-------------|-----------|-------------------|
| Production line output | PLC/MES telemetry | Every 5 sec | Cycle time, throughput trending |
| Quality inspections | Inline measurement | Per-part | SPC control charts, Cpk live calc |
| Equipment sensors | IoT vibration/temp | Every 10 sec | Anomaly detection, predictive alerts |
| Material consumption | Barcode scans | Event-driven | BOM explosion, stock depletion |

---

## 5. Transactional Analytics (HTAP) Module

The HTAP module adds **real-time analytics** alongside the batch medallion architecture — combining the strengths of operational (transactional) processing with analytical workloads.

### 5.1 Architecture

```
                    ┌─────────────────┐
                    │  Event Sources   │
                    │ (IoT, SCADA,    │
                    │  POS, HRIS)     │
                    └────────┬────────┘
                             │ Event Stream
                    ┌────────▼────────┐
                    │   Eventstream    │
                    │  (Fabric RT)    │
                    └───┬─────────┬───┘
                        │         │
              ┌─────────▼──┐  ┌──▼──────────┐
              │ Eventhouse  │  │  Lakehouse   │
              │ (KQL DB)    │  │  (Bronze)    │
              │ HOT PATH    │  │  COLD PATH   │
              └─────┬───────┘  └──────────────┘
                    │
              ┌─────▼───────┐
              │ KQL Queries  │
              │ Real-time    │
              │ Dashboards   │
              └──────────────┘
```

### 5.2 HTAP Config Schema — `htap-config.json`

```json
{
  "htapConfig": {
    "description": "Transactional analytics configuration",
    "eventhouse": {
      "name": "RT_<IndustryPrefix>_Events",
      "database": "EventsDB",
      "retentionDays": 30,
      "cachingDays": 7
    },
    "eventstreams": [
      {
        "name": "ES_GridTelemetry",
        "source": "simulated",
        "schema": {
          "columns": [
            {"name": "EventTime", "type": "datetime"},
            {"name": "DeviceId", "type": "string"},
            {"name": "MeasurementType", "type": "string"},
            {"name": "Value", "type": "real"},
            {"name": "Unit", "type": "string"},
            {"name": "Quality", "type": "int"}
          ]
        },
        "simulatorConfig": {
          "eventsPerSecond": 100,
          "deviceCount": 50,
          "anomalyRate": 0.02
        }
      }
    ],
    "kqlQueries": [
      {
        "name": "RealTimeLoad",
        "description": "Sliding window average load per substation",
        "query": "Events | where MeasurementType == 'Load' | summarize AvgLoad=avg(Value) by bin(EventTime, 1m), DeviceId | order by EventTime desc",
        "refreshInterval": "30s"
      }
    ],
    "alerts": [
      {
        "name": "HighLoadAlert",
        "condition": "AvgLoad > 95",
        "severity": "Critical",
        "action": "notification"
      }
    ]
  }
}
```

### 5.3 NB05 — Transactional Analytics Notebook

```python
# NB05_TransactionalAnalytics.py
# Responsibilities:
# 1. Create Eventhouse + KQL Database (via Fabric REST API)
# 2. Define event stream schemas
# 3. Generate simulated event data (configurable volume)
# 4. Create KQL materialized views for real-time aggregations
# 5. Bridge hot-path (KQL) ↔ cold-path (Lakehouse) via shortcuts
# 6. Create alerting rules
```

### 5.4 HTAP Report Pages (per industry)

| Page | Purpose | Visuals |
|------|---------|---------|
| Real-Time Monitor | Live KPIs, streaming gauges | KQL-backed cards, line charts (auto-refresh) |
| Event Stream | Recent events table, anomaly highlight | Scrolling table, anomaly markers |
| Alerts & Thresholds | Active alerts, SLA tracking | Alert list, threshold gauges, timeline |

---

## 6. Pipeline Architecture

### 6.1 Generation Pipeline (Build Time)

```
ORCHESTRATOR: generate.ps1 -Industry "contoso-energy"
       │
       ├→ @INDUSTRY-DESIGNER: Load industries/contoso-energy/*.json
       │
       ├→ @DATA-ENGINEER: Generate SampleData CSVs + Notebooks + Dataflows
       │   ├→ core/csv_generator.py (sample-data.json → CSVs)
       │   ├→ core/notebook_generator.py (industry.json → NB01–NB06)
       │   └→ core/dataflow_generator.py (sample-data.json → Dataflow configs)
       │
       ├→ @SEMANTIC-MODEL: Generate TMDL + DAX
       │   └→ core/tmdl_generator.py (semantic-model.json → .SemanticModel/)
       │
       ├→ @REPORT-BUILDER: Generate PBIR Reports
       │   └→ core/report_generator.py (reports.json → .Report/)
       │
       ├→ @FORECASTER: Generate Forecast + Planning configs
       │   ├→ core/forecast_generator.py (forecast-config.json → NB04 + tables)
       │   └→ core/planning_generator.py (planning-config.json → Planning notebooks)
       │
       ├→ @HTAP-ENGINEER: Generate HTAP artifacts
       │   └→ core/htap_generator.py (htap-config.json → Eventhouse + KQL + NB05)
       │
       ├→ @DEPLOYER: Generate deployment scripts
       │   └→ core/deploy_generator.py (industry.json → deploy/*.ps1)
       │
       └→ @TESTER: Generate test suite
           └→ core/test_generator.py (all configs → tests/*.Tests.ps1)
```

### 6.2 Deployment Pipeline (Runtime)

```
Deploy-Full.ps1 -WorkspaceId "<guid>" -Industry "contoso-energy"
       │
       Step 1:  Create 3 schema-enabled Lakehouses
       Step 2:  Upload sample CSV files to Bronze/Files
       Step 3:  Deploy Spark Environment
       Step 4:  Deploy Notebooks (NB01–NB06)
       Step 5:  Deploy Dataflows Gen2
       Step 6:  Deploy Data Pipeline (orchestration)
       Step 7:  Run orchestration pipeline
       Step 8:  Deploy Eventhouse + KQL Database (HTAP)
       Step 9:  Run NB05 (Transactional Analytics setup)
       Step 10: Deploy Semantic Model (Direct Lake)
       Step 11: Deploy Power BI Reports (Analytics + Forecast + HTAP)
       Step 12: Deploy Data Agent (F64+)
       Step 13: Deploy Planning tables
       Step 14: Validate deployment
```

---

## 7. Project Structure

```
FabricEndtoEnd/
├── .github/
│   ├── agents/                          # 🤖 Multi-agent definitions
│   │   ├── shared.instructions.md       # Hard constraints (all agents)
│   │   ├── orchestrator.agent.md
│   │   ├── data-engineer.agent.md
│   │   ├── semantic-model.agent.md
│   │   ├── report-builder.agent.md
│   │   ├── forecaster.agent.md
│   │   ├── htap-engineer.agent.md
│   │   ├── deployer.agent.md
│   │   ├── tester.agent.md
│   │   └── industry-designer.agent.md
│   └── workflows/
│       ├── ci-tests.yml                  # Pester + pytest on PR/push
│       └── generate-demo.yml             # On-demand demo generation
│
├── core/                                 # 🔧 Shared generation engine
│   ├── __init__.py
│   ├── csv_generator.py                  # Sample data from schema
│   ├── notebook_generator.py             # PySpark notebook templates
│   ├── dataflow_generator.py             # Dataflow Gen2 queries
│   ├── tmdl_generator.py                 # TMDL semantic model
│   ├── report_generator.py               # PBIR report pages
│   ├── forecast_generator.py             # Holt-Winters + Planning
│   ├── planning_generator.py             # Fabric IQ Planning tables
│   ├── htap_generator.py                 # Eventhouse + KQL + events
│   ├── deploy_generator.py               # PowerShell scripts
│   ├── agent_generator.py                # Data Agent config
│   ├── test_generator.py                 # Pester test suite
│   └── config_loader.py                  # Industry config resolver
│
├── industries/                           # 📦 Industry-specific configs
│   ├── horizon-books/
│   │   ├── industry.json
│   │   ├── sample-data.json
│   │   ├── semantic-model.json
│   │   ├── forecast-config.json
│   │   ├── planning-config.json
│   │   ├── htap-config.json
│   │   ├── reports.json
│   │   ├── data-agent.json
│   │   └── web-enrichment.json
│   ├── contoso-energy/
│   │   └── ... (same structure)
│   ├── northwind-hrfinance/
│   │   └── ... (same structure)
│   └── fabrikam-manufacturing/
│       └── ... (same structure)
│
├── templates/                            # 📝 Reusable templates
│   ├── notebooks/
│   │   ├── 01_BronzeToSilver.py.tpl
│   │   ├── 02_WebEnrichment.py.tpl
│   │   ├── 03_SilverToGold.py.tpl
│   │   ├── 04_Forecasting.py.tpl
│   │   ├── 05_TransactionalAnalytics.py.tpl
│   │   └── 06_DiagnosticCheck.py.tpl
│   ├── deploy/
│   │   ├── Deploy-Full.ps1.tpl
│   │   ├── HorizonBooks.psm1.tpl         # → <Industry>.psm1
│   │   └── Validate-Deployment.ps1.tpl
│   ├── reports/
│   │   ├── executive-dashboard.json.tpl
│   │   ├── financial-pl.json.tpl
│   │   └── ... (visual templates)
│   ├── tmdl/
│   │   ├── table.tmdl.tpl
│   │   ├── measure.tmdl.tpl
│   │   └── relationship.tmdl.tpl
│   └── kql/
│       ├── create-table.kql.tpl
│       ├── materialized-view.kql.tpl
│       └── alert-rule.kql.tpl
│
├── output/                               # 📂 Generated demos (gitignored)
│   ├── contoso-energy/                   # Full demo ← generated
│   │   ├── SampleData/
│   │   ├── notebooks/
│   │   ├── deploy/
│   │   ├── ...
│   │   └── README.md
│   └── northwind-hrfinance/
│       └── ...
│
├── shared/                               # 🔗 Shared utilities
│   ├── deploy/
│   │   ├── FabricHelpers.psm1            # Generic Fabric REST helpers
│   │   └── OneLakeHelpers.psm1           # OneLake DFS upload helpers
│   └── assets/
│       └── default-theme.json            # Base Power BI theme (overridden per industry)
│
├── tests/                                # 🧪 Test suites
│   ├── core/                             # Core engine unit tests (pytest)
│   │   ├── test_csv_generator.py
│   │   ├── test_notebook_generator.py
│   │   ├── test_tmdl_generator.py
│   │   ├── test_report_generator.py
│   │   ├── test_htap_generator.py
│   │   └── test_config_loader.py
│   ├── industries/                       # Per-industry validation (Pester)
│   │   ├── HorizonBooks.Tests.ps1
│   │   ├── ContosoEnergy.Tests.ps1
│   │   ├── NorthwindHRFinance.Tests.ps1
│   │   └── FabrikamManufacturing.Tests.ps1
│   └── integration/                      # Live Fabric integration tests
│       └── Deploy-Integration.Tests.ps1
│
├── generate.ps1                          # 🚀 Main entry: generate a demo
├── generate.py                           # 🐍 Python generation engine
├── README.md
├── ARCHITECTURE.md
├── AGENTS.md                             # Multi-agent docs
├── CONTRIBUTING.md
└── pyproject.toml
```

---

## 8. Agent Definitions & Ownership

### 8.1 @shared — Cross-Cutting Constraints

**File:** `.github/agents/shared.instructions.md`

```yaml
---
applies_to: all_agents
---
```

**Hard Constraints:**
1. **Configuration-driven** — All industry-specific behavior comes from `industries/<id>/*.json`, never hard-coded
2. **Idempotent generation** — Re-running `generate.ps1` produces identical output for same config
3. **No external deps for core** — Python stdlib only (csv, json, os, pathlib, string.Template)
4. **Read before write** — Never assume file contents; always load config first
5. **Test after change** — Run pytest (core) + Pester (industry) after every modification
6. **Git hygiene** — Conventional commits: `feat(energy):`, `fix(core):`, `test(hrfinance):`
7. **Template discipline** — Templates use `{{PLACEHOLDER}}` syntax, never raw string concat
8. **Schema validation** — All JSON configs validated against schemas before generation

**Naming Conventions:**
- Lakehouse: `BronzeLH`, `SilverLH`, `GoldLH`
- Notebooks: `NB01_BronzeToSilver`, `NB02_WebEnrichment`, ...
- Dataflows: `DF_<Domain>` (e.g., `DF_Generation`, `DF_Billing`)
- Pipeline: `PL_<CompanyName>_Orchestration`
- Reports: `<CompanyName>Analytics`, `<CompanyName>Forecasting`, `<CompanyName>HTAP`

### 8.2 @orchestrator

**Owns:** `generate.ps1`, `generate.py`, top-level configs
**Can invoke:** All agents
**Tools:** read, edit, search, execute, todo, agent

**Responsibilities:**
- Parse CLI arguments (`-Industry`, `-OutputDir`, `-SkipHTAP`, etc.)
- Load and validate industry config files
- Invoke generators in correct sequence
- Handle incremental generation (only regenerate changed configs)

### 8.3 @data-engineer

**Owns:** `core/csv_generator.py`, `core/notebook_generator.py`, `core/dataflow_generator.py`, `templates/notebooks/`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate realistic sample CSV data matching schema definitions
- Generate PySpark notebooks with industry-specific transformations
- Generate Dataflow Gen2 query definitions
- Ensure referential integrity across CSVs (FK→PK consistency)
- Web enrichment API integration (with static fallbacks)

### 8.4 @semantic-model

**Owns:** `core/tmdl_generator.py`, `templates/tmdl/`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate TMDL table definitions (columns, data types, annotations)
- Generate DAX measures from `semantic-model.json` specs
- Generate relationship definitions (1:M, cardinality, cross-filter)
- Auto-generate date dimension with fiscal year support
- Validate DAX syntax and relationship integrity

### 8.5 @report-builder

**Owns:** `core/report_generator.py`, `templates/reports/`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate PBIR v4.0 report page definitions
- Map KPIs to appropriate visual types (card, bar, line, map, etc.)
- Apply industry-specific themes (colors, fonts, backgrounds)
- Generate filter pane configurations
- Generate bookmark groups for demo scenarios

### 8.6 @forecaster

**Owns:** `core/forecast_generator.py`, `core/planning_generator.py`, `templates/notebooks/04_Forecasting.py.tpl`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate Holt-Winters forecast models from config
- Generate MLflow experiment tracking code
- Generate Planning IQ table definitions (writeback-enabled)
- Generate scenario modeling (Base/Optimistic/Conservative)
- Generate plan-vs-actual variance calculations

### 8.7 @htap-engineer

**Owns:** `core/htap_generator.py`, `templates/kql/`, `templates/notebooks/05_TransactionalAnalytics.py.tpl`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate Eventhouse and KQL Database definitions
- Generate Eventstream ingestion configs
- Generate KQL queries for real-time aggregations
- Generate simulated event data generators
- Generate materialized views for hot-path analytics
- Generate hot-cold bridge (KQL → Lakehouse shortcuts)
- Generate alerting rules and thresholds

### 8.8 @deployer

**Owns:** `core/deploy_generator.py`, `shared/deploy/`, `templates/deploy/`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate idempotent PowerShell deployment scripts
- Generate Fabric REST API calls (Lakehouse, Notebook, Report, etc.)
- Generate OneLake DFS upload logic
- Generate Eventhouse/KQL deployment
- Generate validation/diagnostic scripts
- Handle parameterized deployment (workspace ID, capacity, skip flags)

### 8.9 @tester

**Owns:** `core/test_generator.py`, `tests/`
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Generate Pester tests for each industry demo
- Generate pytest tests for core generators
- Validate CSV row counts, schema integrity, TMDL syntax
- Validate DAX measure count matches config
- Validate report page count and visual types
- Integration tests against live Fabric workspace

### 8.10 @industry-designer

**Owns:** `industries/*/` config files
**Tools:** read, edit, search, execute, todo

**Responsibilities:**
- Design industry-specific data schemas
- Define business KPIs and DAX measure specifications
- Write company stories and demo scenarios
- Define web enrichment API sources
- Design HTAP event stream scenarios
- Ensure domain accuracy (correct industry terminology, realistic data ranges)

---

## 9. Implementation Phases

### Phase 1 — Core Framework (Sprint 1–3)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S1 | Project scaffolding, config schemas, config loader | @orchestrator |
| S1 | Template engine (`.tpl` → output files) | @orchestrator |
| S1 | `industry.json` + `sample-data.json` schema validation | @orchestrator |
| S2 | CSV generator (from sample-data.json) | @data-engineer |
| S2 | Notebook generator (from templates) | @data-engineer |
| S2 | Dataflow generator | @data-engineer |
| S3 | TMDL generator (tables, measures, relationships) | @semantic-model |
| S3 | PBIR report generator (pages, visuals, theme) | @report-builder |
| S3 | Core pytest test suite | @tester |

### Phase 2 — Horizon Books Migration (Sprint 4–5)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S4 | Extract Horizon Books configs from existing project | @industry-designer |
| S4 | Validate generation matches existing demo output | @tester |
| S5 | Deploy generator (PS1 from templates) | @deployer |
| S5 | Pester test generator | @tester |
| S5 | End-to-end: `generate.ps1 -Industry horizon-books` → identical output | @orchestrator |

### Phase 3 — Forecasting & Planning (Sprint 6–7)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S6 | Forecast generator (Holt-Winters notebook + MLflow) | @forecaster |
| S6 | Planning generator (IQ tables + scenarios) | @forecaster |
| S7 | Forecast/Planning integration with all industries | @forecaster |
| S7 | Planning report pages | @report-builder |

### Phase 4 — Transactional Analytics / HTAP (Sprint 8–9)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S8 | HTAP config schema (`htap-config.json`) | @htap-engineer |
| S8 | Eventhouse + KQL generator | @htap-engineer |
| S8 | Event simulator notebook (NB05) | @htap-engineer |
| S9 | HTAP report pages (real-time dashboards) | @report-builder |
| S9 | Hot-cold bridge (KQL ↔ Lakehouse) | @htap-engineer |
| S9 | HTAP deployment (Eventhouse via REST API) | @deployer |

### Phase 5 — Contoso Energy Demo (Sprint 10–12)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S10 | Energy industry configs (industry.json, sample-data.json) | @industry-designer |
| S10 | Energy CSV data generation (20 files) | @data-engineer |
| S11 | Energy semantic model (110+ measures) | @semantic-model |
| S11 | Energy Power BI reports (12 + 5 + 3 pages) | @report-builder |
| S12 | Energy HTAP (grid telemetry, SCADA events) | @htap-engineer |
| S12 | Energy forecasting (5 models) | @forecaster |
| S12 | Energy deployment + validation | @deployer + @tester |

### Phase 6 — Northwind HR/Finance Demo (Sprint 13–15)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S13 | HR/Finance industry configs | @industry-designer |
| S13 | HR/Finance CSV data (22 files) | @data-engineer |
| S14 | HR/Finance semantic model (130+ measures) | @semantic-model |
| S14 | HR/Finance reports (14 + 5 + 3 pages) | @report-builder |
| S15 | HR/Finance HTAP (payroll, GL, recruitment streams) | @htap-engineer |
| S15 | HR/Finance deployment + validation | @deployer + @tester |

### Phase 7 — Fabrikam Manufacturing Demo (Sprint 16–18)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S16 | Manufacturing configs | @industry-designer |
| S16 | Manufacturing CSV data (25 files) | @data-engineer |
| S17 | Manufacturing semantic model (120+ measures) | @semantic-model |
| S17 | Manufacturing reports (12 + 5 + 3 pages) | @report-builder |
| S18 | Manufacturing HTAP (PLC telemetry, SPC, IoT sensors) | @htap-engineer |
| S18 | Manufacturing deployment + validation | @deployer + @tester |

### Phase 8 — Polish & Cross-Industry (Sprint 19–20)

| Sprint | Deliverable | Agent Lead |
|--------|-------------|------------|
| S19 | Cross-industry comparison dashboard | @report-builder |
| S19 | Demo wizard (interactive selection) | @orchestrator |
| S19 | Documentation (README, ARCHITECTURE, AGENTS) | @orchestrator |
| S20 | CI/CD pipeline (GitHub Actions) | @tester |
| S20 | Performance optimization (generation < 60s per demo) | All agents |
| S20 | Final validation: all 4 demos deploy cleanly | @deployer + @tester |

---

## 10. Testing Strategy

### 10.1 Test Pyramid

```
                    ┌──────────┐
                    │Integration│  Live Fabric workspace
                    │  Tests    │  (Pester, tag:Integration)
                    ├──────────┤
                    │  Industry │  Per-demo validation
                    │  Tests    │  (Pester, tag:Industry)
                    ├──────────┤
                    │  Core Gen │  Generator unit tests
                    │  Tests    │  (pytest)
                    ├──────────┤
                    │  Config   │  JSON schema validation
                    │Validation │  (pytest)
                    └──────────┘
```

### 10.2 Test Coverage Targets

| Layer | Framework | Target | Scope |
|-------|-----------|--------|-------|
| Config validation | pytest | 100% | All industry JSON configs valid against schemas |
| Core generators | pytest | 90%+ | csv_generator, notebook_generator, tmdl_generator, etc. |
| Industry tests | Pester | Per-demo | CSV counts, TMDL measures, report pages, deploy scripts |
| Integration | Pester | Smoke | Deploy to test workspace, validate all artifacts exist |

### 10.3 Test Matrix Per Industry

| Check | Horizon Books | Contoso Energy | Northwind HR | Fabrikam Mfg |
|-------|:---:|:---:|:---:|:---:|
| CSV file count | 17 | 20 | 22 | 25 |
| CSV referential integrity | ✔ | ✔ | ✔ | ✔ |
| TMDL tables | 23 | 28 | 30 | 32 |
| DAX measures | 96 | 110 | 130 | 120 |
| Relationships | 27 | 32 | 38 | 35 |
| Report pages (Analytics) | 10 | 12 | 14 | 12 |
| Report pages (Forecast) | 5 | 5 | 5 | 5 |
| Report pages (HTAP) | 3 | 3 | 3 | 3 |
| Forecast models | 5 | 5 | 5 | 5 |
| Planning models | 5 | 5 | 5 | 5 |
| HTAP event streams | — | 4 | 4 | 4 |
| Notebooks | 5 | 6 | 6 | 6 |
| Deployment steps | 12 | 14 | 14 | 14 |

---

## 11. CI/CD

### 11.1 GitHub Actions Workflows

**Workflow 1: `ci-tests.yml`** — On every push/PR
```yaml
jobs:
  config-validation:
    # Validate all industry JSON configs against schemas
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pytest tests/core/ -v

  industry-pester:
    # Per-industry offline Pester tests
    runs-on: windows-latest
    strategy:
      matrix:
        industry: [horizon-books, contoso-energy, northwind-hrfinance, fabrikam-manufacturing]
    steps:
      - uses: actions/checkout@v4
      - run: |
          .\generate.ps1 -Industry "${{ matrix.industry }}" -OutputDir "output/${{ matrix.industry }}"
          Invoke-Pester -Path "tests/industries/${{ matrix.industry }}.Tests.ps1" -ExcludeTag Integration
```

**Workflow 2: `generate-demo.yml`** — On-demand via workflow_dispatch
```yaml
on:
  workflow_dispatch:
    inputs:
      industry:
        description: 'Industry demo to generate'
        required: true
        type: choice
        options: [horizon-books, contoso-energy, northwind-hrfinance, fabrikam-manufacturing, all]
```

---

## Summary

This plan transforms the single-industry Horizon Books demo into a **multi-industry demo factory** with:

- **4 industry demos** (Publishing, Energy, HR/Finance, Manufacturing)
- **9+1 specialized agents** with clear ownership, handoff protocols, and shared constraints
- **Configuration-driven generation** — add a new industry by adding JSON configs
- **Transactional Analytics (HTAP)** — Eventhouse + KQL + real-time dashboards
- **Forecasting + Planning** — Holt-Winters + MLflow + Fabric IQ
- **One-command deployment** — `Deploy-Full.ps1 -WorkspaceId "<guid>" -Industry "contoso-energy"`
- **Comprehensive testing** — pytest + Pester across all 4 demos
- **20-sprint roadmap** with clear agent ownership per deliverable

The architecture ensures that adding a 5th or 6th industry demo requires only writing new JSON config files — no code changes to the core generators.
