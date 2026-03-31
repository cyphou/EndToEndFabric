# Multi-Agent Architecture

This document describes the **9+1 specialized agents** used for AI-assisted development of the Fabric End-to-End Industry Demo Generator.

Agent definitions live in `.github/agents/` and are inherited by GitHub Copilot when working in this repository.

---

## Agent Overview

```
┌─────────────────────────────────────────────────────────┐
│                    @SHARED CONSTRAINTS                   │
│   Hard rules, coding standards, file ownership model     │
└─────────────────────────────────────────────────────────┘
          ↓ inherited by all agents ↓
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│@ORCHESTRA│ │@DATA-ENG │ │@SEMANTIC │ │@REPORT   │
│  -TOR    │ │  INEER   │ │  -MODEL  │ │  -BUILDER│
└──────────┘ └──────────┘ └──────────┘ └──────────┘
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│@FORECAST │ │@HTAP     │ │@DEPLOYER │ │@TESTER   │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
                    ┌──────────┐
                    │@INDUSTRY │
                    └──────────┘
```

---

## Agent Roles & Ownership

| Agent | Definition File | Owns | Responsibilities |
|-------|-----------------|------|------------------|
| **@orchestrator** | `orchestrator.agent.md` | `generate.py`, `generate.ps1`, top-level configs | CLI pipeline coordination, config loading, step sequencing, `--wizard`, `--compare` |
| **@data-engineer** | `data-engineer.agent.md` | `core/csv_generator.py`, `core/notebook_generator.py`, `core/dataflow_generator.py`, `templates/notebooks/` | Sample CSV data, PySpark notebooks, Dataflow Gen2 configs |
| **@semantic-model** | `semantic-model.agent.md` | `core/tmdl_generator.py`, `templates/tmdl/` | TMDL table definitions, DAX measures, relationships |
| **@report-builder** | `report-builder.agent.md` | `core/report_generator.py`, `core/comparison_generator.py`, `templates/reports/` | PBIR v4.0 pages, visuals, themes, cross-industry comparison |
| **@forecaster** | `forecaster.agent.md` | `core/forecast_generator.py`, `core/planning_generator.py` | Holt-Winters models, MLflow tracking, Planning IQ tables |
| **@htap-engineer** | `htap-engineer.agent.md` | `core/htap_generator.py`, `templates/kql/` | Eventhouse, KQL database, event simulator, hot-cold bridge |
| **@deployer** | `deployer.agent.md` | `core/deploy_generator.py`, `core/writeback_generator.py`, `core/udf_generator.py`, `shared/deploy/`, `templates/deploy/` | PowerShell deployment scripts, Fabric REST API, writeback notebooks, User Data Functions |
| **@tester** | `tester.agent.md` | `core/pester_generator.py`, `core/test_generator.py`, `tests/` | pytest + Pester test suites, validation, performance benchmarks |
| **@industry-designer** | `industry-designer.agent.md` | `industries/*/` config files (10 per industry) | Domain schemas, KPIs, company stories, data-agent configs |
| **@shared** | `shared.instructions.md` | Cross-cutting constraints | Hard rules inherited by all agents |

---

## Shared Constraints (@shared)

All agents inherit these hard rules:

1. **Configuration-driven** — Industry-specific behavior comes from `industries/<id>/*.json`, never hard-coded.
2. **Idempotent generation** — Re-running `generate.py` produces identical output for the same config + seed.
3. **Zero external deps for core** — Python stdlib only (`csv`, `json`, `os`, `pathlib`).
4. **Read before write** — Never assume file contents; always load config first.
5. **Test after change** — Run `python -m pytest tests/ -v` after every modification.
6. **Template discipline** — Templates use `{{PLACEHOLDER}}` syntax; never raw string concatenation.
7. **Schema validation** — All JSON configs validated against `core/schemas/*.json` before generation.

---

## Naming Conventions

| Artifact | Pattern | Example |
|----------|---------|---------|
| Lakehouse | `BronzeLH`, `SilverLH`, `GoldLH` | — |
| Notebooks | `NB0{n}_{Description}` | `NB01_BronzeToSilver` |
| Dataflows | `DF_{Domain}` | `DF_Generation`, `DF_Billing` |
| Pipeline | `PL_{CompanyName}_Orchestration` | `PL_ContosoEnergy_Orchestration` |
| Reports | `{CompanyName}-{Type}` | `ContosoEnergy-Analytics` |
| Semantic Model | `{CompanyName}Model` | `ContosoEnergyModel` |

---

## Adding a New Agent

1. Create `.github/agents/<name>.agent.md` with YAML frontmatter
2. Define the agent's **owns** (files/directories), **tools**, and **responsibilities**
3. Add the agent to the `.github/agents/shared.instructions.md` agent list
4. Update this document
5. Add corresponding generator module in `core/` if needed
6. Add unit tests in `tests/core/`

---

## Agent Interaction Model

Agents have **clear ownership boundaries** to prevent conflicting edits:

- Each file/directory is owned by exactly one agent
- The **@orchestrator** coordinates multi-agent sequences
- The **@shared** constraints are automatically inherited by all agents
- When an agent needs output from another agent's domain, it reads (never writes) that domain's config files

This model ensures safe concurrent development across agents without merge conflicts.
