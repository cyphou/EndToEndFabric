# Architecture

<p align="center">
  <img src="images/pipeline-architecture.png" alt="Pipeline Architecture" width="100%">
</p>

This document describes the architecture of the **Fabric End-to-End Industry Demo Generator** — a config-driven Python engine that produces complete Microsoft Fabric demo projects from industry JSON configuration files.

---

## Design Principles

1. **Zero external dependencies** — The core engine runs on Python 3.12+ stdlib only.
2. **Config-driven** — Industries are defined entirely by 7 JSON config files. No code changes to add a new vertical.
3. **Template-based** — `.tpl` files under `templates/` provide artifact skeletons rendered via `{{PLACEHOLDER}}` substitution.
4. **Deterministic** — A `--seed` flag guarantees reproducible sample data across runs.
5. **Modular** — Each generation step is an independent module. Steps can be skipped via CLI flags.

---

## High-Level Flow

```
                        ┌─────────────┐
                        │  generate.py │   CLI entry point
                        │ (Orchestrator)│
                        └──────┬──────┘
                               │
                    ┌──────────▼──────────┐
                    │  config_loader.py   │   Loads + validates 7 JSON configs
                    │  (+ JSON Schemas)   │   against core/schemas/*.json
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                     │
    ┌─────▼─────┐       ┌─────▼─────┐        ┌─────▼─────┐
    │ industry   │       │ sample-   │        │ semantic- │
    │ .json      │       │ data.json │        │ model.json│
    └─────┬─────┘       └─────┬─────┘        └─────┬─────┘
          │                    │                     │
          ▼                    ▼                     ▼
    ┌───────────────────────────────────────────────────┐
    │          12-Step Generation Pipeline               │
    │                                                    │
    │  1. Config Loader     7. Pipeline Generator        │
    │  2. CSV Generator     8. Forecast Generator        │
    │  3. Notebook Gen      9. HTAP Generator            │
    │  4. Dataflow Gen     10. Writeback Generator      │
    │  5. TMDL Generator   11. Data Agent Generator      │
    │  6. Report Generator 12. Deploy Generator          │
    └───────────────────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │    output/<industry> │
                    │  Complete Fabric Demo│
                    └─────────────────────┘
```

---

## Medallion Lakehouse Pattern

<p align="center">
  <img src="images/medallion-architecture.png" alt="Medallion Architecture" width="100%">
</p>

Generated notebooks follow the **Bronze → Silver → Gold** medallion pattern:

| Layer | Notebook | Transformations |
|---|---|---|
| **Bronze** | NB01 | Raw CSV to Lakehouse Delta tables. No data transformations. Schema-on-read. |
| **Silver** | NB01 | Type casting, null handling, deduplication, domain-specific validation. |
| **Gold** | NB03 | Star schema with Dim/Fact tables. DimDate generation. Aggregated measures. |

**Supporting notebooks:**
- **NB02** — Web enrichment (external API data injection)
- **NB04** — Holt-Winters forecasting with MLflow experiment tracking
- **NB05** — HTAP event simulator (real-time streaming data generation)
- **NB06** — Diagnostic (table inventory, null audit, row counts, schema validation)
- **NB07** — Writeback Setup (creates writeback schema + Delta tables)
- **NB08** — Writeback API (REST API-callable notebook for upserts)

---

## Module Reference

### Entry Points

| File | Role |
|---|---|
| `generate.py` | CLI orchestrator — parses args, loads configs, runs all 12 steps |
| `generate.ps1` | PowerShell wrapper around `generate.py` |

### Core Generators (`core/`)

| Module | Step | Input Configs | Output |
|---|:---:|---|---|
| `config_loader.py` | 1 | `industries/<id>/*.json` | Validated config dict |
| `csv_generator.py` | 2 | `sample-data.json` | CSV files in `SampleData/` |
| `notebook_generator.py` | 3 | `industry.json` + `sample-data.json` | PySpark `.py` notebooks |
| `dataflow_generator.py` | 4 | `industry.json` + `sample-data.json` | Dataflow Gen2 JSON configs |
| `tmdl_generator.py` | 5 | `industry.json` + `semantic-model.json` | TMDL files (tables, measures, relationships) |
| `report_generator.py` | 6 | `industry.json` + `reports.json` | PBIR v4.0 report structure |
| `pipeline_generator.py` | 7 | `industry.json` + `sample-data.json` | Fabric Pipeline JSON |
| `forecast_generator.py` | 8 | `industry.json` + `forecast-config.json` | Holt-Winters notebook + config |
| `htap_generator.py` | 9 | `industry.json` + `htap-config.json` | Eventhouse, KQL, event simulator |
| `writeback_generator.py` | 10 | `industry.json` + `writeback-config.json` | NB07/NB08 writeback notebooks |
| `deploy_generator.py` | 12 | `industry.json` | PowerShell deploy scripts |
| `agent_generator.py` | 11 | `industry.json` + `data-agent.json` | Fabric Data Agent config + README |

### Supporting Modules

| Module | Purpose |
|---|---|
| `template_engine.py` | `{{PLACEHOLDER}}`, `{{#if}}`, `{{#each}}` template rendering |
| `planning_generator.py` | Planning IQ table schemas + scenario notebooks |
| `pester_generator.py` | Pester 5 test suite for deployment validation |
| `pester_generator.py` | Pester 5 test suite for deployment validation |

### Templates (`templates/`)

```
templates/
├── deploy/         # Deploy-Full.ps1.tpl, Upload-SampleData.ps1.tpl, Validate-Deployment.ps1.tpl
├── kql/            # eventhouse.kql.tpl, kql-database.kql.tpl, bridge.kql.tpl
├── notebooks/      # NB01–NB06 PySpark templates (.py.tpl)
├── reports/        # PBIR report.json, page.json, visual.json templates
└── tmdl/           # model.tmdl.tpl, table.tmdl.tpl, relationship.tmdl.tpl
```

Templates use `{{PLACEHOLDER}}` syntax for simple substitutions and `{{#each items}}...{{/each}}` for iteration. The `template_engine.py` module handles all rendering.

### Config Validation (`core/schemas/`)

| Schema | Validates |
|---|---|
| `industry_schema.json` | Company identity, domains, theme, artifact naming |
| `sample_data_schema.json` | Table definitions, columns, types, FK references |
| `semantic_model_schema.json` | Tables, measures, relationships, expressions |

Validation runs at Step 1 before any generation begins.

---

## Config-Driven Design

<p align="center">
  <img src="images/config-driven-design.png" alt="Config-Driven Design" width="100%">
</p>

Each industry lives in `industries/<id>/` with up to 8 JSON files:

| Config File | Contents |
|---|---|
| `industry.json` | Company name, domains, theme colors, Fabric item names |
| `sample-data.json` | Table schemas, column types, row counts, FK relationships |
| `semantic-model.json` | TMDL tables, DAX measures, relationship definitions |
| `reports.json` | Report pages, visual types, field mappings, layout |
| `forecast-config.json` | Forecast models, parameters (alpha/beta/gamma), horizons |
| `planning-config.json` | Planning tables, scenarios, growth rates |
| `htap-config.json` | Eventhouse, KQL database, event stream definitions |
| `web-enrichment.json` | External API sources for Silver-layer enrichment |

### Adding a New Industry

1. Create `industries/<new-id>/`
2. Author the 8 JSON config files (or copy/modify from an existing industry)
3. Run `python generate.py -i <new-id>`
4. All 12 pipeline steps produce output tailored to the new industry

No Python code changes required.

---

## Multi-Agent Architecture

<p align="center">
  <img src="images/multi-agent-architecture.png" alt="Multi-Agent Architecture" width="100%">
</p>

The project defines **9+1 specialized agents** in `.github/agents/` for AI-assisted development:

| Agent | File | Expertise |
|---|---|---|
| **Orchestrator** | `orchestrator.agent.md` | CLI pipeline, config loading |
| **Data Engineer** | `data-engineer.agent.md` | CSV, PySpark notebooks, Dataflow Gen2 |
| **Semantic Model** | `semantic-model.agent.md` | TMDL tables, DAX measures, relationships |
| **Report Builder** | `report-builder.agent.md` | PBIR pages, visuals, themes |
| **Forecaster** | `forecaster.agent.md` | Holt-Winters, MLflow, Planning IQ |
| **HTAP Engineer** | `htap-engineer.agent.md` | Eventhouse, KQL, event simulator |
| **Deployer** | `deployer.agent.md` | PowerShell deploy scripts |
| **Tester** | `tester.agent.md` | pytest, Pester test suites |
| **Industry Designer** | `industry-designer.agent.md` | New industry config authoring |
| **Shared** | `shared.instructions.md` | Hard constraints applied to all agents |

Each agent has clear ownership boundaries, preventing conflicting edits during multi-agent sessions.

---

## Output Artifact Map

<p align="center">
  <img src="images/output-structure.png" alt="Output Structure" width="100%">
</p>

```
output/<industry>/
├── SampleData/           ← Step 2: CSV Generator
│   ├── <domain>/         # Domain-organized CSV files
│   └── ...
├── Notebooks/            ← Step 3: Notebook Generator
│   ├── NB01_Bronze_to_Silver.py
│   ├── NB02_Web_Enrichment.py
│   ├── NB03_Silver_to_Gold.py
│   ├── NB06_Diagnostic.py
│   ├── NB07_WritebackCapture.py
│   └── NB08_WritebackApply.py
├── Dataflows/            ← Step 4: Dataflow Generator
│   └── DF_<domain>_ingestion.json
├── SemanticModel/        ← Step 5: TMDL Generator
│   ├── model.tmdl
│   ├── tables/
│   ├── relationships/
│   └── definition.pbism
├── Reports/              ← Step 6: Report Generator
│   └── <Report>-Analytics/
│       ├── report.json
│       ├── pages/
│       └── theme.json
├── Pipeline/             ← Step 7: Pipeline Generator
│   └── pipeline-content.json
├── Forecast/             ← Step 8: Forecast Generator
│   ├── NB04_Forecast.py
│   └── forecast-config.json
├── HTAP/                 ← Step 9: HTAP Generator
│   ├── eventhouse-definition.json
│   ├── kql-database-script.kql
│   ├── NB05_EventSimulator.py
│   └── bridge-queries.kql
├── Writeback/            ← Step 10: Writeback Generator
│   ├── writeback-config.json
│   ├── NB07_WritebackSetup.py
│   ├── NB08_WritebackAPI.py
│   └── stored_procedures/
├── DataAgent/            ← Step 11: Agent Generator
│   ├── agent-config.json
│   └── README.md
└── Deploy/               ← Step 12: Deploy Generator
    ├── Deploy-Full.ps1
    ├── <Company>.psm1
    ├── Upload-SampleData.ps1
    └── Validate-Deployment.ps1
```

---

## Data Flow Patterns

### FK Integrity in Sample Data

The CSV generator builds tables in **dependency order** — parent tables first, then child tables that reference them via foreign keys. Each FK column is populated exclusively from values in the referenced parent table, ensuring referential integrity without a database.

### Template Rendering Pipeline

```
.tpl file → template_engine.render() → populated artifact file
                    │
        Substitutions:
        - {{COMPANY_NAME}}   → "Contoso Energy"
        - {{LAKEHOUSE_NAME}} → "lh_contoso_energy"
        - {{#each tables}}   → iterates table list
        - {{#if has_dates}}   → conditional block
```

### TMDL Generation

The semantic model generator converts `semantic-model.json` definitions into TMDL files:

1. `model.tmdl` — Root model with culture, default Power BI data source
2. `tables/<name>.tmdl` — Column definitions, partitions (Direct Lake expressions)
3. `relationships/<name>.tmdl` — One-to-many cardinality, cross-filter direction
4. `measures` — DAX measure expressions embedded in their parent table's TMDL file
5. `definition.pbism` — Pointer to the semantic model folder

---

## Technology Stack

<p align="center">
  <img src="images/tech-stack.png" alt="Technology Stack" width="100%">
</p>

| Layer | Technology | Notes |
|---|---|---|
| **Core Engine** | Python 3.12+ | Zero external deps for generation |
| **Generated Notebooks** | PySpark | Runs in Fabric Spark compute |
| **Dataflows** | Power Query M | Fabric Dataflow Gen2 |
| **Semantic Model** | TMDL | Direct Lake mode |
| **Reports** | PBIR v4.0 | Power BI enhanced report format |
| **Deployment** | PowerShell 5.1+ | Fabric REST API automation |
| **Testing** | pytest + Pester 5 | Python unit tests + PowerShell validation |

---

## Testing Architecture

```
tests/
├── core/
│   ├── test_config_loader.py        # Config load, validation, schema errors
│   ├── test_csv_generator.py        # FK integrity, determinism, edge cases
│   ├── test_report_generator.py     # PBIR pages, visuals, themes
│   ├── test_template_engine.py      # {{if}}, {{each}}, nested rendering
│   ├── test_tmdl_generator.py       # Tables, measures, relationships
│   ├── test_dataflow_generator.py   # Dataflow Gen2 per-domain configs
│   ├── test_agent_generator.py      # Data Agent config + README generation
│   └── ...                          # Additional generator tests
├── industries/
│   └── test_per_industry_generation.py  # PLAN.md §10.3 target validation
└── integration/
    └── test_full_pipeline.py        # End-to-end pipeline + idempotency
```

**213+ tests** (plus 80+ subtests), all passing. Tests cover:
- Config loading with invalid inputs (missing fields, bad schemas)
- CSV generation determinism (same seed → same output)
- FK integrity validation (child references match parent values)
- Template rendering edge cases (missing vars, nested blocks)
- TMDL schema correctness (valid Direct Lake expressions)

Run: `python -m pytest tests/ -v`

---

## Security Notes

- All file paths are validated before writing (no directory traversal)
- JSON configs are validated against schemas before processing
- No network calls during generation — everything runs locally
- Generated deploy scripts use Azure AD service principal authentication
- No secrets are stored in generated artifacts
