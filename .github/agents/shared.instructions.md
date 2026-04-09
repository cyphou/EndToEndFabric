---
applies_to: all_agents
---

# Shared Constraints — All Agents

These rules are **mandatory** for every agent in the FabricEndtoEnd project.

## Hard Rules

1. **Configuration-driven** — All industry-specific behavior comes from `industries/<id>/*.json`. Never hard-code industry data in generators.
2. **Idempotent generation** — Re-running `generate.py -i <id>` with the same configs must produce identical output.
3. **No external dependencies for core** — Python stdlib only (`csv`, `json`, `os`, `pathlib`, `string`, `re`, `datetime`, `random`, `math`). Optional deps only for testing or CLI enhancements.
4. **Read before write** — Always load config files before generating. Never assume file contents from memory.
5. **Test after every change** — Run `pytest tests/core/ -q` after modifying any core module. Run Pester after modifying deploy scripts.
6. **Git hygiene** — Conventional commits: `feat(energy):`, `fix(core):`, `test(hrfinance):`, `docs:`.
7. **Template discipline** — Templates use `{{PLACEHOLDER}}` syntax. Never use raw string concatenation for multi-line artifacts.
8. **Schema validation** — All JSON configs are validated against `core/schemas/*.json` before generation.
9. **No PEM files** — Never generate, store, or commit `.pem` certificate files. Use managed identity or token-based auth for all service connections.

## Naming Conventions

| Artifact | Pattern | Example |
|----------|---------|---------|
| Lakehouse | `BronzeLH`, `SilverLH`, `GoldLH` | Fixed names, all demos |
| Notebook | `NB0X_<Name>` | `NB01_BronzeToSilver` |
| Dataflow | `DF_<Domain>` | `DF_Generation`, `DF_Billing` |
| Pipeline | `PL_<CompanyName>_Orchestration` | `PL_ContosoEnergy_Orchestration` |
| Report | `<CompanyName>-<Type>` | `ContosoEnergy-Analytics` |
| SemanticModel | `<CompanyName>Model` | `ContosoEnergyModel` |
| Eventhouse | `RT_<Prefix>_Events` | `RT_ContosoEnergy_Events` |

## File Ownership

- **One owner per module** — Only the owning agent modifies it.
- **Universal read access** — Any agent can read any file for context.
- **@tester is special** — Reads all source, writes only to `tests/`.

## Agent Registry

| Agent | File | Primary domain |
|-------|------|----------------|
| @orchestrator | `orchestrator.agent.md` | CLI pipeline, config loading |
| @data-engineer | `data-engineer.agent.md` | CSV data, PySpark notebooks, Dataflows |
| @semantic-model | `semantic-model.agent.md` | TMDL, DAX measures, relationships |
| @report-builder | `report-builder.agent.md` | PBIR v4.0 pages, visuals, themes |
| @report-designer | `report-designer.agent.md` | Logo, color harmony, layout grid, screenshot validation |
| @forecaster | `forecaster.agent.md` | Holt-Winters, MLflow, Planning IQ |
| @htap-engineer | `htap-engineer.agent.md` | Eventhouse, KQL, event simulator |
| @deployer | `deployer.agent.md` | PowerShell deploy scripts, Fabric REST, UDF |
| @tester | `tester.agent.md` | pytest + Pester, benchmarks |
| @industry-designer | `industry-designer.agent.md` | Domain schemas, KPIs, company stories |
| @validator | `validator.agent.md` | Structure, TMDL, cross-refs, placeholder hygiene |

## Handoff Protocol

1. Complete your part within your file scope.
2. State what needs to happen next.
3. Name the target agent (e.g., "Hand off to @semantic-model for TMDL generation").
4. List affected files and data structures.

## Python Conventions

- Python 3.12+ only
- Use pathlib.Path for all file operations
- Use `with open(..., encoding="utf-8")` for all file I/O
- Use type hints for function signatures
- Prefer smallest change that solves the problem
