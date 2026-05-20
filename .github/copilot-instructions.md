<!-- Copilot instructions for the Microsoft Fabric to End-to-End Demo migration project -->

# Project: Microsoft Fabric to End-to-End Demo Migration

Automated migration of Microsoft Fabric artifacts to End-to-End Demo format.

## Architecture — Pipeline

```
Microsoft Fabric → End-to-End Demo
```

## Project Structure

- **Source / Extraction**: `src/`
- **Target / Generation**: `output/`
- **Tests**: `tests/` (33 test files)
- **Docs**: `docs/`

## Key Modules

- **Generation**:
  - `core\activator_generator.py`
  - `core\agent_generator.py`
  - `core\comparison_generator.py`
  - `core\copilot_generator.py`
  - `core\csv_generator.py`
  - `core\dataflow_generator.py`
  - `core\deploy_generator.py`
  - `core\forecast_generator.py`
  - `core\htap_generator.py`
  - `core\mirroring_generator.py`
  - `core\notebook_generator.py`
  - `core\pester_generator.py`
  - `core\pipeline_generator.py`
  - `core\planning_generator.py`
  - `core\report_generator.py`
  - ... and 7 more
- **Assessment**:
  - `core\validator.py`
- **Utilities**:
  - `core\__init__.py`
  - `core\config_loader.py`
  - `core\report_designer.py`
  - `core\template_engine.py`
  - `output\adventure-works\UserDataFunction\function_app.py`
  - `output\adventure-works\notebooks\01_BronzeToSilver.py`
  - `output\adventure-works\notebooks\02_WebEnrichment.py`
  - `output\adventure-works\notebooks\03_SilverToGold.py`
  - `output\adventure-works\notebooks\04_Forecasting.py`
  - `output\adventure-works\notebooks\05_EventSimulator.py`
  - `output\adventure-works\notebooks\06_DiagnosticCheck.py`
  - `output\adventure-works\notebooks\07_WritebackSetup.py`
  - `output\adventure-works\notebooks\08_WritebackAPI.py`
  - `output\adventure-works\notebooks\09_SQLDatabaseSetup.py`
  - `output\contoso-energy\UserDataFunction\function_app.py`
  - ... and 69 more

## Hard Constraints

1. **Read before write** — never assume file contents from memory
2. **Test after every change** — run `pytest tests/ --tb=short -q`
3. **No duplicate functions** — always search for an existing name before creating one
4. **Git hygiene** — commit only when tests pass, conventional messages (`feat:`, `fix:`, `test:`, `docs:`)

## Multi-Agent Architecture

This project uses a specialized agent architecture. See `docs/AGENTS.md` for the full
architecture diagram and `.github/agents/` for per-agent definitions.

## Workflow Rules

### 1. Plan Before Build
- For multi-step work, create a plan before starting
- If something goes sideways, STOP and re-plan

### 2. Read Before Write
- **Always read target code before editing**
- Read `copilot-instructions.md` at session start for project rules

### 3. Testing Contract
- Run `pytest tests/ --tb=short -q` after EVERY implementation change
- If tests fail → fix them before reporting completion
- New features **require** new tests
- Never weaken test assertions to make tests pass

### 4. Scope Discipline
- Only modify files directly related to the task
- No drive-by refactors
- Prefer the smallest change that solves the problem
