# Contributing

Thank you for your interest in improving the **Fabric End-to-End Industry Demo Generator**.

---

## Prerequisites

| Tool | Version | Required For |
|---|---|---|
| Python | 3.12+ | Core engine, tests |
| PowerShell | 5.1+ | `generate.ps1`, deploy scripts |
| pytest | 8+ | Running test suite |

No external Python packages are required for core generation.

---

## Project Layout

```
FabricEndtoEnd/
├── generate.py              # CLI entry point
├── core/                    # Generator modules (one per pipeline step)
├── industries/              # Per-industry JSON configs
├── templates/               # .tpl template files
├── tests/core/              # pytest unit tests
├── docs/                    # Documentation + PNG diagrams
├── shared/                  # Shared deployment templates
└── .github/agents/          # Agent definitions
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for full details.

---

## Development Workflow

### 1. Clone and verify

```bash
git clone <repo-url>
cd FabricEndtoEnd
python -m pytest tests/core/ -v
```

All 73 tests should pass before making changes.

### 2. Make changes

- **New generator feature** → edit the relevant module in `core/`
- **New industry** → add a folder under `industries/<id>/` with 7 JSON configs
- **Template change** → edit `.tpl` files under `templates/`
- **New test** → add to the corresponding `tests/core/test_<module>.py`

### 3. Run tests

```bash
python -m pytest tests/core/ -v
```

### 4. Validate generation

```bash
# Generate all 3 industries and check for errors
python generate.py -i horizon-books
python generate.py -i contoso-energy
python generate.py -i northwind-hrfinance
```

### 5. Submit a pull request

- Describe the change and which pipeline steps are affected
- Ensure all tests pass
- If adding a new industry, include all 7 JSON config files

---

## Adding a New Industry

1. Create `industries/<new-id>/`
2. Author these 7 JSON config files:

| File | Purpose |
|---|---|
| `industry.json` | Company name, domains, theme colors, artifact names |
| `sample-data.json` | Table schemas, column types, FK references |
| `semantic-model.json` | TMDL tables, DAX measures, relationships |
| `reports.json` | Report pages, visuals, field mappings |
| `forecast-config.json` | Forecast models and parameters |
| `planning-config.json` | Planning tables, scenarios, growth rates |
| `htap-config.json` | Eventhouse, KQL database, event streams |

3. Validate configs match the JSON schemas in `core/schemas/`
4. Run `python generate.py -i <new-id>` to verify all 10 steps succeed
5. Add tests if introducing new column types or visual types

---

## Adding a New Generator Step

1. Create `core/<new>_generator.py` with a `generate_<new>(...)` function
2. Add a corresponding `.tpl` template in `templates/<new>/` if needed
3. Wire the step into `generate.py`'s main pipeline
4. Add unit tests in `tests/core/test_<new>_generator.py`
5. Update the agent definition in `.github/agents/` if applicable

---

## Code Style

- **No external dependencies** for core generation (stdlib only)
- Type hints on public functions
- Use `pathlib.Path` for all file operations
- Template placeholders: `{{UPPER_SNAKE_CASE}}`
- JSON configs follow the schemas in `core/schemas/`

---

## Regenerating Documentation PNGs

```bash
pip install matplotlib pillow
python docs/generate_diagrams.py
```

This regenerates all 10 PNG diagrams in `docs/images/`.

---

## Test Guidelines

- Tests use `unittest.TestCase` via pytest
- Each generator module has a corresponding test file
- Use `tmp_path` fixture for output isolation
- Seed-based tests should verify determinism (same seed → same output)
- FK integrity tests should verify child values exist in parent tables
