"""Output artifact validator.

Post-generation validation of all published artifacts:
structure completeness, metadata correctness, JSON schema
compliance, TMDL integrity, cross-artifact references, and
placeholder hygiene.
"""

import csv
import json
import re
from pathlib import Path
from typing import NamedTuple


class ValidationResult(NamedTuple):
    """Single validation finding."""
    severity: str   # "ERROR", "WARNING", "INFO"
    category: str   # e.g. "structure", "metadata", "tmdl", "cross-ref", "placeholder"
    artifact: str   # e.g. "ContosoEnergy-Analytics.Report"
    message: str


# ── Known PBIR schema URLs ──────────────────────────────────────────────
_EXPECTED_SCHEMAS = {
    "report.json": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
    "page.json": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
    "visual.json": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
    "version.json": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
    "definition.pbir": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
}

# Known placeholder tokens
_KNOWN_PLACEHOLDERS = {
    "WORKSPACE_ID", "SEMANTIC_MODEL_ID", "GOLD_LH_ID", "SILVER_LH_ID",
    "BRONZE_LH_ID", "SQLDB_ID", "SQLDB_SERVER", "SQLDB_NAME",
    "EVENTHOUSE_ID", "KQL_DB_ID", "ENVIRONMENT_ID",
    "ALERT_RECIPIENTS",
    "SOURCE_WORKSPACE_ID", "SOURCE_LAKEHOUSE_ID",
    "ADLS_CONNECTION_ID", "STORAGE_ACCOUNT", "CONTAINER",
    "S3_CONNECTION_ID", "S3_BUCKET", "AWS_REGION",
    "SQL_SERVER", "SQL_DATABASE", "SQL_USERNAME", "SQL_PASSWORD",
    "AZURE_SQL_SERVER", "AZURE_SQL_DATABASE",
    "COSMOS_ENDPOINT", "COSMOS_DATABASE",
    "PG_SERVER", "PG_DATABASE",
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
}
_PLACEHOLDER_RE = re.compile(r"\{\{([A-Z_]+(?:_ID)?)\}\}")
# Match any {{...}} including malformed ones
_ANY_PLACEHOLDER_RE = re.compile(r"\{\{([^}]*)\}\}")


def validate_output(industry_config: dict, configs: dict,
                    output_dir: Path) -> list[ValidationResult]:
    """Run all validation checks on generated output artifacts.

    Args:
        industry_config: Parsed industry.json content.
        configs: Full config dict with all loaded JSON configs.
        output_dir: Demo output root directory.

    Returns:
        List of ValidationResult findings, sorted by severity.
    """
    results: list[ValidationResult] = []
    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo").replace(" ", "")

    results.extend(_validate_config_completeness(configs))
    results.extend(_validate_structure(output_dir, company, configs))
    results.extend(_validate_reports(output_dir, company, configs))
    results.extend(_validate_semantic_model(output_dir, company, configs))
    results.extend(_validate_tmdl_cross_refs(output_dir, company, configs))
    results.extend(_validate_report_bindings(output_dir, company, configs))
    results.extend(_validate_csv_alignment(output_dir, configs))
    results.extend(_validate_dataflows(output_dir, configs))
    results.extend(_validate_pipeline(output_dir, company))
    results.extend(_validate_placeholders(output_dir))
    results.extend(_validate_notebooks(output_dir))
    results.extend(_validate_udf(output_dir, configs))

    # Sort: ERROR first, then WARNING, then INFO
    severity_order = {"ERROR": 0, "WARNING": 1, "INFO": 2}
    results.sort(key=lambda r: (severity_order.get(r.severity, 9), r.category))
    return results


def validate_and_report(industry_config: dict, configs: dict,
                        output_dir: Path,
                        export: bool = False) -> dict:
    """Run validation and return a structured summary dict.

    Args:
        industry_config: Parsed industry.json content.
        configs: Full config dict with all loaded JSON configs.
        output_dir: Demo output root directory.
        export: If True, write validation-report.json and .html to output_dir.

    Returns:
        Dict with 'errors', 'warnings', 'info' counts and 'results' list.
    """
    results = validate_output(industry_config, configs, output_dir)
    summary = {
        "errors": sum(1 for r in results if r.severity == "ERROR"),
        "warnings": sum(1 for r in results if r.severity == "WARNING"),
        "info": sum(1 for r in results if r.severity == "INFO"),
        "total": len(results),
        "passed": sum(1 for r in results if r.severity == "ERROR") == 0,
        "results": [r._asdict() for r in results],
    }
    if export:
        _export_validation_report(summary, output_dir)
    return summary


# ═══════════════════════════════════════════════════════════════════════
# 1. Structure completeness
# ═══════════════════════════════════════════════════════════════════════

def _validate_structure(output_dir: Path, company: str,
                        configs: dict) -> list[ValidationResult]:
    """Verify expected directories and files exist."""
    results = []
    cat = "structure"

    # Required top-level dirs
    expected_dirs = ["SampleData", "notebooks", "deploy", "Pipeline"]
    for d in expected_dirs:
        path = output_dir / d
        if not path.is_dir():
            results.append(ValidationResult(
                "ERROR", cat, d, f"Required directory missing: {d}/"))

    # Reports — check from config
    reports_config = configs.get("reports", {})
    for report in reports_config.get("reports", []):
        report_name = report.get("name", "")
        if report_name:
            report_dir = output_dir / f"{report_name}.Report"
            if not report_dir.is_dir():
                results.append(ValidationResult(
                    "ERROR", cat, report_name,
                    f"Report directory missing: {report_name}.Report/"))
            pbip_file = output_dir / f"{report_name}.pbip"
            if not pbip_file.is_file():
                results.append(ValidationResult(
                    "ERROR", cat, report_name,
                    f"PBIP file missing: {report_name}.pbip"))

    # Semantic model
    sm_config = configs.get("semantic_model", {})
    sm = sm_config.get("semanticModel", sm_config)
    model_name = sm.get("name", f"{company}Model")
    sm_dir = output_dir / f"{model_name}.SemanticModel"
    if not sm_dir.is_dir():
        results.append(ValidationResult(
            "ERROR", cat, model_name,
            f"Semantic model directory missing: {model_name}.SemanticModel/"))

    # Dataflows
    if configs.get("sample_data"):
        df_dir = output_dir / "Dataflows"
        if not df_dir.is_dir():
            results.append(ValidationResult(
                "ERROR", cat, "Dataflows",
                "Dataflows directory missing"))

    # Optional dirs — HTAP
    if configs.get("htap"):
        if not (output_dir / "Transactional").is_dir():
            results.append(ValidationResult(
                "WARNING", cat, "Transactional",
                "HTAP configured but Transactional/ directory missing"))

    # Optional — Writeback
    if configs.get("writeback"):
        if not (output_dir / "Writeback").is_dir():
            results.append(ValidationResult(
                "WARNING", cat, "Writeback",
                "Writeback configured but Writeback/ directory missing"))
        if not (output_dir / "UserDataFunction").is_dir():
            results.append(ValidationResult(
                "WARNING", cat, "UserDataFunction",
                "Writeback configured but UserDataFunction/ directory missing"))

    # Optional — Data Agent
    if configs.get("data_agent"):
        if not (output_dir / "DataAgent").is_dir():
            results.append(ValidationResult(
                "WARNING", cat, "DataAgent",
                "Data agent configured but DataAgent/ directory missing"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 2. Report validation (PBIR metadata + structure)
# ═══════════════════════════════════════════════════════════════════════

def _validate_reports(output_dir: Path, company: str,
                      configs: dict) -> list[ValidationResult]:
    """Validate PBIR report structure, schemas, and page counts."""
    results = []
    cat = "metadata"
    reports_config = configs.get("reports", {})

    for report in reports_config.get("reports", []):
        report_name = report.get("name", "")
        report_dir = output_dir / f"{report_name}.Report"
        if not report_dir.is_dir():
            continue  # Already caught by structure check

        # definition.pbir
        pbir_path = report_dir / "definition.pbir"
        if pbir_path.is_file():
            results.extend(_check_json_schema(
                pbir_path, "definition.pbir", report_name))
        else:
            results.append(ValidationResult(
                "ERROR", cat, report_name, "Missing definition.pbir"))

        # report.json
        report_json = report_dir / "definition" / "report.json"
        if report_json.is_file():
            results.extend(_check_json_schema(
                report_json, "report.json", report_name))
            data = _safe_json_load(report_json)
            if data and "themeCollection" not in data:
                results.append(ValidationResult(
                    "WARNING", cat, report_name,
                    "report.json missing themeCollection"))
        else:
            results.append(ValidationResult(
                "ERROR", cat, report_name, "Missing definition/report.json"))

        # version.json
        version_json = report_dir / "definition" / "version.json"
        if version_json.is_file():
            results.extend(_check_json_schema(
                version_json, "version.json", report_name))
        else:
            results.append(ValidationResult(
                "WARNING", cat, report_name, "Missing definition/version.json"))

        # Pages — count vs config
        pages_dir = report_dir / "definition" / "pages"
        expected_pages = len(report.get("pages", []))
        if pages_dir.is_dir():
            actual_pages = sum(1 for p in pages_dir.iterdir() if p.is_dir())
            if actual_pages != expected_pages and expected_pages > 0:
                results.append(ValidationResult(
                    "ERROR", "cross-ref", report_name,
                    f"Page count mismatch: config has {expected_pages}, "
                    f"output has {actual_pages} page directories"))

            # Validate each page
            for page_dir in pages_dir.iterdir():
                if not page_dir.is_dir():
                    continue
                page_json = page_dir / "page.json"
                if page_json.is_file():
                    results.extend(_check_json_schema(
                        page_json, "page.json", report_name))
                    data = _safe_json_load(page_json)
                    if data:
                        if not data.get("displayName"):
                            results.append(ValidationResult(
                                "WARNING", cat, report_name,
                                f"Page {page_dir.name} has no displayName"))
                        # Validate dimensions
                        h = data.get("height", 0)
                        w = data.get("width", 0)
                        if h <= 0 or w <= 0:
                            results.append(ValidationResult(
                                "ERROR", cat, report_name,
                                f"Page {page_dir.name} has invalid dimensions "
                                f"({w}x{h})"))
                else:
                    results.append(ValidationResult(
                        "ERROR", cat, report_name,
                        f"Page dir {page_dir.name} missing page.json"))

                # Validate visuals
                visuals_dir = page_dir / "visuals"
                if visuals_dir.is_dir():
                    for vis_dir in visuals_dir.iterdir():
                        if not vis_dir.is_dir():
                            continue
                        vis_json = vis_dir / "visual.json"
                        if vis_json.is_file():
                            results.extend(_check_json_schema(
                                vis_json, "visual.json", report_name))
                        else:
                            results.append(ValidationResult(
                                "ERROR", cat, report_name,
                                f"Visual dir {vis_dir.name} missing visual.json"))
        elif expected_pages > 0:
            results.append(ValidationResult(
                "ERROR", "structure", report_name,
                "Pages directory missing from report"))

        # Theme file
        theme_dir = (report_dir / "definition" / "StaticResources"
                     / "SharedResources" / "BaseThemes")
        if theme_dir.is_dir():
            theme_files = list(theme_dir.glob("*.json"))
            if not theme_files:
                results.append(ValidationResult(
                    "WARNING", cat, report_name,
                    "BaseThemes directory exists but has no theme JSON"))
            for tf in theme_files:
                data = _safe_json_load(tf)
                if data:
                    colors = data.get("dataColors", [])
                    if len(colors) < 4:
                        results.append(ValidationResult(
                            "WARNING", cat, report_name,
                            f"Theme {tf.name} has only {len(colors)} data colors "
                            f"(expected at least 4)"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 3. Semantic model validation (TMDL)
# ═══════════════════════════════════════════════════════════════════════

def _validate_semantic_model(output_dir: Path, company: str,
                             configs: dict) -> list[ValidationResult]:
    """Validate TMDL files: syntax, integrity, cross-references."""
    results = []
    cat = "tmdl"
    sm_config = configs.get("semantic_model", {})
    sm = sm_config.get("semanticModel", sm_config)
    model_name = sm.get("name", f"{company}Model")
    sm_dir = output_dir / f"{model_name}.SemanticModel" / "definition"
    if not sm_dir.is_dir():
        return results  # Already caught by structure check

    # model.tmdl
    model_tmdl = sm_dir / "model.tmdl"
    if model_tmdl.is_file():
        content = model_tmdl.read_text(encoding="utf-8")
        if "model Model" not in content and "model " not in content:
            results.append(ValidationResult(
                "ERROR", cat, model_name,
                "model.tmdl missing 'model' declaration"))
        if "culture" not in content:
            results.append(ValidationResult(
                "WARNING", cat, model_name,
                "model.tmdl missing 'culture' setting"))
    else:
        results.append(ValidationResult(
            "ERROR", cat, model_name, "Missing model.tmdl"))

    # Tables
    tables_dir = sm_dir / "tables"
    expected_tables = sm.get("tables", [])
    table_names_in_files: set[str] = set()

    if tables_dir.is_dir():
        tmdl_files = list(tables_dir.glob("*.tmdl"))
        if len(expected_tables) > 0 and len(tmdl_files) != len(expected_tables):
            results.append(ValidationResult(
                "WARNING", "cross-ref", model_name,
                f"Table count mismatch: config has {len(expected_tables)}, "
                f"output has {len(tmdl_files)} .tmdl files"))

        for tf in tmdl_files:
            content = tf.read_text(encoding="utf-8")
            # Check for table declaration
            table_match = re.search(r"^table\s+'?([^'\n]+)'?\s*$",
                                    content, re.MULTILINE)
            if table_match:
                table_names_in_files.add(table_match.group(1).strip("'"))
            else:
                results.append(ValidationResult(
                    "ERROR", cat, model_name,
                    f"{tf.name} missing 'table' declaration"))

            # Check for lineageTag
            if "lineageTag" not in content:
                results.append(ValidationResult(
                    "WARNING", cat, model_name,
                    f"{tf.name} missing lineageTag"))

            # Check for at least one column
            if "column " not in content and "measure " not in content:
                results.append(ValidationResult(
                    "WARNING", cat, model_name,
                    f"{tf.name} has no columns or measures"))

            # Check for partition (required for Direct Lake)
            if "partition " not in content:
                results.append(ValidationResult(
                    "WARNING", cat, model_name,
                    f"{tf.name} missing partition definition"))
    elif expected_tables:
        results.append(ValidationResult(
            "ERROR", "structure", model_name,
            "Tables directory missing from semantic model"))

    # Relationships
    rels_dir = sm_dir / "relationships"
    expected_rels = sm.get("relationships", [])
    if rels_dir.is_dir():
        rel_files = list(rels_dir.glob("*.tmdl"))
        if len(expected_rels) > 0 and len(rel_files) != len(expected_rels):
            results.append(ValidationResult(
                "WARNING", "cross-ref", model_name,
                f"Relationship count mismatch: config has {len(expected_rels)}, "
                f"output has {len(rel_files)} .tmdl files"))

        for rf in rel_files:
            content = rf.read_text(encoding="utf-8")
            # Both fromColumn and toColumn must reference known tables
            from_match = re.search(
                r"fromColumn:\s+'?([^'\n]+)'?", content)
            to_match = re.search(
                r"toColumn:\s+'?([^'\n]+)'?", content)
            if not from_match or not to_match:
                results.append(ValidationResult(
                    "WARNING", cat, model_name,
                    f"{rf.name} missing fromColumn or toColumn"))

    # definition.pbism
    pbism = output_dir / f"{model_name}.SemanticModel" / "definition.pbism"
    if pbism.is_file():
        data = _safe_json_load(pbism)
        if data and data.get("version") != "4.0":
            results.append(ValidationResult(
                "WARNING", cat, model_name,
                f"definition.pbism version is '{data.get('version')}', "
                f"expected '4.0'"))
    else:
        results.append(ValidationResult(
            "ERROR", cat, model_name, "Missing definition.pbism"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 4. CSV ↔ Semantic Model alignment
# ═══════════════════════════════════════════════════════════════════════

def _validate_csv_alignment(output_dir: Path,
                            configs: dict) -> list[ValidationResult]:
    """Check CSV headers align with sample-data.json definitions."""
    results = []
    cat = "cross-ref"
    sd = configs.get("sample_data", {})
    sd_data = sd.get("sampleData", sd)

    for domain in sd_data.get("domains", []):
        folder = domain.get("folder", domain.get("name", ""))
        for table in domain.get("tables", []):
            tname = table["name"]
            fname = table.get("fileName", f"{tname}.csv")
            csv_path = output_dir / "SampleData" / folder / fname
            if not csv_path.is_file():
                results.append(ValidationResult(
                    "ERROR", cat, f"SampleData/{folder}/{fname}",
                    f"CSV file missing for table {tname}"))
                continue

            # Check header row matches column definitions
            expected_cols = [c["name"] for c in table.get("columns", [])]
            if not expected_cols:
                continue

            try:
                with open(csv_path, encoding="utf-8") as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                if header and set(header) != set(expected_cols):
                    missing = set(expected_cols) - set(header)
                    extra = set(header) - set(expected_cols)
                    parts = []
                    if missing:
                        parts.append(f"missing: {sorted(missing)}")
                    if extra:
                        parts.append(f"extra: {sorted(extra)}")
                    results.append(ValidationResult(
                        "WARNING", cat, f"SampleData/{folder}/{fname}",
                        f"CSV headers don't match config for {tname}: "
                        + "; ".join(parts)))
            except Exception:
                results.append(ValidationResult(
                    "WARNING", cat, f"SampleData/{folder}/{fname}",
                    f"Could not read CSV headers for {tname}"))

            # Check row count
            expected_rows = table.get("rowCount", 0)
            if expected_rows > 0:
                try:
                    with open(csv_path, encoding="utf-8") as f:
                        actual_rows = sum(1 for _ in f) - 1  # minus header
                    if actual_rows < expected_rows:
                        results.append(ValidationResult(
                            "WARNING", cat, f"SampleData/{folder}/{fname}",
                            f"{tname}: expected {expected_rows} rows, "
                            f"got {actual_rows}"))
                except Exception:
                    pass

    return results


# ═══════════════════════════════════════════════════════════════════════
# 5. Dataflow validation
# ═══════════════════════════════════════════════════════════════════════

def _validate_dataflows(output_dir: Path,
                        configs: dict) -> list[ValidationResult]:
    """Validate Dataflow Gen2 JSON + Power Query M files."""
    results = []
    cat = "metadata"
    df_dir = output_dir / "Dataflows"
    if not df_dir.is_dir():
        return results

    json_files = list(df_dir.glob("DF_*.json"))
    pq_files = list(df_dir.glob("DF_*.pq"))

    # Every JSON should have a matching .pq
    json_names = {f.stem for f in json_files}
    pq_names = {f.stem for f in pq_files}
    for name in json_names - pq_names:
        results.append(ValidationResult(
            "ERROR", cat, f"Dataflows/{name}",
            f"Dataflow {name}.json has no matching .pq file"))
    for name in pq_names - json_names:
        results.append(ValidationResult(
            "WARNING", cat, f"Dataflows/{name}",
            f"Dataflow {name}.pq has no matching .json metadata"))

    # Validate JSON structure
    for jf in json_files:
        data = _safe_json_load(jf)
        if data is None:
            results.append(ValidationResult(
                "ERROR", cat, f"Dataflows/{jf.name}",
                "Invalid JSON in dataflow metadata"))
            continue
        df = data.get("dataflow", data)
        if not df.get("name"):
            results.append(ValidationResult(
                "WARNING", cat, f"Dataflows/{jf.name}",
                "Dataflow metadata missing 'name'"))

    # Validate PQ files have section declaration
    for pf in pq_files:
        content = pf.read_text(encoding="utf-8")
        if "section " not in content.lower():
            results.append(ValidationResult(
                "WARNING", cat, f"Dataflows/{pf.name}",
                "Power Query M file missing 'section' declaration"))

        # Balanced let/in check (Sprint 23)
        let_count = len(re.findall(r'\blet\b', content, re.IGNORECASE))
        in_count = len(re.findall(r'\bin\b', content, re.IGNORECASE))
        if let_count != in_count:
            results.append(ValidationResult(
                "ERROR", "syntax", f"Dataflows/{pf.name}",
                f"Unbalanced let/in: {let_count} 'let' vs {in_count} 'in'"))

        # Check section ends with semicolon
        stripped = content.rstrip()
        if stripped and not stripped.endswith(";"):
            results.append(ValidationResult(
                "WARNING", "syntax", f"Dataflows/{pf.name}",
                "Power Query M section does not end with semicolon"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 6. Pipeline validation
# ═══════════════════════════════════════════════════════════════════════

def _validate_pipeline(output_dir: Path,
                       company: str) -> list[ValidationResult]:
    """Validate pipeline content JSON: activities, dependencies."""
    results = []
    cat = "metadata"
    pl_dir = output_dir / "Pipeline"
    content_json = pl_dir / "pipeline-content.json"
    if not content_json.is_file():
        return results

    data = _safe_json_load(content_json)
    if data is None:
        results.append(ValidationResult(
            "ERROR", cat, "Pipeline", "Invalid JSON in pipeline-content.json"))
        return results

    activities = data.get("properties", data).get("activities", [])
    if not activities:
        results.append(ValidationResult(
            "ERROR", cat, "Pipeline", "Pipeline has no activities"))
        return results

    # Validate dependency references
    activity_names = {a.get("name") for a in activities}
    for act in activities:
        name = act.get("name", "unknown")
        for dep in act.get("dependsOn", []):
            dep_name = dep.get("activity")
            if dep_name and dep_name not in activity_names:
                results.append(ValidationResult(
                    "ERROR", cat, f"Pipeline/{name}",
                    f"Activity '{name}' depends on unknown activity "
                    f"'{dep_name}'"))

        # Check required fields
        if not act.get("type"):
            results.append(ValidationResult(
                "WARNING", cat, f"Pipeline/{name}",
                f"Activity '{name}' missing 'type'"))

    # DAG cycle detection (Sprint 23)
    # Build adjacency list and run topological sort
    graph: dict[str, list[str]] = {a.get("name", ""): [] for a in activities}
    for act in activities:
        name = act.get("name", "")
        for dep in act.get("dependsOn", []):
            dep_name = dep.get("activity", "")
            if dep_name in graph:
                graph[dep_name].append(name)

    visited: set[str] = set()
    in_stack: set[str] = set()
    has_cycle = False

    def _dfs(node: str) -> bool:
        nonlocal has_cycle
        if node in in_stack:
            has_cycle = True
            return True
        if node in visited:
            return False
        visited.add(node)
        in_stack.add(node)
        for neighbor in graph.get(node, []):
            if _dfs(neighbor):
                return True
        in_stack.discard(node)
        return False

    for node in graph:
        if node not in visited:
            _dfs(node)

    if has_cycle:
        results.append(ValidationResult(
            "ERROR", cat, "Pipeline",
            "Pipeline DAG contains a dependency cycle"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 7. Placeholder hygiene
# ═══════════════════════════════════════════════════════════════════════

def _validate_placeholders(output_dir: Path) -> list[ValidationResult]:
    """Check all {{PLACEHOLDER}} tokens are well-formed and from known set."""
    results = []
    cat = "placeholder"

    # Scan text files for placeholders
    extensions = {".json", ".tmdl", ".ps1", ".psm1", ".py", ".pq", ".kql",
                  ".sql", ".pbip", ".pbir", ".pbism"}

    for path in output_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in extensions:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, PermissionError):
            continue

        rel_path = path.relative_to(output_dir)

        for match in _ANY_PLACEHOLDER_RE.finditer(content):
            token = match.group(1)
            # Skip known non-placeholder patterns (e.g. ODBC connection strings)
            if any(skip in token for skip in ("ODBC", "Driver", "SQL Server")):
                continue
            if not token:
                results.append(ValidationResult(
                    "ERROR", cat, str(rel_path),
                    "Empty placeholder {{}} found"))
            elif not re.match(r"^[A-Z][A-Z0-9_]*$", token):
                results.append(ValidationResult(
                    "WARNING", cat, str(rel_path),
                    f"Non-standard placeholder token: {{{{{token}}}}}"))
            # We don't flag unknown-but-well-formed placeholders as errors
            # since industries may define custom ones

    return results


# ═══════════════════════════════════════════════════════════════════════
# 8. Notebook validation
# ═══════════════════════════════════════════════════════════════════════

def _validate_notebooks(output_dir: Path) -> list[ValidationResult]:
    """Basic notebook validation: file naming, non-empty content."""
    results = []
    cat = "metadata"
    nb_dir = output_dir / "notebooks"
    if not nb_dir.is_dir():
        return results

    py_files = sorted(nb_dir.glob("*.py"))
    if not py_files:
        results.append(ValidationResult(
            "WARNING", cat, "notebooks",
            "Notebooks directory is empty"))
        return results

    for nb in py_files:
        content = nb.read_text(encoding="utf-8")
        if len(content.strip()) < 50:
            results.append(ValidationResult(
                "WARNING", cat, f"notebooks/{nb.name}",
                f"Notebook {nb.name} appears nearly empty"))

        # Check naming convention: NN_Description.py
        if not re.match(r"^\d{2}_\w+\.py$", nb.name):
            results.append(ValidationResult(
                "WARNING", cat, f"notebooks/{nb.name}",
                f"Notebook {nb.name} doesn't follow NB naming convention "
                f"(expected: NN_Description.py)"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 9. User Data Function validation
# ═══════════════════════════════════════════════════════════════════════

def _validate_udf(output_dir: Path,
                  configs: dict) -> list[ValidationResult]:
    """Validate UDF definition.json, function_app.py, and functions.json."""
    results = []
    cat = "metadata"
    udf_dir = output_dir / "UserDataFunction"
    if not udf_dir.is_dir():
        return results  # Optional — only when writeback configured

    # definition.json
    def_json = udf_dir / "definition.json"
    if def_json.is_file():
        data = _safe_json_load(def_json)
        if data is None:
            results.append(ValidationResult(
                "ERROR", cat, "UserDataFunction",
                "Invalid JSON in UDF definition.json"))
        else:
            if not data.get("functions"):
                results.append(ValidationResult(
                    "WARNING", cat, "UserDataFunction",
                    "UDF definition.json has no functions listed"))
    else:
        results.append(ValidationResult(
            "ERROR", cat, "UserDataFunction",
            "Missing definition.json"))

    # function_app.py
    app_py = udf_dir / "function_app.py"
    if app_py.is_file():
        content = app_py.read_text(encoding="utf-8")
        # Cross-ref: writeback tables should have upsert + read functions
        wb_config = configs.get("writeback", {})
        wb = wb_config.get("writebackConfig", wb_config)
        for table in wb.get("tables", []):
            tname = table.get("name", "")
            if tname:
                snake = re.sub(r"(?<!^)(?=[A-Z])", "_", tname).lower()
                if f"def upsert_{snake}" not in content:
                    results.append(ValidationResult(
                        "WARNING", "cross-ref", "UserDataFunction",
                        f"Missing upsert function for writeback table {tname}"))
                if f"def read_{snake}" not in content:
                    results.append(ValidationResult(
                        "WARNING", "cross-ref", "UserDataFunction",
                        f"Missing read function for writeback table {tname}"))
    else:
        results.append(ValidationResult(
            "ERROR", cat, "UserDataFunction",
            "Missing function_app.py"))

    # functions.json
    funcs_json = udf_dir / "resources" / "functions.json"
    if funcs_json.is_file():
        data = _safe_json_load(funcs_json)
        if data is None:
            results.append(ValidationResult(
                "ERROR", cat, "UserDataFunction",
                "Invalid JSON in functions.json"))
    elif (udf_dir / "resources").is_dir():
        results.append(ValidationResult(
            "WARNING", cat, "UserDataFunction",
            "Missing resources/functions.json"))

    # Cross-ref: definition.json function list ↔ function_app.py defs (Sprint 23)
    if def_json.is_file() and app_py.is_file():
        def_data = _safe_json_load(def_json)
        app_content = app_py.read_text(encoding="utf-8")
        # Extract function names from definition.json
        declared_funcs: set[str] = set()
        if def_data:
            for fn in def_data.get("functions", []):
                fname = fn.get("name", "")
                if fname:
                    declared_funcs.add(fname)
        # Extract def names from function_app.py
        implemented_funcs: set[str] = set()
        for m in re.finditer(r"^def\s+(\w+)\s*\(", app_content, re.MULTILINE):
            implemented_funcs.add(m.group(1))
        # Functions declared but not implemented
        for fn in declared_funcs - implemented_funcs:
            results.append(ValidationResult(
                "ERROR", "cross-ref", "UserDataFunction",
                f"Function '{fn}' declared in definition.json "
                f"but not implemented in function_app.py"))
        # Functions implemented but not declared (info only)
        for fn in implemented_funcs - declared_funcs:
            if not fn.startswith("_"):  # Skip private helpers
                results.append(ValidationResult(
                    "INFO", "cross-ref", "UserDataFunction",
                    f"Function '{fn}' in function_app.py not declared "
                    f"in definition.json"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 10. Config Completeness (Sprint 24)
# ═══════════════════════════════════════════════════════════════════════

def _validate_config_completeness(configs: dict) -> list[ValidationResult]:
    """Verify config files are internally complete: tables have columns, measures have DAX, etc."""
    results = []
    cat = "completeness"

    # sample-data.json: every table must have columns
    sd = configs.get("sample_data", {})
    sd_data = sd.get("sampleData", sd)
    for domain in sd_data.get("domains", []):
        dname = domain.get("name", "?")
        for table in domain.get("tables", []):
            tname = table.get("name", "?")
            cols = table.get("columns", [])
            if not cols:
                results.append(ValidationResult(
                    "ERROR", cat, f"sample-data/{dname}/{tname}",
                    f"Table '{tname}' has no columns defined"))
            else:
                for col in cols:
                    if not col.get("name"):
                        results.append(ValidationResult(
                            "ERROR", cat, f"sample-data/{dname}/{tname}",
                            "Column with empty name"))
                    if not col.get("type"):
                        results.append(ValidationResult(
                            "WARNING", cat, f"sample-data/{dname}/{tname}",
                            f"Column '{col.get('name', '?')}' has no type"))
            if not table.get("rowCount") or table.get("rowCount", 0) <= 0:
                results.append(ValidationResult(
                    "WARNING", cat, f"sample-data/{dname}/{tname}",
                    f"Table '{tname}' has no rowCount (or ≤ 0)"))

    # semantic-model.json: tables have columns, measures have DAX
    sm_config = configs.get("semantic_model", {})
    sm = sm_config.get("semanticModel", sm_config)
    for table in sm.get("tables", []):
        tname = table.get("name", "?")
        cols = table.get("columns", [])
        if not cols:
            results.append(ValidationResult(
                "ERROR", cat, f"semantic-model/{tname}",
                f"Table '{tname}' has no columns defined"))
        for col in cols:
            if not col.get("dataType"):
                results.append(ValidationResult(
                    "WARNING", cat, f"semantic-model/{tname}",
                    f"Column '{col.get('name', '?')}' has no dataType"))

    for measure in sm.get("measures", []):
        mname = measure.get("name", "?")
        if not measure.get("expression"):
            results.append(ValidationResult(
                "ERROR", cat, f"semantic-model/measures",
                f"Measure '{mname}' has no DAX expression"))
        if not measure.get("table"):
            results.append(ValidationResult(
                "WARNING", cat, f"semantic-model/measures",
                f"Measure '{mname}' has no parent table"))

    # relationships: all reference valid table names from config
    sm_table_names = {t.get("name") for t in sm.get("tables", [])}
    for rel in sm.get("relationships", []):
        ft = rel.get("fromTable", "")
        tt = rel.get("toTable", "")
        if ft and ft not in sm_table_names:
            results.append(ValidationResult(
                "WARNING", cat, "semantic-model/relationships",
                f"Relationship fromTable '{ft}' not in configured tables"))
        if tt and tt not in sm_table_names:
            results.append(ValidationResult(
                "WARNING", cat, "semantic-model/relationships",
                f"Relationship toTable '{tt}' not in configured tables"))

    # reports.json: pages should have visuals
    reports_config = configs.get("reports", {})
    for report in reports_config.get("reports", []):
        rname = report.get("name", "?")
        pages = report.get("pages", [])
        if not pages:
            results.append(ValidationResult(
                "WARNING", cat, f"reports/{rname}",
                f"Report '{rname}' has no pages"))
        for page in pages:
            pname = page.get("name", "?")
            if not page.get("visuals"):
                results.append(ValidationResult(
                    "WARNING", cat, f"reports/{rname}",
                    f"Page '{pname}' has no visuals"))

    # industry.json: domains should have ≥2 entries (schema enforces this)
    industry_cfg = configs.get("industry", {})
    industry = industry_cfg.get("industry", {})
    if len(industry.get("domains", [])) < 2:
        results.append(ValidationResult(
            "WARNING", cat, "industry.json",
            "Industry should have at least 2 domains"))

    # Check TODO markers in description fields
    for cfg_key in ("industry", "sample_data", "semantic_model"):
        cfg = configs.get(cfg_key, {})
        _check_todo_markers(cfg, cfg_key, results)

    return results


def _check_todo_markers(obj, path: str, results: list[ValidationResult]):
    """Recursively check for 'TODO' strings in config values."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            _check_todo_markers(v, f"{path}/{k}", results)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _check_todo_markers(v, f"{path}[{i}]", results)
    elif isinstance(obj, str) and "TODO" in obj:
        results.append(ValidationResult(
            "WARNING", "completeness", path,
            f"Contains TODO marker: '{obj[:80]}...' " if len(obj) > 80
            else f"Contains TODO marker: '{obj}'"))


# ═══════════════════════════════════════════════════════════════════════
# 11. TMDL Relationship Cross-Reference (Sprint 23)
# ═══════════════════════════════════════════════════════════════════════

def _validate_tmdl_cross_refs(output_dir: Path, company: str,
                              configs: dict) -> list[ValidationResult]:
    """Verify relationship fromColumn/toColumn reference existing table columns."""
    results = []
    cat = "cross-ref"
    sm_config = configs.get("semantic_model", {})
    sm = sm_config.get("semanticModel", sm_config)
    model_name = sm.get("name", f"{company}Model")
    sm_dir = output_dir / f"{model_name}.SemanticModel" / "definition"
    if not sm_dir.is_dir():
        return results

    # Build table → columns map from TMDL files
    table_columns: dict[str, set[str]] = {}
    tables_dir = sm_dir / "tables"
    if tables_dir.is_dir():
        for tf in tables_dir.glob("*.tmdl"):
            content = tf.read_text(encoding="utf-8")
            table_match = re.search(
                r"^table\s+'?([^'\n]+)'?\s*$", content, re.MULTILINE)
            if not table_match:
                continue
            tname = table_match.group(1).strip("'")
            cols: set[str] = set()
            for col_match in re.finditer(
                    r"^\s+column\s+'?([^'\n]+)'?\s*$", content, re.MULTILINE):
                cols.add(col_match.group(1).strip("'"))
            table_columns[tname] = cols

    # Parse relationships and cross-ref
    rels_dir = sm_dir / "relationships"
    if not rels_dir.is_dir():
        return results

    for rf in rels_dir.glob("*.tmdl"):
        content = rf.read_text(encoding="utf-8")
        # Pattern: fromColumn: TableName.ColumnName
        for direction in ("fromColumn", "toColumn"):
            match = re.search(
                rf"{direction}:\s+(\S+)\.(\S+)", content)
            if not match:
                continue
            table_ref = match.group(1)
            col_ref = match.group(2)
            if table_ref not in table_columns:
                results.append(ValidationResult(
                    "ERROR", cat, rf.name,
                    f"{direction} references unknown table '{table_ref}'"))
            elif col_ref not in table_columns[table_ref]:
                results.append(ValidationResult(
                    "ERROR", cat, rf.name,
                    f"{direction} references unknown column "
                    f"'{table_ref}.{col_ref}'"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 11. Report Visual → Semantic Model Binding (Sprint 23)
# ═══════════════════════════════════════════════════════════════════════

def _validate_report_bindings(output_dir: Path, company: str,
                              configs: dict) -> list[ValidationResult]:
    """Check report config measure/column refs exist in the semantic model."""
    results = []
    cat = "cross-ref"
    sm_config = configs.get("semantic_model", {})
    sm = sm_config.get("semanticModel", sm_config)
    model_name = sm.get("name", f"{company}Model")
    sm_dir = output_dir / f"{model_name}.SemanticModel" / "definition"
    if not sm_dir.is_dir():
        return results

    # Collect all measure names and column names from TMDL files
    all_measures: set[str] = set()
    all_columns: dict[str, set[str]] = {}  # table → {columns}
    tables_dir = sm_dir / "tables"
    if tables_dir.is_dir():
        for tf in tables_dir.glob("*.tmdl"):
            content = tf.read_text(encoding="utf-8")
            table_match = re.search(
                r"^table\s+'?([^'\n]+)'?\s*$", content, re.MULTILINE)
            tname = table_match.group(1).strip("'") if table_match else tf.stem
            cols: set[str] = set()
            for col_m in re.finditer(
                    r"^\s+column\s+'?([^'\n]+)'?\s*$", content, re.MULTILINE):
                cols.add(col_m.group(1).strip("'"))
            all_columns[tname] = cols
            for meas_m in re.finditer(
                    r"^\s+measure\s+'([^']+)'", content, re.MULTILINE):
                all_measures.add(meas_m.group(1))

    if not all_measures and not all_columns:
        return results  # No model to validate against

    # Check reports.json visual bindings
    reports_config = configs.get("reports", {})
    for report in reports_config.get("reports", []):
        rname = report.get("name", "")
        for page in report.get("pages", []):
            pname = page.get("name", "")
            for visual in page.get("visuals", []):
                # Check measure references
                measure_ref = visual.get("measure")
                if measure_ref and measure_ref not in all_measures:
                    results.append(ValidationResult(
                        "WARNING", cat, rname,
                        f"Page '{pname}': measure '{measure_ref}' "
                        f"not found in semantic model"))

                # Check values list (measure references)
                for val in visual.get("values", []):
                    if val not in all_measures:
                        results.append(ValidationResult(
                            "WARNING", cat, rname,
                            f"Page '{pname}': value '{val}' "
                            f"not found in semantic model"))

                # Check axis/legend/rows/columns (Table[Column] refs)
                for field_key in ("axis", "legend", "location", "size",
                                  "category"):
                    ref = visual.get(field_key, "")
                    if not ref or "[" not in ref:
                        continue
                    m = re.match(r"(\w+)\[(\w+)\]", ref)
                    if m:
                        tbl, col = m.group(1), m.group(2)
                        if tbl not in all_columns:
                            results.append(ValidationResult(
                                "WARNING", cat, rname,
                                f"Page '{pname}': {field_key} references "
                                f"unknown table '{tbl}'"))
                        elif col not in all_columns[tbl]:
                            results.append(ValidationResult(
                                "WARNING", cat, rname,
                                f"Page '{pname}': {field_key} references "
                                f"unknown column '{tbl}[{col}]'"))

                for ref_list_key in ("rows", "columns"):
                    for ref in visual.get(ref_list_key, []):
                        if "[" not in ref:
                            continue
                        m = re.match(r"(\w+)\[(\w+)\]", ref)
                        if m:
                            tbl, col = m.group(1), m.group(2)
                            if tbl not in all_columns:
                                results.append(ValidationResult(
                                    "WARNING", cat, rname,
                                    f"Page '{pname}': {ref_list_key} "
                                    f"references unknown table '{tbl}'"))
                            elif col not in all_columns[tbl]:
                                results.append(ValidationResult(
                                    "WARNING", cat, rname,
                                    f"Page '{pname}': {ref_list_key} "
                                    f"references unknown column "
                                    f"'{tbl}[{col}]'"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _safe_json_load(path: Path) -> dict | None:
    """Load JSON file, return None on error."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, PermissionError):
        return None


def _check_json_schema(path: Path, expected_name: str,
                       artifact: str) -> list[ValidationResult]:
    """Verify a JSON file has the correct $schema URL."""
    results = []
    data = _safe_json_load(path)
    if data is None:
        results.append(ValidationResult(
            "ERROR", "metadata", artifact,
            f"Invalid JSON: {path.name}"))
        return results

    expected_schema = _EXPECTED_SCHEMAS.get(expected_name)
    if expected_schema:
        actual_schema = data.get("$schema", "")
        if actual_schema != expected_schema:
            results.append(ValidationResult(
                "ERROR", "metadata", artifact,
                f"{path.name}: $schema is '{actual_schema}', "
                f"expected '{expected_schema}'"))

    return results


def _export_validation_report(summary: dict, output_dir: Path) -> None:
    """Write validation-report.json and validation-report.html to output_dir."""
    # JSON export
    json_path = output_dir / "validation-report.json"
    json_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # HTML export
    errors = summary.get("errors", 0)
    warnings = summary.get("warnings", 0)
    info = summary.get("info", 0)
    passed = summary.get("passed", False)
    status_color = "#27ae60" if passed else "#e74c3c"
    status_text = "PASS" if passed else "FAIL"

    rows_html = []
    for r in summary.get("results", []):
        sev = r.get("severity", "INFO")
        sev_class = {"ERROR": "#e74c3c", "WARNING": "#f39c12", "INFO": "#3498db"}.get(sev, "#999")
        rows_html.append(
            f'<tr><td style="color:{sev_class};font-weight:bold">{sev}</td>'
            f'<td>{_html_escape(r.get("category", ""))}</td>'
            f'<td>{_html_escape(r.get("artifact", ""))}</td>'
            f'<td>{_html_escape(r.get("message", ""))}</td></tr>'
        )
    table_rows = "\n".join(rows_html) if rows_html else (
        '<tr><td colspan="4" style="text-align:center;color:#999">No findings</td></tr>'
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Validation Report</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; margin: 2rem; background: #fafafa; }}
  h1 {{ color: #333; }}
  .badge {{ display: inline-block; padding: 4px 12px; border-radius: 4px;
            color: white; font-weight: bold; font-size: 1.1rem; }}
  .summary {{ display: flex; gap: 1.5rem; margin: 1rem 0; }}
  .stat {{ background: white; padding: 1rem 1.5rem; border-radius: 8px;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
  .stat .num {{ font-size: 2rem; font-weight: bold; }}
  .stat .label {{ color: #666; font-size: 0.85rem; }}
  table {{ border-collapse: collapse; width: 100%; background: white;
           box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 8px;
           overflow: hidden; margin-top: 1rem; }}
  th {{ background: #2c3e50; color: white; padding: 10px 12px; text-align: left; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 0.9rem; }}
  tr:hover {{ background: #f5f5f5; }}
</style>
</head>
<body>
<h1>Validation Report
  <span class="badge" style="background:{status_color}">{status_text}</span>
</h1>
<div class="summary">
  <div class="stat"><div class="num" style="color:#e74c3c">{errors}</div><div class="label">Errors</div></div>
  <div class="stat"><div class="num" style="color:#f39c12">{warnings}</div><div class="label">Warnings</div></div>
  <div class="stat"><div class="num" style="color:#3498db">{info}</div><div class="label">Info</div></div>
  <div class="stat"><div class="num">{summary.get("total", 0)}</div><div class="label">Total</div></div>
</div>
<table>
<thead><tr><th>Severity</th><th>Category</th><th>Artifact</th><th>Message</th></tr></thead>
<tbody>
{table_rows}
</tbody>
</table>
</body>
</html>"""

    html_path = output_dir / "validation-report.html"
    html_path.write_text(html, encoding="utf-8")


def _html_escape(text: str) -> str:
    """Minimal HTML escaping for report output."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))
