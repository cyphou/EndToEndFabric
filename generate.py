#!/usr/bin/env python3
"""Fabric End-to-End Industry Demo Generator.

Main entry point  --  generates a complete Fabric demo project from
industry config files.

Usage:
    python generate.py -i contoso-energy
    python generate.py -i horizon-books -o ./my-output
    python generate.py --list
    python generate.py -i contoso-energy --skip-htap --skip-forecast
"""

import argparse
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.config_loader import (
    list_industries,
    load_all_configs,
    get_output_dir,
    IndustryNotFoundError,
    ConfigValidationError,
)
from core.csv_generator import generate_all_csvs
from core.notebook_generator import generate_notebooks
from core.dataflow_generator import generate_dataflows
from core.tmdl_generator import generate_semantic_model, generate_writeback_model
from core.report_generator import generate_reports
from core.pipeline_generator import generate_pipeline
from core.deploy_generator import generate_deploy_scripts
from core.agent_generator import generate_data_agent
from core.udf_generator import generate_udf
from core.comparison_generator import generate_comparison
from core.validator import validate_and_report
from core.workspace_generator import generate_workspace_artifacts
from core.copilot_generator import generate_copilot_instructions
from core.activator_generator import generate_data_activator


def _scaffold_new_industry(industry_id: str) -> int:
    """Create a new industry directory with 10 JSON skeleton configs.

    Returns exit code (0 = success, 1 = error).
    """
    import re as _re

    # Validate ID format
    if not _re.match(r"^[a-z][a-z0-9-]+$", industry_id):
        print(f"Error: Industry ID '{industry_id}' must be lowercase letters, "
              "digits, and hyphens (e.g. 'my-retail').")
        return 1

    target_dir = PROJECT_ROOT / "industries" / industry_id
    if target_dir.exists():
        print(f"Error: Industry directory already exists: {target_dir}")
        return 1

    # Derive names from ID
    pascal = "".join(w.capitalize() for w in industry_id.split("-"))
    display = " ".join(w.capitalize() for w in industry_id.split("-"))

    import json as _json

    templates = {
        "industry.json": {
            "schemaVersion": "1.0",
            "industry": {
                "id": industry_id,
                "name": pascal,
                "displayName": display,
                "description": f"TODO: Describe {display} industry scenario.",
                "domains": ["DomainA", "DomainB"],
                "dataYears": ["FY2025", "FY2026", "FY2027"],
                "dateRange": {"start": "2024-01-01", "end": "2027-12-31"},
                "fiscalYearStartMonth": 7,
                "theme": {
                    "primary": "#1565C0",
                    "secondary": "#E65100",
                    "accent1": "#42A5F5",
                    "accent2": "#FF8F00",
                    "background": "#F5F5F5",
                    "foreground": "#FFFFFF",
                },
            },
            "fabricArtifacts": {
                "workspacePrefix": pascal,
                "cloud": {
                    "fabricApiBase": "https://api.fabric.microsoft.com/v1",
                    "oneLakeDfsBase": "https://onelake.dfs.fabric.microsoft.com",
                },
                "lakehouses": {"bronze": "BronzeLH", "silver": "SilverLH", "gold": "GoldLH"},
                "schemas": {
                    "silver": ["domaina", "domainb", "web"],
                    "gold": ["dim", "fact", "analytics", "planning"],
                },
                "notebooks": 6,
                "dataflows": 2,
                "reports": 3,
                "pipelines": 1,
            },
        },
        "sample-data.json": {
            "sampleData": {
                "description": f"Sample data for {display}",
                "domains": [
                    {
                        "name": "DomainA",
                        "folder": "DomainA",
                        "tables": [
                            {
                                "name": "DimExample",
                                "fileName": "DimExample.csv",
                                "rowCount": 20,
                                "columns": [
                                    {"name": "ExampleID", "type": "int", "primaryKey": True,
                                     "generator": {"method": "sequence", "params": {"start": 1}}},
                                    {"name": "Name", "type": "string",
                                     "generator": {"method": "faker", "params": {"provider": "company"}}},
                                ],
                            },
                            {
                                "name": "FactTransactions",
                                "fileName": "FactTransactions.csv",
                                "rowCount": 500,
                                "columns": [
                                    {"name": "TransactionID", "type": "int", "primaryKey": True,
                                     "generator": {"method": "sequence", "params": {"start": 1}}},
                                    {"name": "ExampleID", "type": "int",
                                     "generator": {"method": "random_int", "params": {"min": 1, "max": 20}}},
                                    {"name": "TransactionDate", "type": "date",
                                     "generator": {"method": "random_date", "params": {"start": "2024-01-01", "end": "2026-12-31"}}},
                                    {"name": "Amount", "type": "float",
                                     "generator": {"method": "random_float", "params": {"min": 10.0, "max": 5000.0, "decimals": 2}}},
                                ],
                            },
                        ],
                    },
                ],
            },
        },
        "semantic-model.json": {
            "semanticModel": {
                "name": f"{pascal}Model",
                "mode": "DirectLake",
                "defaultSchema": "gold",
                "tables": [
                    {
                        "name": "DimExample",
                        "schema": "dim",
                        "columns": [
                            {"name": "ExampleID", "dataType": "Int64", "isKey": True, "summarizeBy": "None"},
                            {"name": "Name", "dataType": "String", "summarizeBy": "None"},
                        ],
                    },
                    {
                        "name": "FactTransactions",
                        "schema": "fact",
                        "columns": [
                            {"name": "TransactionID", "dataType": "Int64", "isKey": True, "summarizeBy": "None"},
                            {"name": "ExampleID", "dataType": "Int64", "summarizeBy": "None"},
                            {"name": "TransactionDate", "dataType": "DateTime", "summarizeBy": "None"},
                            {"name": "Amount", "dataType": "Double", "summarizeBy": "Sum"},
                        ],
                    },
                ],
                "relationships": [
                    {
                        "fromTable": "FactTransactions", "fromColumn": "ExampleID",
                        "toTable": "DimExample", "toColumn": "ExampleID",
                        "cardinality": "ManyToOne",
                    },
                ],
                "measures": [
                    {"name": "Total Amount", "table": "FactTransactions",
                     "expression": "SUM(FactTransactions[Amount])", "formatString": "#,##0.00"},
                    {"name": "Transaction Count", "table": "FactTransactions",
                     "expression": "COUNTROWS(FactTransactions)", "formatString": "#,##0"},
                ],
            },
        },
        "reports.json": {
            "reports": [
                {
                    "name": f"{pascal}-Analytics",
                    "theme": {"primary": "#1565C0", "secondary": "#E65100", "background": "#FAFAFA"},
                    "pages": [
                        {
                            "name": "Overview",
                            "visuals": [
                                {"type": "card", "measure": "Total Amount"},
                                {"type": "card", "measure": "Transaction Count"},
                                {"type": "lineChart", "axis": "DimDate[MonthName]", "values": ["Total Amount"]},
                            ],
                        },
                    ],
                },
            ],
        },
        "forecast-config.json": {
            "forecastModels": [
                {
                    "name": f"{pascal}AmountForecast",
                    "description": f"TODO: {display} amount forecast",
                    "table": "FactTransactions",
                    "dateColumn": "TransactionDate",
                    "valueColumn": "Amount",
                    "groupBy": None,
                    "algorithm": "HoltWinters",
                    "params": {"seasonal": "additive", "seasonalPeriods": 12},
                    "horizonMonths": 12,
                },
            ],
        },
        "htap-config.json": {
            "eventhouse": {
                "name": f"{pascal}Eventhouse",
                "kqlDatabase": f"{pascal}KQL",
                "retentionDays": 30,
            },
            "eventStreams": [
                {
                    "name": f"{pascal}Events",
                    "description": f"TODO: Real-time {display} events",
                    "eventsPerSecond": 10,
                    "schema": [
                        {"name": "EventID", "type": "string"},
                        {"name": "Timestamp", "type": "datetime"},
                        {"name": "Value", "type": "real"},
                    ],
                    "kqlTable": f"{pascal}Events",
                    "kqlAggregations": [],
                },
            ],
        },
        "planning-config.json": {
            "planningModels": [
                {
                    "name": f"{pascal}Budget",
                    "description": f"TODO: {display} budget planning",
                    "sourceTable": "FactTransactions",
                    "valueColumn": "Amount",
                    "planningSchema": "planning",
                    "columns": [
                        {"name": "PlanID", "dataType": "Int64"},
                        {"name": "PlanDate", "dataType": "DateTime"},
                        {"name": "PlannedAmount", "dataType": "Double"},
                    ],
                },
            ],
        },
        "web-enrichment.json": {
            "webEnrichment": {
                "enabled": True,
                "targetLakehouse": "SilverLH",
                "schema": "web",
                "sources": [
                    {
                        "name": f"{pascal}ExternalData",
                        "description": f"TODO: External data source for {display}",
                        "endpoint": "https://api.example.com/data",
                        "method": "GET",
                        "schedule": "daily",
                        "tableName": "ExternalData",
                        "columns": [
                            {"name": "RecordDate", "type": "date"},
                            {"name": "Value", "type": "double"},
                        ],
                    },
                ],
            },
        },
        "writeback-config.json": {
            "writebackConfig": {
                "enabled": True,
                "targetLakehouse": "GoldLH",
                "schema": "planning",
                "tables": [
                    {
                        "name": f"{pascal}Adjustments",
                        "description": f"TODO: Manual adjustments for {display}",
                        "columns": [
                            {"name": "AdjustmentID", "dataType": "Int64", "isKey": True},
                            {"name": "AdjustmentDate", "dataType": "DateTime"},
                            {"name": "Amount", "dataType": "Double"},
                            {"name": "Notes", "dataType": "String"},
                        ],
                        "partitionBy": [],
                    },
                ],
            },
        },
        "data-agent.json": {
            "dataAgent": {
                "name": f"{pascal}Agent",
                "displayName": f"{display} Data Agent",
                "description": f"AI assistant for {display} data exploration and insights.",
                "semanticModel": f"{pascal}Model",
                "systemPrompt": (
                    f"You are an AI data analyst for {display}. "
                    "Help users explore their data, answer business questions, "
                    "and provide actionable insights."
                ),
                "exampleQuestions": [
                    "What is the total amount this year?",
                    "Show me the trend by month.",
                    "Which categories have the highest transactions?",
                ],
                "capabilities": {
                    "requiresF64": True,
                    "supportsFollowUp": True,
                    "supportsVisualization": True,
                },
            },
        },
    }

    target_dir.mkdir(parents=True, exist_ok=True)

    for filename, content in templates.items():
        path = target_dir / filename
        path.write_text(
            _json.dumps(content, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    print(f"\nScaffolded new industry: {industry_id}")
    print(f"  Directory: {target_dir}")
    print(f"  Files created: {len(templates)}")
    print(f"\nNext steps:")
    print(f"  1. Edit industries/{industry_id}/industry.json  --  set description, domains, theme")
    print(f"  2. Edit sample-data.json  --  define tables and columns per domain")
    print(f"  3. Edit semantic-model.json  --  define TMDL tables, relationships, DAX measures")
    print(f"  4. Edit reports.json  --  define report pages and visuals")
    print(f"  5. Run: python generate.py -i {industry_id}")
    print(f"  6. Run: python generate.py -i {industry_id} --export-validation")
    return 0


def _run_wizard(args, parser):
    """Interactive wizard for industry selection and options.

    Mutates and returns args with user selections, or None to abort.
    """
    industries = list_industries()
    if not industries:
        print("No industries found.")
        return None

    print("\n  Fabric Demo Generator  --  Interactive Wizard\n")
    print("  Available industries:")
    for i, ind in enumerate(industries, 1):
        print(f"    {i}. {ind}")
    print(f"    {len(industries) + 1}. all")
    print()

    choice = input(f"  Select industry [1-{len(industries) + 1}]: ").strip()
    try:
        idx = int(choice)
    except ValueError:
        print("  Invalid selection.")
        return None

    if idx == len(industries) + 1:
        # Generate all industries sequentially (handled in main())
        args.industry = "__all__"
        args.output = None
    elif 1 <= idx <= len(industries):
        args.industry = industries[idx - 1]
    else:
        print("  Invalid selection.")
        return None

    # Skip flags
    for flag, label in [
        ("skip_htap", "HTAP"),
        ("skip_forecast", "Forecasting"),
        ("skip_writeback", "Writeback"),
        ("skip_deploy", "Deploy scripts"),
    ]:
        skip = input(f"  Skip {label}? [y/N]: ").strip().lower()
        setattr(args, flag, skip in ("y", "yes"))

    # Seed
    seed_input = input(f"  Random seed [{args.seed}]: ").strip()
    if seed_input:
        try:
            args.seed = int(seed_input)
        except ValueError:
            pass

    # Output directory
    out_input = input("  Output directory [default]: ").strip()
    if out_input:
        args.output = out_input

    return args


def main():
    parser = argparse.ArgumentParser(
        description="Generate a Microsoft Fabric end-to-end demo for a specific industry.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate.py --list                       # List available industries
  python generate.py -i contoso-energy            # Generate Contoso Energy demo
  python generate.py -i horizon-books -o ./out    # Custom output directory
  python generate.py -i contoso-energy --skip-htap
  python generate.py --compare                    # Cross-industry comparison report
  python generate.py --new-industry my-retail     # Scaffold a new industry template
        """,
    )

    parser.add_argument(
        "-i", "--industry",
        help="Industry ID to generate (e.g. 'contoso-energy')",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output directory (default: ./output/<industry-id>)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available industry configs and exit",
    )
    parser.add_argument(
        "--skip-htap", action="store_true",
        help="Skip HTAP (Eventhouse/KQL) generation",
    )
    parser.add_argument(
        "--skip-forecast", action="store_true",
        help="Skip Forecasting & Planning generation",
    )
    parser.add_argument(
        "--skip-writeback", action="store_true",
        help="Skip Writeback generation",
    )
    parser.add_argument(
        "--skip-deploy", action="store_true",
        help="Skip deployment script generation",
    )
    parser.add_argument(
        "--skip-validate", action="store_true",
        help="Skip post-generation output validation",
    )
    parser.add_argument(
        "--export-validation", action="store_true",
        help="Export validation report as JSON + HTML to the output directory",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducible data generation (default: 42)",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="Generate a cross-industry comparison report and exit",
    )
    parser.add_argument(
        "--new-industry", metavar="ID",
        help="Scaffold a new industry template with 10 JSON skeletons (e.g. 'my-retail')",
    )
    parser.add_argument(
        "--wizard", action="store_true",
        help="Interactive wizard to select industry and options",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would be generated without writing any files",
    )
    parser.add_argument(
        "--diff", action="store_true",
        help="Show files that would change compared to existing output",
    )

    args = parser.parse_args()

    # List mode
    if args.list:
        industries = list_industries()
        if not industries:
            print("No industries found. Add configs to industries/<id>/industry.json")
            return 0
        print("Available industries:")
        for ind in industries:
            print(f"  - {ind}")
        return 0

    # Compare mode
    if args.compare:
        out = Path(args.output) if args.output else (PROJECT_ROOT / "output")
        print("Generating cross-industry comparison report...", end=" ", flush=True)
        report_path = generate_comparison(out)
        print(f"OK\n  {report_path}")
        return 0

    # New industry scaffold
    if args.new_industry:
        return _scaffold_new_industry(args.new_industry)

    # Wizard mode
    if args.wizard:
        args = _run_wizard(args, parser)
        if args is None:
            return 0

    # Require --industry for generation
    if not args.industry:
        parser.print_help()
        return 1

    # Handle 'all' from wizard
    if args.industry == "__all__":
        for ind in list_industries():
            args.industry = ind
            args.output = None
            print(f"\n{'='*60}")
            print(f"  Generating {ind}...")
            print(f"{'='*60}")
            _generate_single(args, ind)
        return 0

    # Dry-run mode
    if args.dry_run:
        return _dry_run(args, args.industry)

    # Diff mode
    if args.diff:
        return _diff_output(args, args.industry)

    return _generate_single(args, args.industry)


def _dry_run(args, industry_id: str) -> int:
    """Preview what would be generated without writing files."""
    import tempfile

    print(f"\n  DRY RUN  --  {industry_id}  (no files written)\n")

    configs = load_all_configs(industry_id)
    sample = configs.get("sample_data", {})
    sm = configs.get("semantic_model", {})
    reports = configs.get("reports", {})

    domains = sample.get("sampleData", {}).get("domains", [])
    csv_count = sum(len(d.get("tables", [])) for d in domains)
    sm_tables = sm.get("semanticModel", {}).get("tables", [])
    sm_measures = sm.get("semanticModel", {}).get("measures", [])
    sm_rels = sm.get("semanticModel", {}).get("relationships", [])
    report_list = reports.get("reports", [])
    pages = sum(len(r.get("pages", [])) for r in report_list)

    fc = configs.get("forecast", {})
    fc_models = fc.get("forecastConfig", {}).get("models", []) or fc.get("forecastModels", [])
    htap = configs.get("htap", {})
    streams = htap.get("htapConfig", {}).get("eventStreams", []) or htap.get("eventStreams", [])
    wb = configs.get("writeback", {})
    wb_tables = wb.get("writebackConfig", {}).get("tables", []) or wb.get("tables", [])

    print(f"  Industry:       {industry_id}")
    print(f"  CSV files:      {csv_count}")
    print(f"  Notebooks:      4")
    print(f"  Dataflows:      {csv_count + len(domains)}")
    print(f"  TMDL tables:    {len(sm_tables)} (+{len(wb_tables)} writeback)")
    print(f"  DAX measures:   {len(sm_measures)}")
    print(f"  Relationships:  {len(sm_rels)}")
    print(f"  Reports:        {len(report_list)} ({pages} pages)")
    print(f"  Forecast:       {len(fc_models)} models")
    print(f"  HTAP streams:   {len(streams)}")
    print(f"  Writeback:      {len(wb_tables)} tables")
    print(f"  UDF:            {'7 functions' if wb_tables else 'none'}")
    print(f"  Data Agent:     {'yes' if configs.get('data_agent') else 'no'}")
    print(f"  Deploy scripts: 4")
    print(f"  Workspace:      2 files")
    print()
    return 0


def _diff_output(args, industry_id: str) -> int:
    """Show files that would change compared to existing output."""
    import tempfile
    import filecmp

    configs = load_all_configs(industry_id)

    existing_dir = Path(args.output) if args.output else get_output_dir(industry_id)
    if not existing_dir.exists():
        print(f"  No existing output at {existing_dir}. Run generate first.")
        return 1

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        # Save original args and generate to temp
        orig_output = args.output
        args.output = str(tmp_path)
        args.skip_validate = True
        _generate_single(args, industry_id)
        args.output = orig_output

        # Compare
        new_files = set()
        changed_files = set()
        deleted_files = set()

        tmp_all = set()
        for f in tmp_path.rglob("*"):
            if f.is_file():
                rel = f.relative_to(tmp_path)
                tmp_all.add(rel)
                existing_file = existing_dir / rel
                if not existing_file.exists():
                    new_files.add(rel)
                elif not filecmp.cmp(str(f), str(existing_file), shallow=False):
                    changed_files.add(rel)

        for f in existing_dir.rglob("*"):
            if f.is_file():
                rel = f.relative_to(existing_dir)
                parts = rel.parts
                # Skip screenshots, validation reports, and deployment artifacts
                if parts[0] in ("screenshots", "deploy") or rel.name.startswith("validation-"):
                    continue
                if rel not in tmp_all:
                    deleted_files.add(rel)

    print(f"\n  DIFF  --  {industry_id}")
    print(f"  Comparing against: {existing_dir}\n")

    if not new_files and not changed_files and not deleted_files:
        print("  No changes detected. Output is up to date.")
        return 0

    if new_files:
        print(f"  NEW ({len(new_files)}):")
        for f in sorted(new_files):
            print(f"    + {f}")
    if changed_files:
        print(f"  CHANGED ({len(changed_files)}):")
        for f in sorted(changed_files):
            print(f"    ~ {f}")
    if deleted_files:
        print(f"  REMOVED ({len(deleted_files)}):")
        for f in sorted(deleted_files):
            print(f"    - {f}")

    print(f"\n  Total: {len(new_files)} new, {len(changed_files)} changed, {len(deleted_files)} removed")
    return 0


def _generate_single(args, industry_id: str) -> int:
    """Generate a single industry demo. Returns exit code."""
    print(f"\n{'='*60}")
    print(f"  Fabric Demo Generator  --  {industry_id}")
    print(f"{'='*60}\n")

    try:
        # Step 1: Load and validate all configs
        step_start = time.time()
        print("[1/17] Loading configs...", end=" ", flush=True)
        configs = load_all_configs(industry_id)
        print(f"OK ({time.time() - step_start:.1f}s)")

        # Determine output directory
        if args.output:
            output_dir = Path(args.output)
        else:
            output_dir = get_output_dir(industry_id)

        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"       Output: {output_dir}\n")

        summary = {}

        # Step 2: Generate sample CSV data
        if configs.get("sample_data"):
            step_start = time.time()
            print("[2/17] Generating sample CSV data...", end=" ", flush=True)
            csv_paths = generate_all_csvs(configs["sample_data"], output_dir, seed=args.seed)
            summary["csv_files"] = len(csv_paths)
            print(f"OK  --  {len(csv_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[2/17] Skipping CSV generation (no sample-data.json)")
            summary["csv_files"] = 0

        # Step 3: Generate notebooks
        step_start = time.time()
        print("[3/17] Generating notebooks...", end=" ", flush=True)
        nb_paths = generate_notebooks(
            configs["industry"],
            configs.get("sample_data"),
            output_dir,
            web_enrichment_config=configs.get("web_enrichment"),
        )
        summary["notebooks"] = len(nb_paths)
        print(f"OK  --  {len(nb_paths)} notebooks ({time.time() - step_start:.1f}s)")

        # Step 4: Generate dataflows
        if configs.get("sample_data"):
            step_start = time.time()
            print("[4/17] Generating Dataflow Gen2 configs...", end=" ", flush=True)
            df_paths = generate_dataflows(
                configs["industry"],
                configs["sample_data"],
                output_dir,
            )
            summary["dataflows"] = len(df_paths)
            print(f"OK  --  {len(df_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[4/17] Skipping Dataflow generation")
            summary["dataflows"] = 0

        # Step 5: Generate semantic model (TMDL)
        if configs.get("semantic_model"):
            step_start = time.time()
            print("[5/17] Generating Semantic Model (TMDL)...", end=" ", flush=True)
            sm_result = generate_semantic_model(
                configs["industry"],
                configs["semantic_model"],
                output_dir,
            )
            total_sm = sum(len(v) for v in sm_result.values())
            summary["tmdl_tables"] = len(sm_result.get("tables", []))
            summary["tmdl_relationships"] = len(sm_result.get("relationships", []))
            # Generate separate writeback semantic model if configured
            wb_cfg = configs.get("writeback")
            if wb_cfg:
                wb_result = generate_writeback_model(
                    configs["industry"], wb_cfg, output_dir,
                )
                wb_count = len(wb_result.get("tables", []))
                summary["tmdl_tables"] += wb_count
                print(f"OK  --  {summary['tmdl_tables']} tables ({wb_count} writeback), {summary['tmdl_relationships']} relationships ({time.time() - step_start:.1f}s)")
            else:
                print(f"OK  --  {summary['tmdl_tables']} tables, {summary['tmdl_relationships']} relationships ({time.time() - step_start:.1f}s)")
        else:
            print("[5/17] Skipping Semantic Model (no semantic-model.json)")
            summary["tmdl_tables"] = 0
            summary["tmdl_relationships"] = 0

        # Step 6: Generate reports
        if configs.get("reports"):
            step_start = time.time()
            print("[6/17] Generating Power BI Reports...", end=" ", flush=True)
            report_paths = generate_reports(
                configs["industry"],
                configs["reports"],
                output_dir,
                semantic_model_config=configs.get("semantic_model"),
            )
            summary["report_files"] = len(report_paths)
            print(f"OK  --  {len(report_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[6/17] Skipping Reports (no reports.json)")
            summary["report_files"] = 0

        # Step 7: Pipeline
        step_start = time.time()
        print("[7/17] Generating Pipeline...", end=" ", flush=True)
        pl_paths = generate_pipeline(
            configs["industry"],
            configs.get("sample_data"),
            output_dir,
            skip_forecast=args.skip_forecast,
            skip_htap=args.skip_htap,
        )
        summary["pipeline_files"] = len(pl_paths)
        print(f"OK  --  {len(pl_paths)} files ({time.time() - step_start:.1f}s)")

        # Step 8: Forecast & Planning
        if not args.skip_forecast and configs.get("forecast"):
            step_start = time.time()
            print("[8/17] Generating Forecasting...", end=" ", flush=True)
            from core.forecast_generator import generate_forecast
            fc_paths = generate_forecast(configs["industry"], configs["forecast"], output_dir)
            summary["forecast"] = f"{len(fc_paths)} files"
            print(f"OK  --  {len(fc_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[8/17] Skipping Forecasting" + (" (no config)" if not configs.get("forecast") else " (--skip-forecast)"))
            summary["forecast"] = "skipped"

        # Step 9: HTAP
        if not args.skip_htap and configs.get("htap"):
            step_start = time.time()
            print("[9/17] Generating HTAP...", end=" ", flush=True)
            from core.htap_generator import generate_htap
            htap_paths = generate_htap(configs["industry"], configs["htap"], output_dir)
            summary["htap"] = f"{len(htap_paths)} files"
            print(f"OK  --  {len(htap_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[9/17] Skipping HTAP" + (" (no config)" if not configs.get("htap") else " (--skip-htap)"))
            summary["htap"] = "skipped"

        # Step 10: Writeback
        if not args.skip_writeback and configs.get("writeback"):
            step_start = time.time()
            print("[10/17] Generating Writeback...", end=" ", flush=True)
            from core.writeback_generator import generate_writeback
            wb_paths = generate_writeback(configs["industry"], configs["writeback"], output_dir)
            summary["writeback"] = f"{len(wb_paths)} files"
            print(f"OK  --  {len(wb_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[10/17] Skipping Writeback" + (" (no config)" if not configs.get("writeback") else " (--skip-writeback)"))
            summary["writeback"] = "skipped"

        # Step 11: User Data Functions (writeback API bridge)
        if not args.skip_writeback and configs.get("writeback"):
            step_start = time.time()
            print("[11/17] Generating User Data Functions...", end=" ", flush=True)
            udf_paths = generate_udf(configs["industry"], configs["writeback"], output_dir)
            summary["udf"] = f"{len(udf_paths)} files"
            print(f"OK  --  {len(udf_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[11/17] Skipping User Data Functions")
            summary["udf"] = "skipped"

        # Step 12: Data Agent
        if configs.get("data_agent"):
            step_start = time.time()
            print("[12/17] Generating Data Agent...", end=" ", flush=True)
            agent_paths = generate_data_agent(configs["industry"], configs["data_agent"], output_dir)
            summary["agent"] = f"{len(agent_paths)} files"
            print(f"OK  --  {len(agent_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[12/17] Skipping Data Agent (no data-agent.json)")
            summary["agent"] = "skipped"

        # Step 13: Deploy scripts
        if not args.skip_deploy:
            step_start = time.time()
            print("[13/17] Generating deploy scripts...", end=" ", flush=True)
            deploy_paths = generate_deploy_scripts(
                configs["industry"],
                configs.get("sample_data"),
                output_dir,
                reports_config=configs.get("reports"),
            )
            summary["deploy_files"] = len(deploy_paths)
            print(f"OK  --  {len(deploy_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[13/17] Skipping deploy scripts (--skip-deploy)")
            summary["deploy_files"] = 0

        # Step 14: Task Flow + Workspace Icon
        step_start = time.time()
        print("[14/17] Generating Task Flow + Workspace Icon...", end=" ", flush=True)
        ws_paths = generate_workspace_artifacts(
            configs["industry"],
            configs.get("sample_data"),
            output_dir,
            skip_forecast=args.skip_forecast,
            skip_htap=args.skip_htap,
            skip_writeback=args.skip_writeback,
        )
        summary["workspace"] = f"{len(ws_paths)} files"
        print(f"OK -- {len(ws_paths)} files ({time.time() - step_start:.1f}s)")

        # Step 15: Copilot instructions
        step_start = time.time()
        print("[15/17] Generating Copilot instructions...", end=" ", flush=True)
        copilot_paths = generate_copilot_instructions(
            configs["industry"],
            configs.get("semantic_model"),
            configs.get("sample_data"),
            output_dir,
        )
        summary["copilot"] = f"{len(copilot_paths)} files"
        print(f"OK  --  {len(copilot_paths)} files ({time.time() - step_start:.1f}s)")

        # Step 16: Data Activator (Reflex triggers)
        if not args.skip_htap and configs.get("htap"):
            step_start = time.time()
            print("[16/17] Generating Data Activator...", end=" ", flush=True)
            da_paths = generate_data_activator(
                configs["industry"], configs["htap"], output_dir,
            )
            summary["activator"] = f"{len(da_paths)} files"
            print(f"OK  --  {len(da_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[16/17] Skipping Data Activator (no HTAP config)")
            summary["activator"] = "skipped"

        # Step 17: Post-generation validation
        if not args.skip_validate:
            step_start = time.time()
            print("[17/17] Validating output...", end=" ", flush=True)
            validation = validate_and_report(
                configs["industry"], configs, output_dir,
                export=getattr(args, "export_validation", False))
            summary["validation"] = validation
            errors = validation["errors"]
            warnings = validation["warnings"]
            status = "PASS" if errors == 0 else "FAIL"
            print(f"{status}  --  {errors} errors, {warnings} warnings ({time.time() - step_start:.1f}s)")
            if errors > 0:
                for r in validation["results"]:
                    if r["severity"] == "ERROR":
                        print(f"       ERROR: [{r['category']}] {r['artifact']}: {r['message']}")
        else:
            print("[17/17] Skipping validation (--skip-validate)")
            summary["validation"] = "skipped"

        # Summary
        print(f"\n{'='*60}")
        print(f"  Generation Complete  --  {industry_id}")
        print(f"{'='*60}")
        print(f"  CSV files:      {summary['csv_files']}")
        print(f"  Notebooks:      {summary['notebooks']}")
        print(f"  Dataflows:      {summary['dataflows']}")
        print(f"  TMDL tables:    {summary['tmdl_tables']}")
        print(f"  Relationships:  {summary['tmdl_relationships']}")
        print(f"  Report files:   {summary['report_files']}")
        print(f"  Pipeline:       {summary['pipeline_files']}")
        print(f"  Forecast:       {summary['forecast']}")
        print(f"  HTAP:           {summary['htap']}")
        print(f"  Writeback:      {summary.get('writeback', 'skipped')}")
        print(f"  UDF:            {summary.get('udf', 'skipped')}")
        print(f"  Data Agent:     {summary.get('agent', 'skipped')}")
        print(f"  Deploy scripts: {summary.get('deploy_files', 0)}")
        print(f"  Workspace:      {summary.get('workspace', 'skipped')}")
        print(f"  Copilot:        {summary.get('copilot', 'skipped')}")
        print(f"  Activator:      {summary.get('activator', 'skipped')}")
        if isinstance(summary.get("validation"), dict):
            v = summary["validation"]
            print(f"  Validation:     {'PASS' if v['passed'] else 'FAIL'} ({v['errors']}E/{v['warnings']}W)")
        print(f"  Output:         {output_dir}")
        print()

        return 0

    except IndustryNotFoundError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        return 1
    except ConfigValidationError as e:
        print(f"\nCONFIG ERROR:\n{e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    sys.exit(main())


