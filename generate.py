#!/usr/bin/env python3
"""Fabric End-to-End Industry Demo Generator.

Main entry point — generates a complete Fabric demo project from
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
from core.tmdl_generator import generate_semantic_model
from core.report_generator import generate_reports
from core.pipeline_generator import generate_pipeline
from core.deploy_generator import generate_deploy_scripts
from core.agent_generator import generate_data_agent


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
        "--seed", type=int, default=42,
        help="Random seed for reproducible data generation (default: 42)",
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

    # Require --industry for generation
    if not args.industry:
        parser.print_help()
        return 1

    industry_id = args.industry
    print(f"\n{'='*60}")
    print(f"  Fabric Demo Generator — {industry_id}")
    print(f"{'='*60}\n")

    try:
        # Step 1: Load and validate all configs
        step_start = time.time()
        print("[1/12] Loading configs...", end=" ", flush=True)
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
            print("[2/12] Generating sample CSV data...", end=" ", flush=True)
            csv_paths = generate_all_csvs(configs["sample_data"], output_dir, seed=args.seed)
            summary["csv_files"] = len(csv_paths)
            print(f"OK — {len(csv_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[2/12] Skipping CSV generation (no sample-data.json)")
            summary["csv_files"] = 0

        # Step 3: Generate notebooks
        step_start = time.time()
        print("[3/12] Generating notebooks...", end=" ", flush=True)
        nb_paths = generate_notebooks(
            configs["industry"],
            configs.get("sample_data"),
            output_dir,
        )
        summary["notebooks"] = len(nb_paths)
        print(f"OK — {len(nb_paths)} notebooks ({time.time() - step_start:.1f}s)")

        # Step 4: Generate dataflows
        if configs.get("sample_data"):
            step_start = time.time()
            print("[4/12] Generating Dataflow Gen2 configs...", end=" ", flush=True)
            df_paths = generate_dataflows(
                configs["industry"],
                configs["sample_data"],
                output_dir,
            )
            summary["dataflows"] = len(df_paths)
            print(f"OK — {len(df_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[4/12] Skipping Dataflow generation")
            summary["dataflows"] = 0

        # Step 5: Generate semantic model (TMDL)
        if configs.get("semantic_model"):
            step_start = time.time()
            print("[5/12] Generating Semantic Model (TMDL)...", end=" ", flush=True)
            sm_result = generate_semantic_model(
                configs["industry"],
                configs["semantic_model"],
                output_dir,
            )
            total_sm = sum(len(v) for v in sm_result.values())
            summary["tmdl_tables"] = len(sm_result.get("tables", []))
            summary["tmdl_relationships"] = len(sm_result.get("relationships", []))
            print(f"OK — {summary['tmdl_tables']} tables, {summary['tmdl_relationships']} relationships ({time.time() - step_start:.1f}s)")
        else:
            print("[5/12] Skipping Semantic Model (no semantic-model.json)")
            summary["tmdl_tables"] = 0
            summary["tmdl_relationships"] = 0

        # Step 6: Generate reports
        if configs.get("reports"):
            step_start = time.time()
            print("[6/12] Generating Power BI Reports...", end=" ", flush=True)
            report_paths = generate_reports(
                configs["industry"],
                configs["reports"],
                output_dir,
            )
            summary["report_files"] = len(report_paths)
            print(f"OK — {len(report_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[6/12] Skipping Reports (no reports.json)")
            summary["report_files"] = 0

        # Step 7: Pipeline
        step_start = time.time()
        print("[7/12] Generating Pipeline...", end=" ", flush=True)
        pl_paths = generate_pipeline(
            configs["industry"],
            configs.get("sample_data"),
            output_dir,
        )
        summary["pipeline_files"] = len(pl_paths)
        print(f"OK — {len(pl_paths)} files ({time.time() - step_start:.1f}s)")

        # Step 8: Forecast & Planning
        if not args.skip_forecast and configs.get("forecast"):
            step_start = time.time()
            print("[8/12] Generating Forecasting...", end=" ", flush=True)
            from core.forecast_generator import generate_forecast
            fc_paths = generate_forecast(configs["industry"], configs["forecast"], output_dir)
            summary["forecast"] = f"{len(fc_paths)} files"
            print(f"OK — {len(fc_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[8/12] Skipping Forecasting" + (" (no config)" if not configs.get("forecast") else " (--skip-forecast)"))
            summary["forecast"] = "skipped"

        # Step 9: HTAP
        if not args.skip_htap and configs.get("htap"):
            step_start = time.time()
            print("[9/12] Generating HTAP...", end=" ", flush=True)
            from core.htap_generator import generate_htap
            htap_paths = generate_htap(configs["industry"], configs["htap"], output_dir)
            summary["htap"] = f"{len(htap_paths)} files"
            print(f"OK — {len(htap_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[9/12] Skipping HTAP" + (" (no config)" if not configs.get("htap") else " (--skip-htap)"))
            summary["htap"] = "skipped"

        # Step 10: Writeback
        if not args.skip_writeback and configs.get("writeback"):
            step_start = time.time()
            print("[10/12] Generating Writeback...", end=" ", flush=True)
            from core.writeback_generator import generate_writeback
            wb_paths = generate_writeback(configs["industry"], configs["writeback"], output_dir)
            summary["writeback"] = f"{len(wb_paths)} files"
            print(f"OK — {len(wb_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[10/12] Skipping Writeback" + (" (no config)" if not configs.get("writeback") else " (--skip-writeback)"))
            summary["writeback"] = "skipped"

        # Step 11: Data Agent
        if configs.get("data_agent"):
            step_start = time.time()
            print("[11/12] Generating Data Agent...", end=" ", flush=True)
            agent_paths = generate_data_agent(configs["industry"], configs["data_agent"], output_dir)
            summary["agent"] = f"{len(agent_paths)} files"
            print(f"OK — {len(agent_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[11/12] Skipping Data Agent (no data-agent.json)")
            summary["agent"] = "skipped"

        # Step 12: Deploy scripts
        if not args.skip_deploy:
            step_start = time.time()
            print("[12/12] Generating deploy scripts...", end=" ", flush=True)
            deploy_paths = generate_deploy_scripts(
                configs["industry"],
                configs.get("sample_data"),
                output_dir,
            )
            summary["deploy_files"] = len(deploy_paths)
            print(f"OK — {len(deploy_paths)} files ({time.time() - step_start:.1f}s)")
        else:
            print("[12/12] Skipping deploy scripts (--skip-deploy)")
            summary["deploy_files"] = 0

        # Summary
        print(f"\n{'='*60}")
        print(f"  Generation Complete — {industry_id}")
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
        print(f"  Data Agent:     {summary.get('agent', 'skipped')}")
        print(f"  Deploy scripts: {summary.get('deploy_files', 0)}")
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
