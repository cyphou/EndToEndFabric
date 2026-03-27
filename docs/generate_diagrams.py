"""Generate documentation PNG diagrams for Fabric End-to-End Demo Generator."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
import os

DOCS_DIR = os.path.join(os.path.dirname(__file__), "images")
os.makedirs(DOCS_DIR, exist_ok=True)

# ── Shared palette ──────────────────────────────────────────────────────────
FABRIC_BLUE   = "#0078D4"
FABRIC_DARK   = "#002050"
FABRIC_LIGHT  = "#50E6FF"
FABRIC_ORANGE = "#FF8C00"
FABRIC_GREEN  = "#107C10"
FABRIC_RED    = "#D83B01"
FABRIC_PURPLE = "#8661C5"
BG_COLOR      = "#F9FAFB"
CARD_BG       = "#FFFFFF"
BORDER_COLOR  = "#E1E4E8"

def save(fig, name):
    path = os.path.join(DOCS_DIR, name)
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  -> {path}")


# ═══════════════════════════════════════════════════════════════════════════════
#  1. HERO BANNER — Project overview
# ═══════════════════════════════════════════════════════════════════════════════
def gen_hero():
    fig, ax = plt.subplots(figsize=(14, 4.5))
    fig.set_facecolor(FABRIC_DARK)
    ax.set_xlim(0, 14); ax.set_ylim(0, 4.5); ax.axis("off")

    # Title area
    ax.text(1, 3.4, "Fabric End-to-End Industry Demo Generator",
            fontsize=26, fontweight="bold", color="white", family="Segoe UI")
    ax.text(1, 2.7, "Generate complete Microsoft Fabric demos for any industry in one command",
            fontsize=13, color="#B0C4DE", family="Segoe UI")

    # Stat cards
    stats = [
        ("3", "Industries"),
        ("10", "Generator\nSteps"),
        ("73", "Passing\nTests"),
        ("0", "External\nDeps"),
    ]
    for i, (num, label) in enumerate(stats):
        x = 1.2 + i * 3.1
        rect = FancyBboxPatch((x, 0.4), 2.5, 1.8, boxstyle="round,pad=0.15",
                              facecolor="#FFFFFF15", edgecolor="#FFFFFF30", linewidth=1.2)
        ax.add_patch(rect)
        ax.text(x + 1.25, 1.55, num, fontsize=32, fontweight="bold",
                color=FABRIC_LIGHT, ha="center", va="center", family="Segoe UI")
        ax.text(x + 1.25, 0.8, label, fontsize=10, color="#B0C4DE",
                ha="center", va="center", family="Segoe UI")

    save(fig, "hero-banner.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  2. PIPELINE ARCHITECTURE — 10-step generation flow
# ═══════════════════════════════════════════════════════════════════════════════
def gen_pipeline():
    fig, ax = plt.subplots(figsize=(16, 6))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 16); ax.set_ylim(0, 6); ax.axis("off")

    ax.text(8, 5.6, "10-Step Generation Pipeline", fontsize=20,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    steps = [
        ("1", "Load\nConfigs",    FABRIC_BLUE),
        ("2", "Generate\nCSV",    FABRIC_GREEN),
        ("3", "Generate\nNotebooks", FABRIC_GREEN),
        ("4", "Generate\nDataflows", FABRIC_GREEN),
        ("5", "Generate\nTMDL",   FABRIC_PURPLE),
        ("6", "Generate\nReports", FABRIC_ORANGE),
        ("7", "Generate\nPipeline", FABRIC_BLUE),
        ("8", "Generate\nForecast", FABRIC_RED),
        ("9", "Generate\nHTAP",   FABRIC_RED),
        ("10", "Generate\nDeploy", FABRIC_DARK),
    ]

    for i, (num, label, color) in enumerate(steps):
        col = i % 5
        row = 1 - i // 5
        x = 0.8 + col * 3.0
        y = 0.6 + row * 2.5

        rect = FancyBboxPatch((x, y), 2.4, 2.0, boxstyle="round,pad=0.15",
                              facecolor=color, edgecolor="white", linewidth=1.5, alpha=0.9)
        ax.add_patch(rect)
        ax.text(x + 1.2, y + 1.3, num, fontsize=22, fontweight="bold",
                color="white", ha="center", va="center", family="Segoe UI")
        ax.text(x + 1.2, y + 0.5, label, fontsize=9, color="white",
                ha="center", va="center", family="Segoe UI")

        # Arrow to next
        if i < 9 and i != 4:
            ax.annotate("", xy=(x + 2.6, y + 1.0), xytext=(x + 2.4, y + 1.0),
                        arrowprops=dict(arrowstyle="->", color="#AAA", lw=1.5))

    # Arrow from row 1 to row 2
    ax.annotate("", xy=(14.4, 2.6), xytext=(14.4, 3.1),
                arrowprops=dict(arrowstyle="->", color="#AAA", lw=2, connectionstyle="arc3,rad=-0.3"))

    save(fig, "pipeline-architecture.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  3. MEDALLION ARCHITECTURE
# ═══════════════════════════════════════════════════════════════════════════════
def gen_medallion():
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 14); ax.set_ylim(0, 5); ax.axis("off")

    ax.text(7, 4.6, "Medallion Lakehouse Architecture", fontsize=18,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    zones = [
        ("Bronze", "#CD7F32", 0.5, ["Raw CSV ingestion", "1:1 from source", "No transforms"]),
        ("Silver", "#C0C0C0", 4.8, ["Cleaned & typed", "Dedup / null handling", "Domain schemas"]),
        ("Gold",   "#FFD700", 9.1, ["Star schema", "Dim/Fact tables", "Analytics-ready"]),
    ]

    for name, color, x, items in zones:
        rect = FancyBboxPatch((x, 0.5), 3.8, 3.5, boxstyle="round,pad=0.2",
                              facecolor=color, edgecolor="white", linewidth=2, alpha=0.25)
        ax.add_patch(rect)
        inner = FancyBboxPatch((x + 0.15, 0.65), 3.5, 3.2, boxstyle="round,pad=0.15",
                               facecolor="white", edgecolor=color, linewidth=1.5, alpha=0.9)
        ax.add_patch(inner)
        ax.text(x + 1.9, 3.5, name, fontsize=16, fontweight="bold",
                color=color if name != "Gold" else "#B8860B",
                ha="center", va="center", family="Segoe UI")
        for j, item in enumerate(items):
            ax.text(x + 1.9, 2.7 - j * 0.55, f"  {item}",
                    fontsize=10, color="#444", ha="center", va="center", family="Segoe UI")

    # Arrows between zones
    for x in [4.35, 8.65]:
        ax.annotate("", xy=(x + 0.3, 2.25), xytext=(x, 2.25),
                    arrowprops=dict(arrowstyle="-|>", color=FABRIC_BLUE, lw=2.5))

    # Notebook labels on arrows
    ax.text(4.65, 1.3, "NB01\nBronze\u2192Silver", fontsize=8, color=FABRIC_BLUE,
            ha="center", family="Segoe UI", style="italic")
    ax.text(8.95, 1.3, "NB03\nSilver\u2192Gold", fontsize=8, color=FABRIC_BLUE,
            ha="center", family="Segoe UI", style="italic")

    save(fig, "medallion-architecture.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  4. MULTI-AGENT MODEL
# ═══════════════════════════════════════════════════════════════════════════════
def gen_agents():
    fig, ax = plt.subplots(figsize=(14, 7))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 14); ax.set_ylim(0, 7); ax.axis("off")

    ax.text(7, 6.6, "Multi-Agent Architecture (9+1)", fontsize=18,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    # Orchestrator at top
    rect = FancyBboxPatch((4.5, 5.2), 5, 1.1, boxstyle="round,pad=0.15",
                          facecolor=FABRIC_BLUE, edgecolor="white", linewidth=2)
    ax.add_patch(rect)
    ax.text(7, 5.75, "Orchestrator", fontsize=14, fontweight="bold",
            color="white", ha="center", va="center", family="Segoe UI")

    # Agents in rows
    agents = [
        [("Data\nEngineer", FABRIC_GREEN), ("Semantic\nModel", FABRIC_PURPLE),
         ("Report\nBuilder", FABRIC_ORANGE), ("Forecaster", FABRIC_RED)],
        [("HTAP\nEngineer", "#B22222"), ("Deployer", FABRIC_DARK),
         ("Tester", "#2F4F4F"), ("Industry\nDesigner", "#6B8E23")],
    ]

    for row_i, row in enumerate(agents):
        y = 3.3 - row_i * 2.0
        for col_i, (name, color) in enumerate(row):
            x = 0.7 + col_i * 3.3
            rect = FancyBboxPatch((x, y), 2.7, 1.5, boxstyle="round,pad=0.12",
                                  facecolor=color, edgecolor="white", linewidth=1.5, alpha=0.9)
            ax.add_patch(rect)
            ax.text(x + 1.35, y + 0.75, name, fontsize=10, fontweight="bold",
                    color="white", ha="center", va="center", family="Segoe UI")
            # Connection line to orchestrator
            ax.plot([x + 1.35, 7], [y + 1.5, 5.2], color="#CCC", linewidth=0.8,
                    linestyle="--", alpha=0.6)

    # Shared instructions badge
    rect = FancyBboxPatch((5, 0.2), 4, 0.7, boxstyle="round,pad=0.1",
                          facecolor="#F0F0F0", edgecolor=BORDER_COLOR, linewidth=1)
    ax.add_patch(rect)
    ax.text(7, 0.55, "shared.instructions.md  (Hard Rules for All Agents)",
            fontsize=9, color="#666", ha="center", va="center", family="Segoe UI", style="italic")

    save(fig, "multi-agent-architecture.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  5. PER-INDUSTRY CARDS
# ═══════════════════════════════════════════════════════════════════════════════
def gen_industry_card(name, icon, primary, secondary, domains, tables, rows,
                      sm_tables, sm_rels, measures, reports, pages, visuals,
                      forecast, planning, streams, filename):
    fig, ax = plt.subplots(figsize=(10, 7))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 10); ax.set_ylim(0, 7); ax.axis("off")

    # Header band
    header = FancyBboxPatch((0.3, 5.5), 9.4, 1.3, boxstyle="round,pad=0.15",
                            facecolor=primary, edgecolor="white", linewidth=2)
    ax.add_patch(header)
    ax.text(5, 6.35, f"{icon}  {name}", fontsize=22, fontweight="bold",
            color="white", ha="center", va="center", family="Segoe UI")
    ax.text(5, 5.8, " | ".join(domains), fontsize=10,
            color="#FFFFFFCC", ha="center", va="center", family="Segoe UI")

    # Data card
    def card(x, y, w, h, title, items, accent):
        bg = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.12",
                            facecolor="white", edgecolor=accent, linewidth=1.5)
        ax.add_patch(bg)
        ax.text(x + w/2, y + h - 0.3, title, fontsize=11, fontweight="bold",
                color=accent, ha="center", va="center", family="Segoe UI")
        for i, (k, v) in enumerate(items):
            ax.text(x + 0.3, y + h - 0.7 - i * 0.4, k,
                    fontsize=9, color="#666", va="center", family="Segoe UI")
            ax.text(x + w - 0.3, y + h - 0.7 - i * 0.4, str(v),
                    fontsize=10, fontweight="bold", color="#222",
                    ha="right", va="center", family="Segoe UI")

    card(0.3, 2.8, 4.5, 2.4, "Data Foundation", [
        ("CSV Tables", tables),
        ("Total Rows", f"{rows:,}"),
        ("Semantic Tables", sm_tables),
        ("Relationships", sm_rels),
    ], primary)

    card(5.2, 2.8, 4.5, 2.4, "Analytics Layer", [
        ("DAX Measures", measures),
        ("Reports", reports),
        ("Report Pages", pages),
        ("Visual Configs", visuals),
    ], secondary)

    card(0.3, 0.3, 4.5, 2.2, "Advanced Analytics", [
        ("Forecast Models", forecast),
        ("Planning Tables", planning),
        ("Event Streams", streams),
    ], FABRIC_PURPLE)

    # Theme swatch
    swatch_bg = FancyBboxPatch((5.2, 0.3), 4.5, 2.2, boxstyle="round,pad=0.12",
                               facecolor="white", edgecolor=BORDER_COLOR, linewidth=1.5)
    ax.add_patch(swatch_bg)
    ax.text(7.45, 2.15, "Theme", fontsize=11, fontweight="bold",
            color="#666", ha="center", va="center", family="Segoe UI")
    for i, (lbl, col) in enumerate([("Primary", primary), ("Secondary", secondary)]):
        cx = 6.4 + i * 2.1
        circ = plt.Circle((cx, 1.35), 0.35, color=col, ec="white", linewidth=2)
        ax.add_patch(circ)
        ax.text(cx, 0.75, f"{lbl}\n{col}", fontsize=7, color="#666",
                ha="center", va="center", family="Segoe UI")

    save(fig, filename)


def gen_industries():
    gen_industry_card(
        "Horizon Books Publishing", "\U0001F4DA",
        "#1B3A5C", "#E8A838",
        ["Finance", "HR", "Operations"],
        17, 9496, 18, 14, 20, 2, 15, 53, 5, 5, 3,
        "industry-horizon-books.png"
    )
    gen_industry_card(
        "Contoso Energy", "\u26A1",
        "#2E7D32", "#FF6F00",
        ["Generation", "Grid Ops", "Billing", "Sustainability", "Field Ops"],
        13, 25365, 14, 9, 20, 3, 20, 65, 5, 5, 3,
        "industry-contoso-energy.png"
    )
    gen_industry_card(
        "Northwind HR & Finance", "\U0001F3E2",
        "#1565C0", "#E65100",
        ["HR", "Payroll", "Finance", "Budgeting", "Performance"],
        19, 39085, 20, 16, 24, 3, 22, 67, 5, 5, 3,
        "industry-northwind-hrfinance.png"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  6. OUTPUT STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════
def gen_output_structure():
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 12); ax.set_ylim(0, 8); ax.axis("off")

    ax.text(6, 7.6, "Generated Output Structure", fontsize=18,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    artifacts = [
        ("\U0001F4C1 SampleData/",     FABRIC_GREEN,  0.5, 6.2, ["domain1/Table.csv", "domain2/Table.csv", "..."]),
        ("\U0001F4D3 Notebooks/",       FABRIC_BLUE,   0.5, 4.5, ["NB01_Bronze_to_Silver.py", "NB02_Web_Enrichment.py",
                                                                    "NB03_Silver_to_Gold.py", "NB06_Diagnostic.py"]),
        ("\U0001F504 Dataflows/",       FABRIC_ORANGE, 6.2, 6.2, ["DF_domain_ingestion.json", "..."]),
        ("\U0001F4CA SemanticModel/",   FABRIC_PURPLE, 6.2, 4.5, ["model.tmdl", "tables/Dim*.tmdl",
                                                                     "tables/Fact*.tmdl", "relationships/"]),
        ("\U0001F4C8 Reports/",         FABRIC_RED,    0.5, 2.5, ["Analytics/pages/", "Forecasting/pages/",
                                                                    "theme.json", "report.json"]),
        ("\u2699\ufe0f Pipeline/",      FABRIC_DARK,   6.2, 2.5, ["pipeline-content.json", "README.md"]),
        ("\U0001F52E Forecast/",        "#B22222",     0.5, 0.6, ["NB04_Forecast.py", "forecast-config.json"]),
        ("\u26A1 HTAP/",               "#6B8E23",     6.2, 0.6, ["Eventhouse/", "KQL/", "NB05_EventSim.py"]),
    ]

    for title, color, x, y, items in artifacts:
        w = 5.0
        h = 1.4 if len(items) <= 3 else 1.6
        rect = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1",
                              facecolor="white", edgecolor=color, linewidth=1.8)
        ax.add_patch(rect)
        ax.text(x + 0.2, y + h - 0.3, title, fontsize=10, fontweight="bold",
                color=color, va="center", family="Segoe UI")
        for j, item in enumerate(items[:3]):
            ax.text(x + 0.4, y + h - 0.6 - j * 0.3, f"\u2022 {item}",
                    fontsize=8, color="#555", va="center", family="Consolas")

    save(fig, "output-structure.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  7. TECHNOLOGY STACK
# ═══════════════════════════════════════════════════════════════════════════════
def gen_tech_stack():
    fig, ax = plt.subplots(figsize=(12, 4.5))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 12); ax.set_ylim(0, 4.5); ax.axis("off")

    ax.text(6, 4.1, "Technology Stack", fontsize=18,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    techs = [
        ("Python 3.12+", "Zero deps\ncore engine", FABRIC_BLUE),
        ("PySpark", "Notebook\npipelines", FABRIC_GREEN),
        ("Power Query M", "Dataflow Gen2\ningestion", FABRIC_ORANGE),
        ("TMDL", "Semantic model\nDirect Lake", FABRIC_PURPLE),
        ("PBIR v4.0", "Report\ndefinitions", FABRIC_RED),
        ("PowerShell", "Deploy scripts\n& Pester tests", FABRIC_DARK),
    ]

    for i, (name, desc, color) in enumerate(techs):
        x = 0.3 + i * 1.95
        rect = FancyBboxPatch((x, 0.5), 1.7, 3.0, boxstyle="round,pad=0.12",
                              facecolor=color, edgecolor="white", linewidth=1.5, alpha=0.9)
        ax.add_patch(rect)
        ax.text(x + 0.85, 2.6, name, fontsize=10, fontweight="bold",
                color="white", ha="center", va="center", family="Segoe UI")
        ax.text(x + 0.85, 1.6, desc, fontsize=8, color="#FFFFFFCC",
                ha="center", va="center", family="Segoe UI")

    save(fig, "tech-stack.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  8. CONFIG-DRIVEN DESIGN
# ═══════════════════════════════════════════════════════════════════════════════
def gen_config_design():
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 14); ax.set_ylim(0, 5); ax.axis("off")

    ax.text(7, 4.6, "Config-Driven Industry Design", fontsize=18,
            fontweight="bold", color=FABRIC_DARK, ha="center", family="Segoe UI")

    configs = [
        ("industry.json",       "Identity, domains\ntheme, Fabric artifacts", FABRIC_BLUE),
        ("sample-data.json",    "Tables, columns\nrow counts, FK refs", FABRIC_GREEN),
        ("semantic-model.json", "TMDL tables\nmeasures, relationships", FABRIC_PURPLE),
        ("reports.json",        "Report pages\nvisuals, themes", FABRIC_ORANGE),
        ("forecast-config.json","Holt-Winters models\nMLflow tracking", FABRIC_RED),
        ("planning-config.json","Planning tables\nscenarios", "#B22222"),
        ("htap-config.json",    "Eventhouse, KQL\nevent streams", "#6B8E23"),
    ]

    for i, (name, desc, color) in enumerate(configs):
        x = 0.3 + i * 1.95
        rect = FancyBboxPatch((x, 0.5), 1.7, 3.5, boxstyle="round,pad=0.1",
                              facecolor="white", edgecolor=color, linewidth=2)
        ax.add_patch(rect)
        # Colored top bar
        bar = FancyBboxPatch((x + 0.05, 3.4), 1.6, 0.5, boxstyle="round,pad=0.05",
                             facecolor=color, edgecolor=color, linewidth=0)
        ax.add_patch(bar)
        ax.text(x + 0.85, 3.65, "{ }", fontsize=12, fontweight="bold",
                color="white", ha="center", va="center", family="Consolas")
        ax.text(x + 0.85, 2.7, name.replace("-", "-\n") if len(name) > 16 else name,
                fontsize=8, fontweight="bold", color=color, ha="center", va="center",
                family="Consolas")
        ax.text(x + 0.85, 1.5, desc, fontsize=7.5, color="#555",
                ha="center", va="center", family="Segoe UI")

    save(fig, "config-driven-design.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  RUN ALL
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Generating documentation PNGs...")
    gen_hero()
    gen_pipeline()
    gen_medallion()
    gen_agents()
    gen_industries()
    gen_output_structure()
    gen_tech_stack()
    gen_config_design()
    print("Done! All PNGs saved to docs/images/")
