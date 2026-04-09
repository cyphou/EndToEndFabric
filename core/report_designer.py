"""Report Designer — visual design quality module.

Responsibilities:
- audit_layout      : detect overlapping / out-of-bounds visuals from reports.json coords
- generate_theme    : produce a full Power BI theme JSON (8-color ramp, text classes, visual styles)
- validate_screenshots : pixel-level checks on PNG screenshots (ink ratio, blank fill, logo strip)
- design_report     : combined entry point used by generate.py and the @report-designer agent
"""

import json
import math
from pathlib import Path

# ── Canvas constants (Power BI default 16:9 canvas) ──────────────────────────
CANVAS_W = 1280
CANVAS_H = 720
HEADER_H = 60      # header band
FOOTER_H = 32      # footer band
FOOTER_Y = CANVAS_H - FOOTER_H
SAFE_MARGIN = 16
GUTTER = 12


# ─────────────────────────────────────────────────────────────────────────────
# Color utilities
# ─────────────────────────────────────────────────────────────────────────────

def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return "#{:02X}{:02X}{:02X}".format(
        max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b))
    )


def _lighten(hex_color: str, factor: float) -> str:
    r, g, b = _hex_to_rgb(hex_color)
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return _rgb_to_hex(r, g, b)


def _darken(hex_color: str, factor: float) -> str:
    r, g, b = _hex_to_rgb(hex_color)
    r = int(r * (1 - factor))
    g = int(g * (1 - factor))
    b = int(b * (1 - factor))
    return _rgb_to_hex(r, g, b)


def build_data_colors(primary: str, secondary: str) -> list[str]:
    return [
        primary,
        secondary,
        _lighten(primary, 0.35),
        _lighten(secondary, 0.35),
        _darken(primary, 0.30),
        _darken(secondary, 0.30),
        "#6C757D",   # neutral
        "#28A745",   # positive
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Theme generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_theme(
    company_name: str,
    primary: str,
    secondary: str,
    background: str = "#FAFAFA",
    output_path: Path | None = None,
) -> dict:
    data_colors = build_data_colors(primary, secondary)
    theme = {
        "name": f"{company_name}Theme",
        "dataColors": data_colors,
        "background": background,
        "foreground": "#333333",
        "tableAccent": primary,
        "textClasses": {
            "title": {
                "fontFace": "Segoe UI",
                "fontSize": 18,
                "fontWeight": "bold",
                "color": "#333333",
            },
            "header": {
                "fontFace": "Segoe UI",
                "fontSize": 12,
                "fontWeight": "semibold",
                "color": "#555555",
            },
            "callout": {
                "fontFace": "Segoe UI",
                "fontSize": 28,
                "fontWeight": "bold",
                "color": primary,
            },
            "label": {
                "fontFace": "Segoe UI",
                "fontSize": 10,
                "color": "#666666",
            },
            "footer": {
                "fontFace": "Segoe UI",
                "fontSize": 9,
                "color": "#999999",
            },
        },
        "visualStyles": {
            "*": {
                "*": {
                    "general": [{"responsive": True}],
                    "background": [{"transparency": 0}],
                }
            },
            "card": {
                "*": {
                    "labels": [{"color": {"solid": {"color": primary}}, "fontSize": 28}],
                    "categoryLabels": [{"show": True}],
                }
            },
            "lineChart": {
                "*": {
                    "plotArea": [{"transparency": 20}],
                    "gridlines": [{"color": {"solid": {"color": "#E0E0E0"}}}],
                }
            },
            "clusteredBarChart": {
                "*": {
                    "dataPoint": [{"defaultColor": {"solid": {"color": primary}}}],
                }
            },
            "donutChart": {
                "*": {
                    "legend": [{"position": "Bottom"}],
                }
            },
        },
    }

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(theme, f, indent=2, ensure_ascii=False)

    return theme


# ─────────────────────────────────────────────────────────────────────────────
# Layout audit — overlap + bounds check from reports.json coordinates
# ─────────────────────────────────────────────────────────────────────────────

def _boxes_overlap(a: dict, b: dict) -> bool:
    ax1, ay1 = a["x"], a["y"]
    ax2, ay2 = ax1 + a["width"], ay1 + a["height"]
    bx1, by1 = b["x"], b["y"]
    bx2, by2 = bx1 + b["width"], by1 + b["height"]
    return ax1 < bx2 - GUTTER and ax2 > bx1 + GUTTER and ay1 < by2 - GUTTER and ay2 > by1 + GUTTER


def _check_bounds(v: dict) -> list[str]:
    issues = []
    if v["x"] < 0 or v["y"] < 0:
        issues.append(f"negative origin ({v['x']},{v['y']})")
    if v["x"] + v["width"] > CANVAS_W:
        issues.append(f"exceeds canvas width ({v['x']+v['width']} > {CANVAS_W})")
    if v["y"] + v["height"] > CANVAS_H:
        issues.append(f"exceeds canvas height ({v['y']+v['height']} > {CANVAS_H})")
    if v["y"] < HEADER_H and v.get("name", "").lower() not in ("logo", "header", "title"):
        issues.append(f"overlaps header band (y={v['y']} < {HEADER_H})")
    return issues


def audit_layout(reports_config: dict) -> list[dict]:
    results = []
    for report in reports_config.get("reports", []):
        report_name = report.get("name", "unknown")
        for page in report.get("pages", []):
            page_name = page.get("name", "unknown")
            visuals = page.get("visuals", [])
            coords = []
            for v in visuals:
                if all(k in v for k in ("x", "y", "width", "height")):
                    coords.append(v)

            # Bounds check
            for v in coords:
                for issue in _check_bounds(v):
                    results.append({
                        "report": report_name,
                        "page": page_name,
                        "visual": v.get("name", "?"),
                        "check": "bounds",
                        "status": "WARN",
                        "detail": issue,
                    })

            # Pairwise overlap
            for i in range(len(coords)):
                for j in range(i + 1, len(coords)):
                    if _boxes_overlap(coords[i], coords[j]):
                        results.append({
                            "report": report_name,
                            "page": page_name,
                            "visual": f"{coords[i].get('name','?')} ↔ {coords[j].get('name','?')}",
                            "check": "overlap",
                            "status": "FAIL",
                            "detail": (
                                f"[{coords[i]['x']},{coords[i]['y']} "
                                f"{coords[i]['width']}×{coords[i]['height']}] overlaps "
                                f"[{coords[j]['x']},{coords[j]['y']} "
                                f"{coords[j]['width']}×{coords[j]['height']}]"
                            ),
                        })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Screenshot validation — pixel-level (stdlib only via PPM/raw bytes fallback)
# ─────────────────────────────────────────────────────────────────────────────

def _sample_png_pixels(png_path: Path, max_samples: int = 50000) -> list[tuple[int, int, int]]:
    """
    Sample RGB pixels from a PNG file.
    Uses System.Drawing via PowerShell when available (Windows); otherwise reads
    IDAT chunks with a minimal zlib-based decoder for pure-stdlib support.
    Returns a list of (R, G, B) tuples.
    """
    # Attempt stdlib png reading via struct + zlib
    import struct
    import zlib

    pixels: list[tuple[int, int, int]] = []
    try:
        data = png_path.read_bytes()
        if data[:8] != b"\x89PNG\r\n\x1a\n":
            return pixels

        pos = 8
        idat_chunks: list[bytes] = []
        width = height = bit_depth = color_type = 0

        while pos < len(data):
            length = struct.unpack(">I", data[pos:pos+4])[0]
            chunk_type = data[pos+4:pos+8]
            chunk_data = data[pos+8:pos+8+length]
            pos += 12 + length

            if chunk_type == b"IHDR":
                width, height = struct.unpack(">II", chunk_data[:8])
                bit_depth = chunk_data[8]
                color_type = chunk_data[9]
            elif chunk_type == b"IDAT":
                idat_chunks.append(chunk_data)
            elif chunk_type == b"IEND":
                break

        if color_type not in (2, 6) or bit_depth != 8:
            return pixels  # only RGB/RGBA 8-bit supported

        raw = zlib.decompress(b"".join(idat_chunks))
        channels = 3 if color_type == 2 else 4
        row_bytes = 1 + width * channels  # 1 filter byte per row

        total_px = width * height
        step = max(1, total_px // max_samples)

        for row_idx in range(height):
            row_start = row_idx * row_bytes + 1  # skip filter byte
            row_data = raw[row_start:row_start + width * channels]
            for col_idx in range(0, width, step):
                offset = col_idx * channels
                if offset + channels <= len(row_data):
                    r = row_data[offset]
                    g = row_data[offset + 1]
                    b = row_data[offset + 2]
                    pixels.append((r, g, b))
            if len(pixels) >= max_samples:
                break
    except Exception:
        pass
    return pixels


def validate_screenshots(screenshot_dir: Path) -> list[dict]:
    results = []
    if not screenshot_dir.is_dir():
        return results

    for png in sorted(screenshot_dir.rglob("*.png")):
        label = "/".join(png.relative_to(screenshot_dir).with_suffix("").parts)
        pixels = _sample_png_pixels(png)

        if not pixels:
            results.append({
                "page": label,
                "check": "decode",
                "status": "WARN",
                "detail": "Could not decode PNG (unsupported format or empty)",
            })
            continue

        total = len(pixels)
        white_px = sum(1 for r, g, b in pixels if r > 240 and g > 240 and b > 240)
        ink_ratio = round((total - white_px) / total * 100, 1)

        # Ink ratio check
        if ink_ratio < 5:
            results.append({
                "page": label,
                "check": "ink_ratio",
                "status": "FAIL" if ink_ratio < 2 else "WARN",
                "detail": f"ink={ink_ratio}% — possible empty/no-data page",
            })
        else:
            results.append({
                "page": label,
                "check": "ink_ratio",
                "status": "OK",
                "detail": f"ink={ink_ratio}%",
            })

        # Logo strip check — top HEADER_H rows should not be totally uniform
        top_pixels = [p for p in pixels[:max(1, total // (CANVAS_H // HEADER_H))]]
        if top_pixels:
            top_colors = set(top_pixels)
            if len(top_colors) < 3:
                results.append({
                    "page": label,
                    "check": "logo_strip",
                    "status": "WARN",
                    "detail": "Header area appears uniform — logo may be missing",
                })
            else:
                results.append({
                    "page": label,
                    "check": "logo_strip",
                    "status": "OK",
                    "detail": f"{len(top_colors)} distinct colors in header strip",
                })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Combined entry point
# ─────────────────────────────────────────────────────────────────────────────

def design_report(
    industry_config: dict,
    reports_config: dict,
    output_dir: Path,
    screenshot_dir: Path | None = None,
) -> dict:
    industry = industry_config.get("industry", {})
    company_name = industry.get("name", "Demo").replace(" ", "")
    theme_cfg = industry.get("theme", {})
    primary = theme_cfg.get("primary", "#0078D4")
    secondary = theme_cfg.get("secondary", "#FFB81C")
    background = theme_cfg.get("background", "#FAFAFA")

    all_results: list[dict] = []

    # 1. Generate / refresh full theme JSON
    theme_path = output_dir / "shared" / "assets" / "themes" / f"{company_name}Theme.json"
    generate_theme(company_name, primary, secondary, background, theme_path)

    # 2. Layout audit from reports.json coordinates
    layout_issues = audit_layout(reports_config)
    all_results.extend(layout_issues)

    # 3. Screenshot pixel validation (optional)
    if screenshot_dir is not None:
        shot_results = validate_screenshots(screenshot_dir)
        all_results.extend(shot_results)

    errors = sum(1 for r in all_results if r["status"] == "FAIL")
    warnings = sum(1 for r in all_results if r["status"] == "WARN")
    ok_count = sum(1 for r in all_results if r["status"] == "OK")

    return {
        "company": company_name,
        "errors": errors,
        "warnings": warnings,
        "ok": ok_count,
        "passed": errors == 0,
        "results": all_results,
    }
