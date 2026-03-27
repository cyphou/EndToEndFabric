"""Template engine for generating Fabric demo artifacts.

Reads .tpl template files and substitutes {{PLACEHOLDER}} tokens
with values from industry configs. Supports nested templates and
conditional blocks.
"""

import os
import re
from pathlib import Path

from core.config_loader import PROJECT_ROOT


TEMPLATES_DIR = PROJECT_ROOT / "templates"

# Pattern: {{KEY}} or {{KEY|default_value}}
_PLACEHOLDER_RE = re.compile(r"\{\{([A-Za-z_][A-Za-z0-9_.]*?)(?:\|([^}]*))?\}\}")

# Conditional block: {{#IF KEY}} ... {{/IF KEY}}
_IF_BLOCK_RE = re.compile(
    r"\{\{#IF\s+([A-Za-z_][A-Za-z0-9_.]*?)\s*\}\}(.*?)\{\{/IF\s+\1\s*\}\}",
    re.DOTALL,
)

# Loop block: {{#EACH items}} ... {{/EACH}}
_EACH_BLOCK_RE = re.compile(
    r"\{\{#EACH\s+([A-Za-z_][A-Za-z0-9_.]*?)\s*\}\}(.*?)\{\{/EACH\s+\1\s*\}\}",
    re.DOTALL,
)


def _resolve_value(key: str, context: dict):
    """Resolve a dotted key path (e.g. 'industry.name') from context dict."""
    parts = key.split(".")
    value = context
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
        if value is None:
            return None
    return value


def render_template(template_text: str, context: dict) -> str:
    """Render a template string with the given context.

    Supports:
      - {{KEY}} — replaced with context value (dot-notation for nesting)
      - {{KEY|default}} — uses default if KEY is missing/None
      - {{#IF KEY}} ... {{/IF KEY}} — includes block only if KEY is truthy
      - {{#EACH items}} ... {{/EACH items}} — repeats block for each item in list
    """
    result = template_text

    # 1. Process conditional blocks
    def _replace_if(match):
        key = match.group(1)
        body = match.group(2)
        val = _resolve_value(key, context)
        if val:
            return render_template(body, context)
        return ""

    result = _IF_BLOCK_RE.sub(_replace_if, result)

    # 2. Process loop blocks
    def _replace_each(match):
        key = match.group(1)
        body = match.group(2)
        items = _resolve_value(key, context)
        if not isinstance(items, list):
            return ""
        parts = []
        for i, item in enumerate(items):
            loop_ctx = {**context, "item": item, "index": i, "index1": i + 1}
            parts.append(render_template(body, loop_ctx))
        return "".join(parts)

    result = _EACH_BLOCK_RE.sub(_replace_each, result)

    # 3. Substitute placeholders
    def _replace_placeholder(match):
        key = match.group(1)
        default = match.group(2)
        val = _resolve_value(key, context)
        if val is None:
            return default if default is not None else f"{{{{MISSING:{key}}}}}"
        if isinstance(val, (list, dict)):
            return str(val)
        return str(val)

    result = _PLACEHOLDER_RE.sub(_replace_placeholder, result)

    return result


def render_template_file(template_path: Path | str, context: dict) -> str:
    """Load a template file and render it with the given context."""
    path = Path(template_path)
    if not path.is_absolute():
        path = TEMPLATES_DIR / path
    with open(path, encoding="utf-8") as f:
        template_text = f.read()
    return render_template(template_text, context)


def write_rendered(template_path: Path | str, output_path: Path | str,
                   context: dict) -> Path:
    """Render a template and write the result to output_path.

    Creates parent directories as needed. Returns the output Path.
    """
    rendered = render_template_file(template_path, context)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(rendered)
    return out


def build_context(configs: dict) -> dict:
    """Build a flat template context from loaded industry configs.

    Merges all config sections into a single dict for template resolution.
    Example: configs["industry"]["industry"]["name"] → context["industry.name"]
    is accessible as {{industry.name}} in templates.
    """
    context = {}
    for config_key, data in configs.items():
        if data is not None:
            context[config_key] = data
            # Also flatten top-level keys for convenience
            if isinstance(data, dict):
                for k, v in data.items():
                    context[k] = v
    return context


def list_templates(subfolder: str = "") -> list[Path]:
    """List all .tpl files under templates/ (optionally filtered by subfolder)."""
    search_dir = TEMPLATES_DIR / subfolder if subfolder else TEMPLATES_DIR
    if not search_dir.is_dir():
        return []
    return sorted(search_dir.rglob("*.tpl"))
