"""Configuration loader and validator for industry demo configs.

Loads industry JSON configs from industries/<id>/ and validates
them against JSON schemas in core/schemas/.
"""

import json
import os
from pathlib import Path


# Root of the project (parent of core/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDUSTRIES_DIR = PROJECT_ROOT / "industries"
SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"

# Config file names expected in each industry folder
CONFIG_FILES = {
    "industry":       "industry.json",
    "sample_data":    "sample-data.json",
    "semantic_model": "semantic-model.json",
    "forecast":       "forecast-config.json",
    "planning":       "planning-config.json",
    "htap":           "htap-config.json",
    "reports":        "reports.json",
    "data_agent":     "data-agent.json",
    "web_enrichment": "web-enrichment.json",
}

# Schema files for validation
SCHEMA_FILES = {
    "industry":       "industry_schema.json",
    "sample_data":    "sample_data_schema.json",
    "semantic_model": "semantic_model_schema.json",
}


class ConfigValidationError(Exception):
    """Raised when a config file fails schema validation."""


class IndustryNotFoundError(Exception):
    """Raised when the requested industry folder does not exist."""


def list_industries() -> list[str]:
    """Return sorted list of available industry IDs."""
    if not INDUSTRIES_DIR.is_dir():
        return []
    return sorted(
        d.name for d in INDUSTRIES_DIR.iterdir()
        if d.is_dir() and (d / "industry.json").is_file()
    )


def _load_json(path: Path) -> dict:
    """Load and parse a JSON file."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _validate_against_schema(data: dict, schema_key: str) -> list[str]:
    """Validate data against a JSON schema. Returns list of error messages.

    Uses a lightweight built-in validator (no jsonschema dependency).
    Checks required fields, types, and patterns.
    """
    errors = []
    schema_path = SCHEMAS_DIR / SCHEMA_FILES.get(schema_key, "")
    if not schema_path.is_file():
        return errors  # No schema to validate against

    schema = _load_json(schema_path)
    _validate_object(data, schema, "", errors)
    return errors


def _validate_object(data, schema: dict, path: str, errors: list[str]):
    """Recursively validate an object against a JSON schema subset."""
    schema_type = schema.get("type")

    if schema_type == "object":
        if not isinstance(data, dict):
            errors.append(f"{path}: expected object, got {type(data).__name__}")
            return
        # Check required fields
        for req in schema.get("required", []):
            if req not in data:
                errors.append(f"{path}: missing required field '{req}'")
        # Validate properties
        props = schema.get("properties", {})
        for key, prop_schema in props.items():
            if key in data:
                _validate_object(data[key], prop_schema, f"{path}.{key}", errors)

    elif schema_type == "array":
        if not isinstance(data, list):
            errors.append(f"{path}: expected array, got {type(data).__name__}")
            return
        min_items = schema.get("minItems", 0)
        if len(data) < min_items:
            errors.append(f"{path}: expected at least {min_items} items, got {len(data)}")
        item_schema = schema.get("items")
        if item_schema:
            for i, item in enumerate(data):
                _validate_object(item, item_schema, f"{path}[{i}]", errors)

    elif schema_type == "string":
        if not isinstance(data, str):
            errors.append(f"{path}: expected string, got {type(data).__name__}")
        elif "pattern" in schema:
            import re
            if not re.search(schema["pattern"], data):
                errors.append(f"{path}: '{data}' does not match pattern '{schema['pattern']}'")
        if "enum" in schema and data not in schema["enum"]:
            errors.append(f"{path}: '{data}' not in allowed values {schema['enum']}")

    elif schema_type == "integer":
        if not isinstance(data, int) or isinstance(data, bool):
            errors.append(f"{path}: expected integer, got {type(data).__name__}")
        elif "minimum" in schema and data < schema["minimum"]:
            errors.append(f"{path}: {data} < minimum {schema['minimum']}")

    elif schema_type == "number":
        if not isinstance(data, (int, float)) or isinstance(data, bool):
            errors.append(f"{path}: expected number, got {type(data).__name__}")

    elif schema_type == "boolean":
        if not isinstance(data, bool):
            errors.append(f"{path}: expected boolean, got {type(data).__name__}")


def load_industry_config(industry_id: str) -> dict:
    """Load and validate the master industry.json for a given industry.

    Returns the parsed config dict.
    Raises IndustryNotFoundError if the industry folder doesn't exist.
    Raises ConfigValidationError if the config is invalid.
    """
    industry_dir = INDUSTRIES_DIR / industry_id
    if not industry_dir.is_dir():
        available = list_industries()
        raise IndustryNotFoundError(
            f"Industry '{industry_id}' not found. "
            f"Available: {available}"
        )

    config_path = industry_dir / CONFIG_FILES["industry"]
    if not config_path.is_file():
        raise IndustryNotFoundError(
            f"Missing industry.json in {industry_dir}"
        )

    data = _load_json(config_path)
    errors = _validate_against_schema(data, "industry")
    if errors:
        raise ConfigValidationError(
            f"industry.json validation failed:\n" +
            "\n".join(f"  - {e}" for e in errors)
        )
    return data


def load_config_file(industry_id: str, config_key: str) -> dict | None:
    """Load a specific config file for an industry.

    Args:
        industry_id: Industry folder name (e.g. 'contoso-energy')
        config_key: Key from CONFIG_FILES (e.g. 'sample_data', 'semantic_model')

    Returns:
        Parsed dict, or None if the file doesn't exist.
    Raises:
        ConfigValidationError if the file exists but fails validation.
    """
    if config_key not in CONFIG_FILES:
        raise ValueError(f"Unknown config key: {config_key}. Valid: {list(CONFIG_FILES.keys())}")

    industry_dir = INDUSTRIES_DIR / industry_id
    config_path = industry_dir / CONFIG_FILES[config_key]

    if not config_path.is_file():
        return None

    data = _load_json(config_path)

    # Validate if we have a schema for this config type
    if config_key in SCHEMA_FILES:
        errors = _validate_against_schema(data, config_key)
        if errors:
            raise ConfigValidationError(
                f"{CONFIG_FILES[config_key]} validation failed:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )

    return data


def load_all_configs(industry_id: str) -> dict[str, dict | None]:
    """Load all config files for an industry.

    Returns a dict keyed by config_key with parsed dicts (or None if missing).
    The 'industry' key is always required and validated.
    """
    configs = {}
    # Always load and validate industry.json first
    configs["industry"] = load_industry_config(industry_id)

    for key in CONFIG_FILES:
        if key == "industry":
            continue
        configs[key] = load_config_file(industry_id, key)

    return configs


def get_industry_dir(industry_id: str) -> Path:
    """Return the absolute path to an industry's config folder."""
    return INDUSTRIES_DIR / industry_id


def get_output_dir(industry_id: str, base_output: Path | None = None) -> Path:
    """Return the output directory for a generated demo."""
    base = base_output or (PROJECT_ROOT / "output")
    return base / industry_id
