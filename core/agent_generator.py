"""Data Agent generator — produces Fabric AI Agent configuration artifacts.

Generates from data-agent.json:
  DataAgent/agent-config.json — Agent definition for Fabric deployment
  DataAgent/README.md         — Agent documentation
"""

import json
from pathlib import Path


def generate_data_agent(industry_config: dict,
                        agent_config: dict | None,
                        output_dir: Path) -> list[Path]:
    """Generate Data Agent artifacts for the industry demo.

    Args:
        industry_config: Parsed industry.json content.
        agent_config: Parsed data-agent.json content, or None if missing.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    if not agent_config:
        return []

    agent = agent_config.get("dataAgent", {})
    if not agent:
        return []

    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo")

    agent_dir = output_dir / "DataAgent"
    agent_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # ── Agent Config JSON ──
    config = {
        "name": agent.get("name", f"{company.replace(' ', '')}-Agent"),
        "displayName": agent.get("displayName", f"{company} AI Assistant"),
        "description": agent.get("description", ""),
        "semanticModel": agent.get("semanticModel", ""),
        "systemPrompt": agent.get("systemPrompt", ""),
        "exampleQuestions": agent.get("exampleQuestions", []),
        "capabilities": agent.get("capabilities", {
            "requiresF64": True,
            "supportsFollowUp": True,
            "supportsVisualization": True,
        }),
    }
    config_path = agent_dir / "agent-config.json"
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    created.append(config_path)

    # ── Agent README ──
    questions = "\n".join(f"- {q}" for q in config["exampleQuestions"])
    readme = f"""# {config['displayName']}

{config['description']}

## Semantic Model

Connected to: **{config['semanticModel']}**

## Example Questions

{questions}

## Capabilities

| Feature | Enabled |
|---------|---------|
| F64 Semantic Link | {config['capabilities'].get('requiresF64', True)} |
| Follow-up Questions | {config['capabilities'].get('supportsFollowUp', True)} |
| Visualization | {config['capabilities'].get('supportsVisualization', True)} |

## Deployment

This agent is deployed as part of the {company} Fabric demo.
Use the `Deploy-Full.ps1` script to deploy all artifacts including this agent.
"""
    readme_path = agent_dir / "README.md"
    readme_path.write_text(readme, encoding="utf-8")
    created.append(readme_path)

    return created
