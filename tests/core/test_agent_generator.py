"""Tests for agent_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.agent_generator import generate_data_agent


class TestAgentGenerator(unittest.TestCase):
    """Test Data Agent artifact generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.agent_config = load_config_file("contoso-energy", "data_agent")

    def test_generates_two_files(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="agent_"))
        result = generate_data_agent(self.industry_config, self.agent_config, tmpdir)
        self.assertEqual(len(result), 2)
        names = {p.name for p in result}
        self.assertIn("agent-config.json", names)
        self.assertIn("README.md", names)

    def test_config_json_valid(self):
        import json
        tmpdir = Path(tempfile.mkdtemp(prefix="agent_"))
        result = generate_data_agent(self.industry_config, self.agent_config, tmpdir)
        config_path = [p for p in result if p.name == "agent-config.json"][0]
        data = json.loads(config_path.read_text())
        self.assertIn("name", data)
        self.assertIn("semanticModel", data)
        self.assertIn("exampleQuestions", data)

    def test_returns_empty_when_no_config(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="agent_"))
        result = generate_data_agent(self.industry_config, None, tmpdir)
        self.assertEqual(result, [])

    def test_all_industries_have_agent_config(self):
        for ind in ["contoso-energy", "horizon-books", "northwind-hrfinance",
                     "fabrikam-manufacturing"]:
            with self.subTest(industry=ind):
                cfg = load_config_file(ind, "data_agent")
                self.assertIsNotNone(cfg, f"{ind} missing data-agent.json")


if __name__ == "__main__":
    unittest.main()
