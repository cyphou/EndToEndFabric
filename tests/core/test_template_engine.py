"""Tests for template_engine module."""

import unittest

from core.template_engine import render_template, build_context


class TestRenderTemplate(unittest.TestCase):
    """Test template rendering."""

    def test_simple_placeholder(self):
        result = render_template("Hello {{name}}!", {"name": "World"})
        self.assertEqual(result, "Hello World!")

    def test_nested_key(self):
        ctx = {"industry": {"name": "Contoso"}}
        result = render_template("Company: {{industry.name}}", ctx)
        self.assertEqual(result, "Company: Contoso")

    def test_default_value(self):
        result = render_template("{{missing|fallback}}", {})
        self.assertEqual(result, "fallback")

    def test_missing_key_marker(self):
        result = render_template("{{unknown}}", {})
        self.assertIn("MISSING:unknown", result)

    def test_if_block_truthy(self):
        template = "start{{#IF flag}} visible{{/IF flag}} end"
        result = render_template(template, {"flag": True})
        self.assertEqual(result, "start visible end")

    def test_if_block_falsy(self):
        template = "start{{#IF flag}} hidden{{/IF flag}} end"
        result = render_template(template, {"flag": False})
        self.assertEqual(result, "start end")

    def test_if_block_missing(self):
        template = "start{{#IF flag}} hidden{{/IF flag}} end"
        result = render_template(template, {})
        self.assertEqual(result, "start end")

    def test_each_block(self):
        template = "{{#EACH items}}[{{item}}]{{/EACH items}}"
        result = render_template(template, {"items": ["a", "b", "c"]})
        self.assertEqual(result, "[a][b][c]")

    def test_each_with_index(self):
        template = "{{#EACH items}}{{index1}}.{{item}} {{/EACH items}}"
        result = render_template(template, {"items": ["x", "y"]})
        self.assertEqual(result, "1.x 2.y ")

    def test_each_non_list(self):
        template = "{{#EACH items}}X{{/EACH items}}"
        result = render_template(template, {"items": "not-a-list"})
        self.assertEqual(result, "")

    def test_integer_value(self):
        result = render_template("Count: {{count}}", {"count": 42})
        self.assertEqual(result, "Count: 42")

    def test_nested_if_with_placeholder(self):
        template = "{{#IF show}}{{name}} is here{{/IF show}}"
        result = render_template(template, {"show": True, "name": "Alice"})
        self.assertEqual(result, "Alice is here")


class TestBuildContext(unittest.TestCase):
    """Test context building from configs."""

    def test_flattens_top_level(self):
        configs = {
            "industry": {"industry": {"id": "test", "name": "Test"}, "fabricArtifacts": {"x": 1}},
            "sample_data": None,
        }
        ctx = build_context(configs)
        self.assertIn("industry", ctx)
        self.assertIn("fabricArtifacts", ctx)
        self.assertEqual(ctx["fabricArtifacts"]["x"], 1)

    def test_none_configs_skipped(self):
        configs = {"industry": {"a": 1}, "htap": None}
        ctx = build_context(configs)
        self.assertIn("industry", ctx)
        self.assertNotIn("htap", ctx)


if __name__ == "__main__":
    unittest.main()
