import json
import unittest
from urllib.parse import parse_qs, urlsplit

import prompt_enrichment as prompt


class PromptEnrichmentTests(unittest.TestCase):
    def config(self, **overrides):
        return prompt.normalize_config({
            "api_url": "https://example.com/v1/ai-overview?prompt={prompt}&mode=json",
            "prompt_template": 'Find {{company}} in {{city}}, {{custom_1}}. Return {"name":""}.',
            "timeout_seconds": 120,
            **overrides,
        }, {"name": "full_name"}, {"company", "city", "custom_1", "full_name"})

    def test_encoding_fallback_empty_tags_and_literal_json(self):
        contact = {"company": " ", "business_name": "A&B / 50% + Sons", "city": None}
        url, text = prompt.request_url(self.config(), contact)
        self.assertEqual(text, 'Find A&B / 50% + Sons in , . Return {"name":""}.')
        self.assertEqual(parse_qs(urlsplit(url).query)["prompt"], [text])
        self.assertEqual(parse_qs(urlsplit(url).query)["mode"], ["json"])
        self.assertEqual(prompt.render_prompt("{{company}}", {"company": "Real", "business_name": "Fallback"}), "Real")

    def test_rate_defaults_and_concurrency(self):
        config = self.config()
        self.assertEqual(config["requests_per_minute"], 30)
        self.assertEqual(prompt.automatic_concurrency(config), 60)
        self.assertEqual(prompt.automatic_concurrency(self.config(requests_per_minute=600)), 100)
        self.assertEqual(config["endpoint_key"], self.config(api_url="https://example.com/v1/ai-overview?mode=json&prompt={prompt}")["endpoint_key"])
        for value in (0, -1, 601, "invalid"):
            with self.assertRaises(ValueError):
                self.config(requests_per_minute=value)

    def test_validation_requires_prompt_query_and_known_tags(self):
        for config in ({"api_url": "file:///tmp/data?prompt={prompt}"},
                       {"api_url": "https://example.com?prompt=hello"},
                       {"prompt_template": "Find {{typo}}"}, {"prompt_template": ""}):
            with self.assertRaises(ValueError):
                self.config(**config)

    def test_extracts_json_object_from_noisy_google_answer(self):
        expected = {"name": "Michael D. Piraino", "phone": "630-932-1810", "email": "mike@example.com"}
        payload = {"ok": True, "api": {"name": "AI_Overview"}, "result": {
            "ok": True, "ai_answer": "json" + json.dumps(expected) + " Use code with caution. CopiedFailed to copy"}}
        self.assertEqual(prompt.extract_object(payload), expected)

    def test_nested_keys_braces_in_strings_and_nulls(self):
        answer = prompt.extract_object({"result": {"ai_answer": '```json\n{"owner":{"name":"A {B} \\\"C\\\""}, "email": null}\n```'}})
        self.assertEqual(prompt.mapped_values(answer, {"owner.name": "full_name", "email": "email"}), {"owner.name": 'A {B} "C"'})
        self.assertEqual(prompt.mapped_values({"email": "Unknown", "name": []}, {"email": "email", "name": "full_name"}), {})

    def test_provider_failures_and_invalid_text_are_not_success(self):
        for payload in ({"ok": False, "error": "No quota"},
                        {"ok": True, "result": {"ok": False, "error": "Search failed"}},
                        {"result": {"ai_answer": "No data"}},
                        {"result": {"ai_answer": 'json{"name": invalid}'}}):
            with self.assertRaises(ValueError):
                prompt.extract_object(payload)


if __name__ == "__main__":
    unittest.main()
