import base64
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import report


class ReportTests(unittest.TestCase):
    def test_validation_rejects_entry_point_version_used_as_content_version(self):
        files = report.report_files("model-id")
        version = json.loads(files["definition/version.json"])
        version["version"] = "4.0.0"
        files["definition/version.json"] = json.dumps(version).encode()
        with patch.object(report.urllib.request, "urlopen") as retrieve:
            with self.assertRaisesRegex(ValueError, "entry-point version"):
                report.validate(files)
            retrieve.assert_not_called()

    def test_verification_exports_service_binding_for_desktop(self):
        files = report.report_files("model-id")
        binding = json.loads(files["definition.pbir"])
        binding["datasetReference"]["byConnection"]["connectionString"] = (
            "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;"
            "initial catalog=Model;integrated security=ClaimsToken;semanticmodelid=model-id"
        )
        returned = dict(files)
        returned["definition.pbir"] = json.dumps(binding).encode()
        parts = [
            {"path": path, "payload": base64.b64encode(content).decode()}
            for path, content in returned.items()
        ]
        state = {"workspace_id": "workspace-id", "report_id": "report-id",
                 "semantic_model_id": "model-id"}
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(report.project, "ROOT", Path(directory)), \
                    patch.object(report.project, "save"), patch.object(report, "Fabric") as fabric:
                fabric.return_value.call.return_value = {"definition": {"parts": parts}}
                fabric.return_value.request.return_value = (
                    200, {}, {"datasetId": "model-id", "webUrl": "https://app.powerbi.com/report"}
                )
                report.verify_report(state, files)
                exported = Path(directory) / ".local" / "report" / "definition-liveConnect.pbir"
                self.assertEqual(json.loads(exported.read_bytes()), binding)
                self.assertEqual(
                    json.loads(files["definition.pbir"])["datasetReference"]["byConnection"]
                    ["connectionString"], "semanticmodelid=model-id",
                )

    def test_service_connection_and_theme_normalization_preserves_semantics(self):
        files = report.report_files("expected-model-id")
        returned = {
            path: {"payload": base64.b64encode(content).decode()}
            for path, content in files.items()
        }
        binding = json.loads(files["definition.pbir"])
        binding["datasetReference"]["byConnection"]["connectionString"] = (
            "Data Source=powerbi://example;initial catalog=Example;"
            "semanticmodelid=expected-model-id"
        )
        returned["definition.pbir"]["payload"] = base64.b64encode(json.dumps(binding).encode())
        root = json.loads(files["definition/report.json"])
        registered = next(p for p in root["resourcePackages"] if p["type"] == "RegisteredResources")
        registered["items"][0]["path"] = "GitHubTeamActivity"
        returned["definition/report.json"]["payload"] = base64.b64encode(json.dumps(root).encode())
        report.verify_parts(files, returned, "expected-model-id")
        binding["datasetReference"]["byConnection"]["connectionString"] = "semanticmodelid=wrong"
        returned["definition.pbir"]["payload"] = base64.b64encode(json.dumps(binding).encode())
        with self.assertRaisesRegex(RuntimeError, "wrong semantic model"):
            report.verify_parts(files, returned, "expected-model-id")

    def test_readback_rejects_changed_visual_queries(self):
        files = report.report_files("model-id")
        returned = {
            path: {"payload": base64.b64encode(content).decode()}
            for path, content in files.items()
        }
        path = next(p for p in files if p.endswith("/visual.json")
                    and "query" in json.loads(files[p])["visual"])
        visual = json.loads(files[path])
        visual["visual"]["query"]["queryState"] = {}
        returned[path]["payload"] = base64.b64encode(json.dumps(visual).encode())
        with self.assertRaisesRegex(RuntimeError, "differs from source"):
            report.verify_parts(files, returned, "model-id")

    def test_theme_references_resolve_to_packaged_resources(self):
        files = report.report_files("model-id")
        root = json.loads(files["definition/report.json"])
        self.assertIn("baseTheme", root["themeCollection"])
        for theme in root["themeCollection"].values():
            package = next(p for p in root["resourcePackages"] if p["name"] == theme["type"])
            item = next(i for i in package["items"] if i["name"] == theme["name"])
            if package["type"] == "RegisteredResources":
                self.assertEqual(item["name"], Path(item["path"]).name)
                self.assertIn(f"StaticResources/{package['name']}/{item['path']}", files)
            else:
                self.assertEqual(item["type"], "BaseTheme")
                self.assertEqual(item["path"], f"BaseThemes/{theme['name']}.json")

    def test_visual_bindings_exist_in_the_versioned_model(self):
        root = Path(__file__).resolve().parents[1]
        model = json.loads((root / "semantic_model" / "model.bim").read_text())["model"]
        fields = {
            (kind, table["name"], item["name"])
            for table in model["tables"]
            for kind, collection in [("Column", "columns"), ("Measure", "measures")]
            for item in table.get(collection, [])
        }

        def check(value):
            if isinstance(value, dict):
                for kind in ("Column", "Measure"):
                    expression = value.get(kind, {})
                    source = expression.get("Expression", {}).get("SourceRef", {})
                    if "Entity" in source:
                        self.assertIn((kind, source["Entity"], expression["Property"]), fields)
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)

        for content in report.report_files("model-id").values():
            check(json.loads(content))

    def test_all_visuals_fit_and_have_unique_names_per_page(self):
        pages = {}
        for path, content in report.report_files("model-id").items():
            if path.endswith("/visual.json"):
                item = json.loads(content)
                names = pages.setdefault(path.split("/")[2], set())
                self.assertNotIn(item["name"], names)
                names.add(item["name"])
                box = item["position"]
                self.assertGreaterEqual(box["x"], 0)
                self.assertGreaterEqual(box["y"], 0)
                self.assertLessEqual(box["x"] + box["width"], report.WIDTH)
                self.assertLessEqual(box["y"] + box["height"], report.HEIGHT)
        self.assertEqual(set(pages), {"overview", "workflow", "contributions"})

    def test_parameterized_binding_and_cohort_safeguards(self):
        files = report.report_files("expected-model-id")
        connection = json.loads(files["definition.pbir"])["datasetReference"]["byConnection"]
        self.assertEqual(connection["connectionString"], "semanticmodelid=expected-model-id")
        workflow = [
            json.loads(content) for path, content in files.items()
            if "/workflow/visuals/" in path
        ]
        detail = next(v for v in workflow if "filterConfig" in v)
        self.assertEqual(detail["filterConfig"]["filters"][0]["name"], "open-prs-only")
        titles = json.dumps(workflow)
        self.assertIn("creation cohorts", titles)
        self.assertNotIn("merges per month", titles)
        for page in ("overview", "workflow", "contributions"):
            slicers = [
                json.loads(content)["visual"] for path, content in files.items()
                if f"/{page}/visuals/" in path
                and json.loads(content)["visual"]["visualType"] == "slicer"
            ]
            self.assertEqual({v["syncGroup"]["groupName"] for v in slicers},
                             {"UTC dates", "Repository", "Language", "GitHub account"})
