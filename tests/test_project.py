import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from fabric_api import Fabric
import project


class ProjectTests(unittest.TestCase):
    def test_paginated_discovery_follows_continuation_uri(self):
        api = Fabric()
        with patch.object(api, "call", side_effect=[
            {"value": [{"id": "a"}], "continuationUri": "/next"},
            {"value": [{"id": "b"}]},
        ]) as call:
            self.assertEqual(api.list("/items"), [{"id": "a"}, {"id": "b"}])
            self.assertEqual(call.call_args_list[1].args, ("GET", "/next"))

    def test_ambiguous_names_fail_instead_of_selecting_arbitrarily(self):
        with self.assertRaisesRegex(RuntimeError, "Ambiguous"):
            project.exact([{"displayName": "demo"}, {"displayName": "demo"}], "demo")

    def test_failed_lro_surfaces_error_without_resubmission(self):
        api = Fabric()
        with patch.object(api, "request", side_effect=[
            (202, {"Location": "https://example.test/operation"}, None),
            (200, {}, {"status": "Failed", "error": {"message": "invalid definition"}}),
        ]) as request, patch("fabric_api.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "invalid definition"):
                api.call("POST", "/items", {})
            self.assertEqual(sum(c.args[0] == "POST" for c in request.call_args_list), 1)

    def test_notebook_binding_and_empty_execution_fields(self):
        import nbformat
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".local").mkdir()
            source = root / "layer.py"
            source.write_text("first = 1\nsecond = 2\n")
            with patch.object(project, "ROOT", root):
                result = project.notebook_definition(source, {
                    "workspace_id": "workspace", "lakehouse_id": "lakehouse",
                })
            notebook = nbformat.read(root / ".local" / "layer.ipynb", as_version=4)
            nbformat.validate(notebook)
            self.assertEqual(result["format"], "ipynb")
            cell = notebook.cells[1]
            self.assertEqual(cell.outputs, [])
            self.assertIsNone(cell.execution_count)
            self.assertEqual(cell.source, "first = 1\nsecond = 2\n")
            binding = notebook.metadata.dependencies.lakehouse
            self.assertEqual(binding.default_lakehouse, "lakehouse")
            self.assertEqual(binding.default_lakehouse_workspace_id, "workspace")

    def test_saved_progress_preserves_independent_state_fields(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "deployment.json"
            path.write_text(json.dumps({"schemas_created": True}))
            with patch.object(project, "STATE", path):
                project.save({"latest_batch": {"batch_id": "batch"}})
            result = json.loads(path.read_text())
            self.assertTrue(result["schemas_created"])
            self.assertEqual(result["latest_batch"]["batch_id"], "batch")

    def test_in_progress_job_is_monitored_not_resubmitted(self):
        state = {
            "workspace_id": "workspace", "lakehouse_id": "lakehouse",
            "notebooks": {"01_bronze": "notebook"},
        }
        api = Mock()
        api.list.return_value = [{"id": "existing-job", "status": "InProgress"}]
        api.call.return_value = {"id": "existing-job", "status": "Completed"}
        with patch.object(project, "load", return_value=state), \
                patch.object(project, "save"), \
                patch.object(project, "Fabric", return_value=api):
            project.run(SimpleNamespace(notebook=None))
        api.request.assert_not_called()
        self.assertEqual(state["jobs"]["01_bronze"], "existing-job")


if __name__ == "__main__":
    unittest.main()
