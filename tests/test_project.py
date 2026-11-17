import json
from pathlib import Path
import subprocess
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
            self.assertEqual(notebook.cells[1].metadata.tags, ["parameters"])
            cell = notebook.cells[2]
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

    def test_notebook_export_preserves_markdown_and_shared_code_state(self):
        import nbformat
        source_text = (
            "# %% [markdown]\n"
            "# ## Prepare the input\n"
            "#\n"
            "# Keep the captured value available to later cells.\n"
            "# %%\n"
            "first = 40\n"
            "# %% [markdown]\n"
            "# ## Finish the calculation\n"
            "# %%\n"
            "# This code comment must not become markdown.\n"
            "result = first + 2\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".local").mkdir()
            source = root / "layer.py"
            source.write_text(source_text)
            with patch.object(project, "ROOT", root):
                project.notebook_definition(source, {
                    "workspace_id": "workspace", "lakehouse_id": "lakehouse",
                })
            notebook = nbformat.read(root / ".local" / "layer.ipynb", as_version=4)
            self.assertEqual(
                [cell.cell_type for cell in notebook.cells],
                ["markdown", "code", "markdown", "code", "markdown", "code"],
            )
            self.assertIn("## Prepare the input", notebook.cells[2].source)
            self.assertIn("\n\nKeep the captured value", notebook.cells[2].source)
            self.assertIn("# This code comment", notebook.cells[-1].source)
            namespace = {}
            for cell in notebook.cells:
                if cell.cell_type == "code":
                    exec(cell.source, namespace)
            self.assertEqual(namespace["result"], 42)

    def test_notebook_markdown_cannot_hide_executable_python(self):
        with self.assertRaisesRegex(ValueError, "only comment lines"):
            project.notebook_cells("# %% [markdown]\n# Description\nresult = 1\n")

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
            project.run(SimpleNamespace(notebook=["01_bronze"]))
        api.request.assert_not_called()
        self.assertEqual(state["jobs"]["01_bronze"], "existing-job")

    def test_inventory_eligibility_excludes_forks_private_and_other_owners(self):
        repository = {
            "private": False, "fork": False, "owner": {"login": "Example"},
            "archived": True,
        }
        self.assertTrue(project.eligible_repository(repository, "example"))
        for overrides in [
            {"private": True}, {"fork": True}, {"owner": {"login": "someone-else"}},
        ]:
            with self.subTest(overrides=overrides):
                self.assertFalse(project.eligible_repository(
                    {**repository, **overrides}, "example"
                ))

    def test_empty_repository_does_not_hide_other_github_errors(self):
        body = {"message": "Git Repository is empty.", "status": "409"}
        for response in (body, [body]):
            empty = subprocess.CalledProcessError(1, "gh", output=json.dumps(response))
            with self.subTest(response=response), \
                    patch.object(project, "command_json", side_effect=empty):
                self.assertEqual(project.github_pages("/commits", True), [])
        forbidden = subprocess.CalledProcessError(
            1, "gh", output=json.dumps({"message": "Forbidden", "status": "403"})
        )
        with patch.object(project, "command_json", side_effect=forbidden):
            with self.assertRaises(subprocess.CalledProcessError):
                project.github_pages("/commits", True)

    def test_github_pages_flattens_every_page(self):
        pages = [[{"sha": "first"}], [{"sha": "second"}]]
        with patch.object(project, "command_json", return_value=pages):
            self.assertEqual(
                project.github_pages("/commits"),
                [{"sha": "first"}, {"sha": "second"}],
            )

    def test_staging_uses_inventory_and_preserves_shared_commits_per_repository(self):
        descriptors = [
            {"id": 1, "full_name": "example/one"},
            {"id": 2, "full_name": "example/two"},
        ]
        inventory = {
            "owner": "example", "repository_count": 2, "repositories": descriptors,
            "discovery_run_id": "scan",
        }
        details = [
            {**value, "private": False, "fork": False, "owner": {"login": "example"}}
            for value in descriptors
        ]
        api = Mock()
        api.request.side_effect = lambda method, *args, **kwargs: (
            (200, {}, inventory) if method == "GET" else (201, {}, None)
        )
        shared = {"sha": "a" * 40}
        with patch.object(project, "load", return_value={}), \
                patch.object(project, "save"), \
                patch.object(project, "CONFIG", {"owner": "example"}), \
                patch.object(project, "Fabric", return_value=api), \
                patch.object(project, "files_url", return_value="https://example.test/Files"), \
                patch.object(project, "command_json", side_effect=details), \
                patch.object(project, "github_pages", side_effect=[
                    [shared], [], [], [shared], [], [],
                ]):
            project.stage(SimpleNamespace())
        payload = next(
            call.kwargs["raw"] for call in api.request.call_args_list
            if call.args[0] == "PATCH" and "/records.jsonl?action=append" in call.args[1]
        )
        commits = [
            record for line in payload.decode().splitlines()
            if (record := json.loads(line))["entity"] == "commits"
        ]
        self.assertEqual({record["repository_id"] for record in commits}, {1, 2})
        self.assertEqual({record["record_id"] for record in commits}, {"a" * 40})


if __name__ == "__main__":
    unittest.main()
