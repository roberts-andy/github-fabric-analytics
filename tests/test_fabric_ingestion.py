from io import BytesIO
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
import urllib.error
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import refresh

INGEST = runpy.run_path(str(ROOT / "notebooks" / "00_ingest_github.py"))
MODEL = runpy.run_path(str(ROOT / "notebooks" / "04_refresh_semantic_model.py"))


class Response(BytesIO):
    def __init__(self, value, headers=None):
        super().__init__(json.dumps(value).encode())
        self.headers = headers or {}


def utilities(files):
    def put(path, content, overwrite):
        target = files.parent / path
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w" if overwrite else "x", encoding="utf-8") as output:
            output.write(content)
    return SimpleNamespace(fs=SimpleNamespace(put=put))


class IngestionTests(unittest.TestCase):
    def test_pagination_cannot_forward_credentials_to_another_host(self):
        client = INGEST["GitHubAPI"]("test-only-token")
        client.opener.open = Mock(side_effect=[
            Response([{"sha": "first"}], {
                "Link": '<https://api.github.com/next>; rel="next"',
            }),
            Response([{"sha": "second"}]),
        ])
        self.assertEqual(list(client.pages("https://api.github.com/first")),
                         [{"sha": "first"}, {"sha": "second"}])
        client.opener.open = Mock(return_value=Response([], {
            "Link": '<https://example.test/collect>; rel="next"',
        }))
        with self.assertRaisesRegex(ValueError, "must stay"):
            list(client.pages("https://api.github.com/first"))
        self.assertEqual(client.opener.open.call_count, 1)

    def test_empty_commits_are_allowed_but_other_failures_and_duplicates_are_not(self):
        client = INGEST["GitHubAPI"]()
        empty = urllib.error.HTTPError(
            "https://api.github.com/commits", 409, "Conflict", {},
            BytesIO(b'{"message":"Git Repository is empty."}'),
        )
        client.opener.open = Mock(side_effect=empty)
        self.assertEqual(list(client.pages("https://api.github.com/commits", True)), [])
        forbidden = urllib.error.HTTPError(
            "https://api.github.com/issues", 403, "Forbidden", {}, BytesIO(b"{}"),
        )
        client.opener.open = Mock(side_effect=forbidden)
        with self.assertRaisesRegex(RuntimeError, "HTTP 403"):
            list(client.pages("https://api.github.com/issues", True))
        client.opener.open = Mock(return_value=Response([], {
            "Link": '<https://api.github.com/first>; rel="next"',
        }))
        with self.assertRaisesRegex(ValueError, "Cyclic"):
            list(client.pages("https://api.github.com/first"))

    def test_batch_contract_retains_provenance_and_publishes_only_after_success(self):
        descriptors = [{"id": i, "full_name": f"example/repo{i}"} for i in (1, 2)]
        inventory = {
            "owner": "example", "repository_count": 2, "repositories": descriptors,
            "discovery_run_id": "scan",
        }
        details = [
            {**r, "private": False, "fork": False, "owner": {"login": "example"},
             "default_branch": "main"}
            for r in descriptors
        ]
        client = Mock(requests=7)
        client.page.return_value = ([{"sha": "a" * 40}], "")
        client.pages.side_effect = [
            details, [{"sha": "a" * 40}], [], [], [{"sha": "a" * 40}], [], [],
        ]
        with tempfile.TemporaryDirectory() as temporary:
            files = Path(temporary) / "Files"
            files.mkdir()
            fs = utilities(files)
            manifest = INGEST["ingest"](inventory, "example", client, fs, files)
            latest = files / "github" / "latest.json"
            self.assertEqual(json.loads(latest.read_text()), manifest)
            rows = [
                json.loads(line) for line in
                (files / "github" / manifest["batch_id"] / "records.jsonl").read_text().splitlines()
            ]
            commits = [r for r in rows if r["entity"] == "commits"]
            self.assertEqual({r["repository_id"] for r in commits}, {1, 2})
            self.assertEqual({r["record_id"] for r in commits}, {"a" * 40})
            self.assertEqual(manifest["record_count"], len(rows))
            self.assertEqual(sum(manifest["counts"].values()), len(rows))
            before = latest.read_text()
            for failure in (RuntimeError("page unavailable"), [{"sha": "duplicate"}] * 2):
                client.pages.side_effect = [details, failure] if isinstance(failure, Exception) else [
                    details, failure, [], [], [], [], [],
                ]
                with self.assertRaises((RuntimeError, ValueError)):
                    INGEST["ingest"](inventory, "example", client, fs, files)
                self.assertEqual(latest.read_text(), before)
            client.pages.side_effect = [[{**r, "fork": True} for r in details]]
            with self.assertRaisesRegex(ValueError, "changed eligibility"):
                INGEST["ingest"](inventory, "example", client, fs, files)
            self.assertEqual(latest.read_text(), before)

    def test_incremental_load_fetches_changed_issue_and_pr_details_not_full_history(self):
        repository = {
            "id": 1, "full_name": "example/repo", "private": False, "fork": False,
            "owner": {"login": "example"}, "default_branch": "main",
        }
        descriptor = {"id": 1, "full_name": "example/repo"}
        inventory = {
            "owner": "example", "repository_count": 1, "repositories": [descriptor],
            "discovery_run_id": "scan",
        }
        watermark = datetime.now(timezone.utc) - timedelta(hours=12)
        checkpoint = {"owner": "example", "repositories": {"1": {
            "default_branch": "main", "head_sha": "a" * 40, "watermark": watermark.isoformat(),
        }}}
        client = Mock(requests=4)
        client.page.side_effect = [
            ([{"sha": "a" * 40}], ""), ({"id": 8, "number": 8, "merged_at": "now"}, ""),
        ]
        client.pages.side_effect = [[repository], [
            {"id": 7, "number": 7, "state": "closed"},
            {"id": 8, "number": 8, "pull_request": {"url": "https://api.github.com/pr"}},
        ]]
        with tempfile.TemporaryDirectory() as temporary:
            files = Path(temporary) / "Files"
            files.mkdir()
            manifest = INGEST["ingest"](
                inventory, "example", client, utilities(files), files, checkpoint
            )
            self.assertFalse((files / "github" / "checkpoint.json").exists())
        self.assertEqual(manifest["counts"], {
            "repositories": 1, "commits": 0, "issues": 2, "pull_requests": 1,
        })
        self.assertEqual(manifest["repository_loads"]["1"]["commits"], "incremental")
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(
            client.pages.call_args_list[1].args[0]
        ).query)
        self.assertEqual(query["since"], [
            (watermark - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ])
        self.assertEqual(client.pages.call_count, 2)
        self.assertTrue(client.page.call_args_list[1].args[0].endswith("/pulls/8"))

    def test_new_repository_bootstraps_while_unchanged_repository_is_incremental(self):
        descriptors = [{"id": i, "full_name": f"example/repo{i}"} for i in (1, 2)]
        details = [
            {**r, "private": False, "fork": False, "owner": {"login": "example"},
             "default_branch": "main"} for r in descriptors
        ]
        inventory = {
            "owner": "example", "repository_count": 2, "repositories": descriptors,
            "discovery_run_id": "scan",
        }
        checkpoint = {"owner": "example", "repositories": {"1": {
            "default_branch": "main", "head_sha": "a" * 40,
            "watermark": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
        }}}
        client = Mock(requests=7)
        client.page.return_value = ([{"sha": "a" * 40}], "")
        client.pages.side_effect = [details, [], [{"sha": "a" * 40}], [], []]
        with tempfile.TemporaryDirectory() as temporary:
            files = Path(temporary) / "Files"
            files.mkdir()
            manifest = INGEST["ingest"](
                inventory, "example", client, utilities(files), files, checkpoint
            )
        self.assertEqual(manifest["repository_loads"]["1"]["issues"], "incremental")
        self.assertTrue(all(mode == "full" for mode in manifest["repository_loads"]["2"].values()))
        self.assertEqual(manifest["counts"]["commits"], 1)

    def test_commit_comparison_paginates_and_keeps_backdated_new_commits(self):
        old, middle, head = ("a" * 40, "b" * 40, "c" * 40)
        base = "https://api.github.com/repos/example/repo"
        compare = {
            "status": "ahead", "base_commit": {"sha": old},
            "merge_base_commit": {"sha": old}, "total_commits": 2,
        }
        client = Mock()
        client.page.side_effect = [
            ([{"sha": head}], ""),
            ({**compare, "commits": [{"sha": middle, "commit": {
                "author": {"date": "2001-01-01T00:00:00Z"},
            }}]}, '<https://api.github.com/next>; rel="next"'),
            ({**compare, "commits": [{"sha": head}]}, ""),
        ]
        objects, _, mode, actual_head = INGEST["changed_commits"](
            client, base, "main", {"head_sha": old}, False
        )
        self.assertEqual([c["sha"] for c in objects], [middle, head])
        self.assertEqual((mode, actual_head), ("incremental", head))
        client.pages.assert_not_called()

    def test_rewritten_history_triggers_full_commit_reconciliation(self):
        client = Mock()
        client.page.side_effect = [
            ([{"sha": "b" * 40}], ""), ({"status": "diverged"}, ""),
        ]
        client.pages.return_value = [{"sha": "b" * 40}]
        objects, _, mode, _ = INGEST["changed_commits"](
            client, "https://api.github.com/repos/example/repo",
            "main", {"head_sha": "a" * 40}, False,
        )
        self.assertEqual(mode, "full")
        self.assertEqual(list(objects), [{"sha": "b" * 40}])

    def test_authentication_never_silently_falls_back_to_anonymous(self):
        utility = Mock()
        with self.assertRaisesRegex(ValueError, "valid Azure Key Vault"):
            INGEST["github_token"]("key_vault", "", "", utility)
        with self.assertRaisesRegex(ValueError, "Select key_vault"):
            INGEST["github_token"]("anonymous", "https://example.vault.azure.net", "github", utility)
        utility.credentials.getSecret.side_effect = RuntimeError("vault unavailable")
        with self.assertRaisesRegex(RuntimeError, "vault unavailable"):
            INGEST["github_token"](
                "key_vault", "https://example.vault.azure.net", "github", utility
            )

    def test_pipeline_chains_all_stages_and_refuses_implicit_retries(self):
        state = {
            "workspace_id": "workspace",
            "notebooks": {name: f"id-{name}" for name in refresh.NOTEBOOKS},
        }
        properties = refresh.pipeline_content(state)["properties"]
        self.assertEqual(properties["concurrency"], 1)
        activities = properties["activities"]
        self.assertEqual(activities[0]["dependsOn"], [])
        for previous, activity in zip(activities, activities[1:]):
            self.assertEqual(activity["dependsOn"], [{
                "activity": previous["name"], "dependencyConditions": ["Succeeded"],
            }])
        self.assertTrue(all(a["policy"]["retry"] == 0 for a in activities))

    def test_pipeline_run_resumes_an_active_job_without_resubmitting(self):
        state = {
            "workspace_id": "workspace", "refresh_pipeline_id": "pipeline",
            "notebooks": {},
        }
        api = Mock()
        api.call.side_effect = [
            {"capacityId": "capacity"}, {"status": "Completed"},
        ]
        api.list.side_effect = [
            [{"id": "capacity", "state": "Active"}],
            [{"id": "existing-job", "status": "InProgress"}],
        ]
        api.request.return_value = (200, {}, {"batch_id": "new-cloud-batch"})
        with patch.object(refresh.project, "load", return_value=state), \
                patch.object(refresh.project, "save"), \
                patch.object(refresh.project, "files_url", return_value="https://example.test/Files"), \
                patch.object(refresh, "Fabric", return_value=api):
            refresh.run(SimpleNamespace())
        self.assertEqual(state["refresh_pipeline_job_id"], "existing-job")
        self.assertEqual(state["latest_batch"]["batch_id"], "new-cloud-batch")
        self.assertEqual([c.args[0] for c in api.request.call_args_list], ["GET"])

    def test_pipeline_submission_refuses_an_individual_notebook_already_running(self):
        api = Mock()
        api.call.return_value = {"capacityId": "capacity"}
        api.list.side_effect = [
            [{"id": "capacity", "state": "Active"}], [],
            [{"status": "InProgress"}],
        ]
        state = {
            "workspace_id": "workspace", "refresh_pipeline_id": "pipeline",
            "notebooks": {name: name for name in refresh.NOTEBOOKS},
        }
        with patch.object(refresh.project, "load", return_value=state), \
                patch.object(refresh, "Fabric", return_value=api):
            with self.assertRaisesRegex(RuntimeError, "already has an active job"):
                refresh.run(SimpleNamespace())
        api.request.assert_not_called()


class RefreshModelTests(unittest.TestCase):
    def test_refresh_waits_for_completion_and_persists_request_identity(self):
        utility = Mock()
        utility.runtime.context = {"currentWorkspaceId": "11111111-1111-1111-1111-111111111111"}
        utility.fs.head.side_effect = lambda path, *args: json.dumps({
            "batch_id": "batch", "owner": "example", "repositories": [{"id": 1}],
            "checkpoints": {"1": {"head_sha": "a" * 40}},
        }) if path.endswith("latest.json") else '{"batch_id":"batch"}'
        model = "22222222-2222-2222-2222-222222222222"
        refresh_id = "33333333-3333-3333-3333-333333333333"
        location = (
            "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/"
            f"v1.0/myorg/datasets/{model}/refreshes/{refresh_id}"
        )
        with patch("urllib.request.urlopen", side_effect=[
            Response({"value": []}), Response(None, {"Location": location}),
            Response({"status": "InProgress"}), Response({"status": "Completed"}),
        ]) as request, patch("time.sleep"):
            result = MODEL["refresh_model"](model, utility)
        self.assertEqual(result["status"], "Completed")
        self.assertEqual(result["refresh_id"], refresh_id)
        self.assertEqual([c.args[0].method for c in request.call_args_list],
                         ["GET", "POST", "GET", "GET"])
        self.assertEqual(utility.fs.put.call_count, 3)
        self.assertEqual(utility.fs.put.call_args.args[0], "Files/github/checkpoint.json")
        self.assertTrue(request.call_args.args[0].full_url.startswith("https://api.powerbi.com/"))

    def test_active_refresh_and_post_failure_are_not_resubmitted(self):
        utility = Mock()
        utility.runtime.context = {"currentWorkspaceId": "11111111-1111-1111-1111-111111111111"}
        utility.fs.head.return_value = '{"batch_id":"batch","owner":"example","repositories":[]}'
        model = "22222222-2222-2222-2222-222222222222"
        with patch("urllib.request.urlopen", return_value=Response({
            "value": [{"status": "Unknown"}],
        })) as request:
            with self.assertRaisesRegex(RuntimeError, "already active"):
                MODEL["refresh_model"](model, utility)
            self.assertEqual(request.call_count, 1)
        error = urllib.error.HTTPError("url", 503, "Unavailable", {}, BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=[
            Response({"value": []}), error,
        ]) as request:
            with self.assertRaisesRegex(RuntimeError, "not resubmitted"):
                MODEL["refresh_model"](model, utility)
            self.assertEqual(request.call_count, 2)
        self.assertFalse(utility.fs.put.called)

    def test_failed_model_refresh_does_not_advance_the_extraction_checkpoint(self):
        utility = Mock()
        utility.runtime.context = {"currentWorkspaceId": "11111111-1111-1111-1111-111111111111"}
        utility.fs.head.return_value = json.dumps({
            "batch_id": "batch", "owner": "example", "repositories": [{"id": 1}],
            "checkpoints": {"1": {"head_sha": "a" * 40}},
        })
        with patch("urllib.request.urlopen", side_effect=[
            Response({"value": []}),
            Response(None, {"x-ms-request-id": "33333333-3333-3333-3333-333333333333"}),
            Response({"status": "Failed"}),
        ]), patch("time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "ended with Failed"):
                MODEL["refresh_model"]("22222222-2222-2222-2222-222222222222", utility)
        self.assertFalse(any(
            c.args[0].endswith("checkpoint.json") for c in utility.fs.put.call_args_list
        ))


if __name__ == "__main__":
    unittest.main()
