"""Provision and refresh the GitHub lakehouse. Run from any working directory."""

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import urllib.parse
import uuid

from fabric_api import Fabric, command_json

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / "config.json").read_text())
STATE = ROOT / ".local" / "deployment.json"


def save(state):
    STATE.parent.mkdir(exist_ok=True)
    existing = json.loads(STATE.read_text()) if STATE.exists() else {}
    STATE.write_text(json.dumps({**existing, **state}, indent=2) + "\n")


def load():
    return json.loads(STATE.read_text())


def exact(items, name):
    matches = [item for item in items if item["displayName"] == name]
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous duplicate display name: {name}")
    return matches[0] if matches else None


def provision(args):
    api = Fabric()
    capacities = api.list("/capacities")
    capacity = next(
        (c for c in capacities if c["id"] == args.capacity_id and c["state"] == "Active"),
        None,
    )
    if not capacity:
        raise RuntimeError("Select an active Fabric capacity from GET /capacities")
    workspace = exact(api.list("/workspaces"), CONFIG["workspace"])
    if not workspace:
        workspace = api.call("POST", "/workspaces", {
            "displayName": CONFIG["workspace"],
            "description": "GitHub repository analytics: Bronze, Silver, Gold and Power BI",
            "capacityId": capacity["id"],
        })
    workspace_id = workspace["id"]
    # Persist immediately so interrupted provisioning is recoverable.
    state = load() if STATE.exists() else {}
    state["workspace_id"] = workspace_id
    save(state)
    workspace = api.call("GET", f"/workspaces/{workspace_id}")
    if workspace.get("capacityId") != capacity["id"]:
        raise RuntimeError("Existing workspace is assigned to a different capacity")
    deadline = time.monotonic() + 180
    while workspace.get("capacityAssignmentProgress") in ("InProgress", "Pending"):
        if time.monotonic() > deadline:
            raise TimeoutError("Workspace capacity assignment has not completed")
        time.sleep(5)
        workspace = api.call("GET", f"/workspaces/{workspace_id}")
    if workspace.get("capacityAssignmentProgress") == "Failed":
        raise RuntimeError("Workspace capacity assignment failed")
    lakehouse = exact(api.list(f"/workspaces/{workspace_id}/lakehouses"), CONFIG["lakehouse"])
    if not lakehouse:
        lakehouse = api.call("POST", f"/workspaces/{workspace_id}/lakehouses", {
            "displayName": CONFIG["lakehouse"],
            "description": "Schema-enabled GitHub medallion lakehouse",
            "creationPayload": {"enableSchemas": True},
        })
    state["lakehouse_id"] = lakehouse["id"]
    save(state)
    print(json.dumps(state, indent=2))


def bootstrap(_args):
    api = Fabric("spark-cli")
    state = load()
    if state.get("schemas_created"):
        print("Medallion schemas were already created")
        return
    base = (
        f"/workspaces/{state['workspace_id']}/lakehouses/{state['lakehouse_id']}"
        "/livyapi/versions/2023-12-01"
    )
    # Recover the session ID rather than submitting a duplicate after interruption.
    if state.get("bootstrap_session_id") is None:
        _, _, session = api.request("POST", base + "/sessions", {
            "name": "github-medallion-bootstrap",
            "driverMemory": "28g", "driverCores": 4,
            "executorMemory": "28g", "executorCores": 4,
            "conf": {"spark.dynamicAllocation.enabled": "true"},
        })
        state["bootstrap_session_id"] = session["id"]
        save(state)
    session_path = base + f"/sessions/{state['bootstrap_session_id']}"
    deadline = time.monotonic() + 720
    try:
        while time.monotonic() < deadline:
            session = api.call("GET", session_path)
            print(f"Bootstrap session: {session['state']}", flush=True)
            if session["state"] == "idle":
                break
            if session["state"] in ("error", "dead", "killed", "success"):
                raise RuntimeError(json.dumps(session))
            time.sleep(15)
        else:
            raise TimeoutError("Bootstrap session readiness timed out")
        if state.get("bootstrap_statement_id") is None:
            _, _, statement = api.request("POST", session_path + "/statements", {
                "kind": "pyspark",
                "code": (
                    "for layer in ['bronze', 'silver', 'gold']:\n"
                    "    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {layer}')\n"
                    "print(spark.sql('SHOW SCHEMAS').toJSON().collect())"
                ),
            })
            state["bootstrap_statement_id"] = statement["id"]
            save(state)
        while time.monotonic() < deadline:
            statement = api.call(
                "GET", session_path + f"/statements/{state['bootstrap_statement_id']}"
            )
            if statement["state"] == "available":
                print(json.dumps(statement["output"]), flush=True)
                if statement["output"]["status"] != "ok":
                    raise RuntimeError(json.dumps(statement["output"]))
                state["schemas_created"] = True
                save(state)
                return
            if statement["state"] in ("error", "cancelled"):
                raise RuntimeError(json.dumps(statement))
            time.sleep(5)
        raise TimeoutError("Bootstrap DDL timed out")
    finally:
        api.request("DELETE", session_path)


def notebook_definition(path, state):
    import nbformat

    source = path.read_text(encoding="utf-8")
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell(
                f"# {path.stem.replace('_', ' ')}\nSource: {CONFIG['repository']}"
            ),
            nbformat.v4.new_code_cell(source),
        ],
        metadata={
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python", "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
            "dependencies": {"lakehouse": {
                "default_lakehouse": state["lakehouse_id"],
                "default_lakehouse_name": CONFIG["lakehouse"],
                "default_lakehouse_workspace_id": state["workspace_id"],
            }},
        },
    )
    nbformat.validate(notebook)
    output = ROOT / ".local" / (path.stem + ".ipynb")
    output.write_text(nbformat.writes(notebook), encoding="utf-8")
    return {"format": "ipynb", "parts": [{
        "path": "notebook-content.ipynb", "payloadType": "InlineBase64",
        "payload": base64.b64encode(output.read_bytes()).decode(),
    }]}


def deploy(_args):
    state = load()
    if not state.get("schemas_created"):
        raise RuntimeError("Run bootstrap before deploying layer notebooks")
    api = Fabric("spark-cli")
    workspace = f"/workspaces/{state['workspace_id']}"
    items = api.list(workspace + "/items?type=Notebook")
    state.setdefault("notebooks", {})
    for path in sorted((ROOT / "notebooks").glob("*.py")):
        definition = notebook_definition(path, state)
        name = path.stem
        item = exact(items, name)
        if item:
            api.call("POST", workspace + f"/notebooks/{item['id']}/updateDefinition",
                     {"definition": definition})
        else:
            item = api.call("POST", workspace + "/items", {
                "displayName": name, "type": "Notebook", "definition": definition,
            })
        state["notebooks"][name] = item["id"]
        save(state)
        print(f"Deployed {name}: {item['id']}", flush=True)


def stage(_args):
    state = load()
    api = Fabric()
    repository = CONFIG["repository"]
    batch = uuid.uuid4().hex
    captured = datetime.now(timezone.utc).isoformat()
    rows = []
    counts = {}
    for entity, endpoint, key in [
        ("repositories", "", "id"),
        ("commits", "/commits?per_page=100", "sha"),
        ("issues", "/issues?state=all&per_page=100", "id"),
        ("pull_requests", "/pulls?state=all&per_page=100", "id"),
    ]:
        url = f"repos/{repository}{endpoint}"
        if not endpoint:
            objects = [command_json(["gh", "api", url])]
            if objects[0]["private"]:
                raise ValueError("This public-data POC does not ingest private repositories")
        else:
            pages = command_json(["gh", "api", "--paginate", "--slurp", url])
            objects = [item for page in pages for item in page]
        counts[entity] = len(objects)
        for item in objects:
            rows.append({
                "entity": entity, "record_id": str(item[key]),
                "payload": json.dumps(item, separators=(",", ":")),
                "source_url": f"https://api.github.com/{url}",
                "batch_id": batch, "ingested_at": captured,
            })
    lakehouse = api.call(
        "GET", f"/workspaces/{state['workspace_id']}/lakehouses/{state['lakehouse_id']}"
    )
    files = lakehouse["properties"]["oneLakeFilesPath"]
    parsed = urllib.parse.urlparse(files)
    if parsed.scheme == "abfss":
        files = f"https://{parsed.hostname}/{parsed.username}{parsed.path}"
    if not files.startswith("https://"):
        raise RuntimeError(f"Unsupported OneLake files path: {files}")
    directory = files.rstrip("/") + f"/github/{batch}"

    def upload(path, data):
        api.request("PUT", path + "?resource=file", raw=b"",
                    resource="https://storage.azure.com")
        api.request("PATCH", path + "?action=append&position=0", raw=data,
                    headers={"Content-Type": "application/octet-stream"},
                    resource="https://storage.azure.com")
        api.request("PATCH", path + f"?action=flush&position={len(data)}", raw=b"",
                    resource="https://storage.azure.com")

    # A unique batch directory cannot overwrite an existing completed load.
    api.request("PUT", directory + "?resource=directory", raw=b"",
                resource="https://storage.azure.com")
    upload(directory + "/records.jsonl",
           ("\n".join(json.dumps(row) for row in rows) + "\n").encode())
    manifest = {
        "batch_id": batch, "ingested_at": captured, "repository": repository,
        "counts": counts, "record_count": len(rows),
    }
    upload(directory + "/manifest.json", json.dumps(manifest).encode())
    # Only publish the batch pointer after every source page and upload succeeded.
    upload(files.rstrip("/") + "/github/latest.json", json.dumps(manifest).encode())
    state["latest_batch"] = manifest
    save(state)
    print(json.dumps(manifest, indent=2))


def run(args):
    state = load()
    api = Fabric("spark-cli")
    workspace = f"/workspaces/{state['workspace_id']}"
    selected = args.notebook or sorted(state["notebooks"])
    for name in selected:
        item = state["notebooks"][name]
        path = workspace + f"/items/{item}/jobs/instances"
        recent = api.list(path)
        active = [
            j for j in recent
            if j["status"] in ("NotStarted", "Running", "InProgress", "Deduped")
        ]
        if len(active) > 1:
            raise RuntimeError(f"Multiple active jobs for {name}; inspect job history")
        if active:
            job_id = active[0]["id"]
        else:
            _, headers, _ = api.request("POST", path + "?jobType=RunNotebook", {
                "executionData": {"configuration": {"defaultLakehouse": {
                    "id": state["lakehouse_id"], "name": CONFIG["lakehouse"],
                    "workspaceId": state["workspace_id"],
                }}},
            })
            location = next(v for k, v in headers.items() if k.lower() == "location")
            job_id = location.rstrip("/").split("/")[-1]
        state.setdefault("jobs", {})[name] = job_id
        save(state)
        deadline = time.monotonic() + 1800
        last_status = None
        while time.monotonic() < deadline:
            job = api.call("GET", path + "/" + job_id)
            if job["status"] != last_status:
                print(f"{name}: {job['status']} ({job_id})", flush=True)
                last_status = job["status"]
            if job["status"] == "Completed":
                break
            if job["status"] in ("Failed", "Cancelled"):
                raise RuntimeError(json.dumps(job))
            time.sleep(15)
        else:
            raise TimeoutError(f"Job {job_id} still running; monitor it, do not resubmit")


def status(_args):
    state = load()
    api = Fabric()
    workspace = f"/workspaces/{state['workspace_id']}"
    print(json.dumps(api.list(workspace + "/items"), indent=2))
    for name, job_id in state.get("jobs", {}).items():
        job = api.call(
            "GET", workspace + f"/items/{state['notebooks'][name]}/jobs/instances/{job_id}"
        )
        print(name, json.dumps(job))


def model(args):
    """Deploy the exported model snapshot; replacement must be explicitly requested."""
    state = load()
    api = Fabric("semantic-model-authoring")
    workspace = f"/workspaces/{state['workspace_id']}"
    existing = exact(api.list(workspace + "/items?type=SemanticModel"), CONFIG["semantic_model"])
    if existing and not args.replace:
        raise RuntimeError(
            "The semantic model already exists. Use modeling tools for edits, or "
            "model --replace to deliberately replace it with the checked-in snapshot."
        )
    text = (ROOT / "semantic_model" / "model.bim").read_text()
    text = text.replace("${WORKSPACE_ID}", state["workspace_id"])
    text = text.replace("${LAKEHOUSE_ID}", state["lakehouse_id"])
    database = json.loads(text)
    database["name"] = CONFIG["semantic_model"]
    parts = []
    if existing:
        current = api.call(
            "POST", workspace + f"/semanticModels/{existing['id']}/getDefinition?format=TMSL",
            {},
        )
        parts = [
            part for part in current["definition"]["parts"]
            if part["path"] not in (".platform", "model.bim", "definition.pbism")
        ]
    for path, content in [
        ("model.bim", json.dumps(database).encode()),
        ("definition.pbism", (ROOT / "semantic_model" / "definition.pbism").read_bytes()),
    ]:
        parts.append({
            "path": path, "payloadType": "InlineBase64",
            "payload": base64.b64encode(content).decode(),
        })
    definition = {"format": "TMSL", "parts": parts}
    if existing:
        api.call("POST", workspace + f"/semanticModels/{existing['id']}/updateDefinition",
                 {"definition": definition})
        item = existing
    else:
        item = api.call("POST", workspace + "/semanticModels", {
            "displayName": CONFIG["semantic_model"], "definition": definition,
        })
    state["semantic_model_id"] = item["id"]
    save(state)
    print(f"Deployed semantic model: {item['id']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    create = commands.add_parser("provision")
    create.add_argument("--capacity-id", required=True)
    commands.add_parser("bootstrap")
    commands.add_parser("deploy")
    commands.add_parser("stage")
    execute = commands.add_parser("run")
    execute.add_argument("--notebook", action="append")
    commands.add_parser("status")
    semantic = commands.add_parser("model")
    semantic.add_argument("--replace", action="store_true")
    options = parser.parse_args()
    globals()[options.action](options)
