"""Deploy or run the Fabric-native GitHub refresh pipeline."""

import argparse
import base64
import json
import time

from fabric_api import Fabric
import project

NAME = "GitHub Analytics Refresh"
NOTEBOOKS = (
    "00_discover_repositories", "00_ingest_github",
    "01_bronze", "02_silver", "03_gold", "04_refresh_semantic_model",
)
ACTIVE = ("NotStarted", "Running", "InProgress", "Deduped")


def pipeline_content(state):
    activities = []
    for name in NOTEBOOKS:
        activities.append({
            "name": name, "type": "TridentNotebook",
            "dependsOn": [{
                "activity": activities[-1]["name"], "dependencyConditions": ["Succeeded"],
            }] if activities else [],
            "policy": {
                "timeout": "0.01:00:00", "retry": 0,
                "secureInput": True, "secureOutput": False,
            },
            "typeProperties": {
                "notebookId": state["notebooks"][name],
                "workspaceId": state["workspace_id"],
            },
        })
    return {"properties": {
        "description": "Public GitHub discovery, API landing, Bronze/Silver/Gold and model refresh",
        "concurrency": 1, "activities": activities,
    }}


def deploy(_args):
    state = project.load()
    api = Fabric("spark-cli")
    workspace = f"/workspaces/{state['workspace_id']}"
    model = project.exact(
        api.list(workspace + "/items?type=SemanticModel"), project.CONFIG["semantic_model"]
    )
    if not model or model["id"] != state.get("semantic_model_id"):
        raise RuntimeError("Resolve the existing semantic-model deployment before deploying refresh")
    content = pipeline_content(state)
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    definition = {"parts": [{
        "path": "pipeline-content.json", "payloadType": "InlineBase64", "payload": encoded,
    }]}
    item = project.exact(api.list(workspace + "/items?type=DataPipeline"), NAME)
    if item:
        api.call("POST", workspace + f"/items/{item['id']}/getDefinition", {})
        api.call("POST", workspace + f"/items/{item['id']}/updateDefinition",
                 {"definition": definition})
    else:
        item = api.call("POST", workspace + "/items", {
            "displayName": NAME, "type": "DataPipeline", "definition": definition,
        })
    state["refresh_pipeline_id"] = item["id"]
    project.save(state)
    verify(_args)
    print(f"Deployed {NAME}: {item['id']} (no schedule created)", flush=True)


def verify(_args):
    state = project.load()
    api = Fabric("spark-cli")
    path = f"/workspaces/{state['workspace_id']}/items/{state['refresh_pipeline_id']}"
    result = api.call("POST", path + "/getDefinition", {})
    part = next(p for p in result["definition"]["parts"] if p["path"] == "pipeline-content.json")
    actual = json.loads(base64.b64decode(part["payload"]))
    expected = pipeline_content(state)["properties"]
    properties = actual["properties"]
    if properties.get("concurrency") != 1:
        raise ValueError("Published pipeline does not serialize concurrent refresh runs")
    activities = properties["activities"]
    if [a["name"] for a in activities] != list(NOTEBOOKS):
        raise ValueError("Published pipeline has unexpected activities or ordering")
    for activity, wanted in zip(activities, expected["activities"]):
        for field in ("type", "dependsOn", "typeProperties", "policy"):
            if activity[field] != wanted[field]:
                raise ValueError(f"Published pipeline changed {activity['name']}.{field}")
    print("Published pipeline dependencies, notebook bindings and concurrency verified", flush=True)


def run(_args):
    state = project.load()
    api = Fabric("spark-cli")
    workspace = f"/workspaces/{state['workspace_id']}"
    capacity = api.call("GET", workspace).get("capacityId")
    if not any(c["id"] == capacity and c["state"] == "Active" for c in api.list("/capacities")):
        raise RuntimeError("The workspace needs an active capacity; no capacity was started")
    path = workspace + f"/items/{state['refresh_pipeline_id']}/jobs/instances"
    active = [j for j in api.list(path) if j["status"] in ACTIVE]
    if len(active) > 1:
        raise RuntimeError("Multiple pipeline runs are active; inspect the run history")
    if active:
        job_id = active[0]["id"]
    else:
        for name in NOTEBOOKS:
            jobs = api.list(workspace + f"/items/{state['notebooks'][name]}/jobs/instances")
            if any(j["status"] in ACTIVE for j in jobs):
                raise RuntimeError(f"{name} already has an active job; do not overlap refreshes")
        _, headers, _ = api.request("POST", path + "?jobType=Pipeline", {})
        location = next(v for k, v in headers.items() if k.lower() == "location")
        job_id = location.rstrip("/").rsplit("/", 1)[-1]
    state["refresh_pipeline_job_id"] = job_id
    project.save(state)
    deadline = time.monotonic() + 7200
    last_status = None
    while time.monotonic() < deadline:
        result = api.call("GET", path + "/" + job_id)
        if result["status"] != last_status:
            last_status = result["status"]
            print(f"{NAME}: {last_status} ({job_id})", flush=True)
        if result["status"] == "Completed":
            files = project.files_url(api, state)
            manifest = api.request(
                "GET", files + "/github/latest.json", resource="https://storage.azure.com"
            )[2]
            state["latest_batch"] = manifest
            project.save(state)
            print(json.dumps(manifest, indent=2), flush=True)
            return
        if result["status"] in ("Failed", "Cancelled"):
            raise RuntimeError(json.dumps(result))
        time.sleep(20)
    raise TimeoutError(f"Pipeline {job_id} is still running; monitor it, do not resubmit")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("deploy", "verify", "run"))
    args = parser.parse_args()
    globals()[args.action](args)
