# %% [markdown]
# ## Refresh the existing model, then commit extraction checkpoints
#
# This is the final step after discovery, ingestion, Bronze, Silver and Gold.
# **Inputs:** the published manifest, matching Gold validation, and the existing
# semantic-model ID supplied by deployment.
#
# **Outputs:** a model-refresh audit in `Files/github/model_refreshes/` and,
# only after success, `Files/github/checkpoint.json` for the next extraction.
# The existing Direct Lake model is refreshed/reframed; no model or report is
# recreated and no additional GitHub data is fetched here.
#
# ## Coordinate refresh submission, polling and checkpoint promotion
#
# Refuse overlapping model refreshes. Submit one transactional full refresh and
# poll that specific request with bounded safe GET retries. Never retry a refresh
# POST blindly: the first request could have been accepted even if its response
# was lost. Failures leave the previous checkpoint in place so missed changes
# can be replayed safely by the next run.
# %%
from datetime import datetime, timezone
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def refresh_model(model_id, utilities):
    """Gate on Gold, refresh the model, persist its audit and advance checkpoints."""
    model_id = str(uuid.UUID(model_id))
    workspace_id = str(uuid.UUID(utilities.runtime.context["currentWorkspaceId"]))
    base = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{model_id}/refreshes"
    )
    manifest = json.loads(utilities.fs.head("Files/github/latest.json", 16384))
    gold = json.loads(utilities.fs.head("Files/github/validation_gold.json", 1048576))
    if gold["batch_id"] != manifest["batch_id"]:
        raise ValueError("The published batch has not completed Gold; checkpoint was not advanced")
    # Legacy full batches can refresh the model but do not carry incremental checkpoints.
    proposed = manifest.get("checkpoints")
    if proposed is not None and set(proposed) != {
        str(repository["id"]) for repository in manifest["repositories"]
    }:
        raise ValueError("Proposed checkpoints do not cover the completed repository inventory")

    def request(method, url, body=None):
        """Send a Power BI request; retry only status reads, never refresh submission."""
        # Acquire a runtime token per call rather than retaining one throughout a long poll.
        token = utilities.credentials.getToken("pbi")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        raw = json.dumps(body).encode() if body is not None else None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(
                    urllib.request.Request(url, data=raw, headers=headers, method=method), timeout=60
                ) as response:
                    data = response.read()
                    return dict(response.headers), json.loads(data) if data else None
            except urllib.error.HTTPError as error:
                error.close()
                if method == "GET" and error.code in (429, 502, 503, 504) and attempt < 2:
                    wait = min(int(error.headers.get("Retry-After", 5 * 2**attempt)), 60)
                    print(f"Refresh status GET HTTP {error.code}; retrying in {wait}s", flush=True)
                    time.sleep(max(wait, 1))
                    continue
                raise RuntimeError(
                    f"Semantic-model refresh request failed: HTTP {error.code}. "
                    "Check the pipeline run identity, model permissions and model connections; "
                    "the refresh was not resubmitted."
                ) from error
            except urllib.error.URLError as error:
                raise RuntimeError(
                    "Semantic-model refresh network error; inspect refresh history before "
                    "rerunning. The request was not resubmitted."
                ) from error
        raise RuntimeError("Refresh status GET retry limit reached")

    _, history = request("GET", base + "?$top=5")
    if any(r["status"] in ("Unknown", "NotStarted", "InProgress") for r in history["value"]):
        raise RuntimeError("A semantic-model refresh is already active; wait before rerunning")
    started = datetime.now(timezone.utc).isoformat()
    headers, _ = request("POST", base, {
        "type": "full", "commitMode": "transactional", "retryCount": 0,
    })
    request_id = next((v for k, v in headers.items() if k.lower() == "x-ms-request-id"), None)
    location = next((v for k, v in headers.items() if k.lower() == "location"), "")
    # Service Location headers may use regional URLs. Extract only the request ID,
    # then poll the canonical endpoint instead of forwarding credentials to that URL.
    candidate = urllib.parse.urlsplit(location).path.rstrip("/").rsplit("/", 1)[-1]
    refresh_id = candidate if re.fullmatch(r"[a-fA-F0-9-]{36}", candidate) else request_id
    if not refresh_id:
        raise RuntimeError("Refresh accepted without a request ID; inspect history, do not resubmit")
    refresh_id = str(uuid.UUID(refresh_id))
    location = base + "/" + refresh_id
    audit = {
        "model_id": model_id, "refresh_id": refresh_id,
        "started_at": started, "status": "InProgress", "batch_id": manifest["batch_id"],
    }
    utilities.fs.mkdirs("Files/github/model_refreshes")
    audit_path = f"Files/github/model_refreshes/{refresh_id}.json"
    if utilities.fs.put(audit_path, json.dumps(audit), False) is False:
        raise RuntimeError(f"Refresh {refresh_id} was submitted but its audit write failed")
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        time.sleep(10)
        _, result = request("GET", location)
        status = result["status"]
        print(f"Semantic model: {status}", flush=True)
        if status == "Completed":
            audit.update(status=status, completed_at=datetime.now(timezone.utc).isoformat())
            if utilities.fs.put(audit_path, json.dumps(audit), True) is False:
                raise RuntimeError(f"Refresh {refresh_id} completed but its audit write failed")
            if proposed is not None:
                # This is the success boundary for extraction, not the earlier landing write.
                checkpoint = {
                    "batch_id": manifest["batch_id"], "owner": manifest["owner"],
                    "refresh_id": refresh_id, "repositories": proposed,
                }
                if utilities.fs.put(
                    "Files/github/checkpoint.json", json.dumps(checkpoint), True
                ) is False:
                    raise RuntimeError("Model refreshed but checkpoint publication failed; replay is safe")
            else:
                print("Legacy full batch has no checkpoints; the next native run will bootstrap")
            return audit
        if status in ("Failed", "Cancelled", "Disabled"):
            audit["status"] = status
            if utilities.fs.put(audit_path, json.dumps(audit), True) is False:
                raise RuntimeError(f"Refresh {refresh_id} {status}; its audit update also failed")
            raise RuntimeError(f"Semantic-model refresh {refresh_id} ended with {status}")
    raise TimeoutError(f"Refresh {refresh_id} is still pending; inspect it before rerunning")


# %% [markdown]
# ## Execute as the pipeline's final activity
#
# Use the Fabric run identity's Power BI token and existing model permissions.
# Print only the audit metadata, never credentials. Successful return means
# the model refreshed and any proposed checkpoints were published.
# %%
if __name__ == "__main__":
    import notebookutils

    print(json.dumps(refresh_model(semantic_model_id, notebookutils)), flush=True)
