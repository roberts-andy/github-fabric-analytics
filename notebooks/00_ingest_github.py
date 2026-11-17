# %% [markdown]
# ## Land GitHub activity from the Fabric notebook
#
# **Inputs:** the discovery inventory, notebook authentication/load parameters,
# and `Files/github/checkpoint.json` from the last fully successful refresh.
# The GitHub token is retrieved from Azure Key Vault at runtime, never embedded
# in notebook parameters or written to the lakehouse.
#
# **Outputs:** a unique batch directory containing raw JSON Lines and a manifest,
# then `Files/github/latest.json` pointing to the completed extraction.
# Each record retains repository ID, entity, source URL, batch ID and capture time.
#
# HTTP runs on the driver, not once per Spark row: this gives pagination, retries,
# rate limits and batch publication one controlled execution path.
#
# ## HTTP and credential helpers
#
# All GitHub requests, next-page links and redirects must remain on
# `https://api.github.com` so authenticated requests cannot follow an external
# link. Only bounded transient GET failures are retried; authentication and
# permission failures never trigger a silent switch to anonymous access.
# %%
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def github_url(url):
    """Validate a GitHub API URL before requesting it or following a link."""
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.netloc != "api.github.com"
            or parsed.fragment or parsed.username or parsed.password):
        raise ValueError("GitHub requests must stay on https://api.github.com")
    return url


class GitHubRedirect(urllib.request.HTTPRedirectHandler):
    """Apply the same host restriction to redirects as to ordinary requests."""

    def redirect_request(self, request, response, code, message, headers, url):
        """Reject a redirect target before urllib forwards the request."""
        github_url(url)
        return super().redirect_request(request, response, code, message, headers, url)


class GitHubAPIError(RuntimeError):
    """Expose HTTP status so commit reconciliation can distinguish a missing base."""

    def __init__(self, status, message):
        """Retain status separately from the safe, credential-free error text."""
        super().__init__(message)
        self.status = status


def next_page(link):
    """Extract one validated next-page URL, rejecting ambiguous pagination."""
    links = re.findall(r'<([^>]+)>;\s*rel="next"', link)
    if len(links) > 1:
        raise ValueError("Ambiguous GitHub next-page link")
    return github_url(links[0]) if links else None


class GitHubAPI:
    """Driver-side JSON client with guarded pagination and bounded GET retries."""

    def __init__(self, token=None):
        """Keep an optional runtime token in request headers, not in batch data."""
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "github-fabric-analytics-ingestion",
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        self.opener = urllib.request.build_opener(GitHubRedirect())
        self.requests = 0

    def page(self, url, allow_empty_repository=False):
        """Read one JSON response and its Link header or raise an explicit failure."""
        github_url(url)
        for attempt in range(3):
            request = urllib.request.Request(url, headers=self.headers)
            self.requests += 1
            try:
                with self.opener.open(request, timeout=60) as response:
                    return json.load(response), response.headers.get("Link", "")
            except urllib.error.HTTPError as error:
                with error:
                    raw = error.read().decode("utf-8", errors="replace")
                if error.code == 409 and allow_empty_repository:
                    # A zero-size repository can still have commits; only this API error proves emptiness.
                    body = json.loads(raw)
                    if body.get("message") == "Git Repository is empty.":
                        print(f"{url}: empty repository (0 commits)", flush=True)
                        return [], ""
                if error.code in (429, 502, 503, 504) and attempt < 2:
                    wait = min(int(error.headers.get("Retry-After", 5 * 2**attempt)), 60)
                    print(f"GitHub HTTP {error.code}; retrying GET after {wait}s", flush=True)
                    time.sleep(max(wait, 1))
                    continue
                raise GitHubAPIError(error.code,
                    f"GitHub extraction failed: HTTP {error.code} at {url}; "
                    f"remaining={error.headers.get('X-RateLimit-Remaining')}, "
                    f"reset Unix time={error.headers.get('X-RateLimit-Reset')}. "
                    "Check GitHub availability, rate limits and configured credentials. "
                    "The previous completed batch has not been replaced."
                ) from error
            except urllib.error.URLError as error:
                raise RuntimeError(
                    f"GitHub network request failed at {url}; previous batch retained"
                ) from error
        raise RuntimeError("GitHub GET retry limit reached")

    def pages(self, url, allow_empty_repository=False):
        """Yield every object from list pages while detecting cycles and bad shapes."""
        seen = set()
        while url:
            github_url(url)
            if url in seen:
                raise ValueError("Cyclic GitHub pagination link")
            seen.add(url)
            page, link = self.page(url, allow_empty_repository)
            if not isinstance(page, list):
                raise ValueError(f"Expected a GitHub list response at {url}")
            yield from page
            url = next_page(link)


def github_token(mode, vault_url, secret_name, utilities):
    """Resolve the selected credential mode without logging or persisting the token."""
    if mode == "anonymous":
        if vault_url or secret_name:
            raise ValueError("Select key_vault authentication to use a configured secret")
        print("Using anonymous public GitHub access (low shared-IP rate limit)", flush=True)
        return None
    if mode != "key_vault":
        raise ValueError("github_auth_mode must be anonymous or key_vault")
    if not re.fullmatch(r"https://[a-zA-Z0-9-]+\.vault\.azure\.net/?", vault_url):
        raise ValueError("Specify a valid Azure Key Vault HTTPS URL")
    if not re.fullmatch(r"[a-zA-Z0-9-]+", secret_name):
        raise ValueError("Specify a valid GitHub token secret name")
    # An unversioned reference picks up token rotation without redeploying this notebook.
    token = utilities.credentials.getSecret(vault_url, secret_name)
    if not isinstance(token, str) or not token.strip():
        raise ValueError("The configured GitHub secret is empty")
    if token.strip() == "REPLACE_WITH_GITHUB_TOKEN":
        raise ValueError(
            "Replace the github-token placeholder in Azure Key Vault with your "
            "GitHub token before running ingestion"
        )
    return token.strip()


# %% [markdown]
# ## Decide which changes to extract
#
# Commits use saved default-branch head SHAs, not author-date filters: a newly
# pushed commit can have an old author date. Fast-forward comparisons fetch only
# new commits; a changed default branch or unavailable/rewritten history requires
# a complete commit reconciliation for that repository.
#
# Issues use the last successful watermark minus five minutes. The overlap
# reduces boundary/timing gaps, and Silver upserts make repeated records safe.
# Checkpoints are proposed here but committed only after Gold and the semantic
# model refresh succeed, so a failed chain does not skip changes on the next run.
# %%
def changed_commits(client, base, branch, previous, full):
    """Return commit objects, source URL, load mode and the captured branch head."""
    head_page, _ = client.page(
        base + "/commits?" + urllib.parse.urlencode({"sha": branch, "per_page": 1}),
        allow_empty_repository=True,
    )
    if not isinstance(head_page, list) or len(head_page) > 1:
        raise ValueError("Expected one default-branch head commit")
    head = head_page[0]["sha"] if head_page else None
    old_head = previous.get("head_sha")
    for sha in (head, old_head):
        if sha is not None and not re.fullmatch(r"[a-f0-9]{40,64}", sha):
            raise ValueError("Invalid commit SHA in GitHub response or checkpoint")
    if not full and head == old_head:
        return [], base + "/commits", "incremental", head
    full_url = base + "/commits?" + urllib.parse.urlencode({"sha": head, "per_page": 100})
    if not full and head and old_head:
        url = f"{base}/compare/{old_head}...{head}?per_page=100"
        compare_url = url
        seen = set()
        commits = []
        expected = None
        while url:
            if url in seen:
                raise ValueError("Cyclic GitHub comparison pagination")
            seen.add(url)
            try:
                page, link = client.page(url)
            except GitHubAPIError as error:
                if error.status != 404:
                    raise
                print(f"{base}: previous commit is unavailable; reconciling commit history")
                break
            if page["status"] in ("behind", "diverged"):
                print(f"{base}: branch history changed; reconciling commits")
                break
            if page["status"] != "ahead" or page["base_commit"]["sha"] != old_head:
                raise ValueError("Unexpected GitHub commit comparison")
            if page["merge_base_commit"]["sha"] != old_head:
                raise ValueError("Commit comparison is not a fast-forward")
            if expected is None:
                expected = page["total_commits"]
            commits.extend(page["commits"])
            url = next_page(link)
        else:
            # Only a fully exhausted, fast-forward comparison is safe to treat as incremental.
            if len(commits) != expected or not any(c["sha"] == head for c in commits):
                raise ValueError("Incomplete paginated commit comparison")
            return commits, compare_url, "incremental", head
    # Anchor a full read to the captured SHA so a moving branch name cannot change its target.
    objects = client.pages(full_url, allow_empty_repository=True) if head else []
    return objects, full_url, "full", head


def incremental_since(previous, captured):
    """Return a UTC overlap boundary based on the last successful capture start."""
    watermark = datetime.fromisoformat(previous["watermark"])
    now = datetime.fromisoformat(captured)
    if watermark.tzinfo is None or watermark > now:
        raise ValueError("Checkpoint watermark must be timezone-aware and not in the future")
    return (watermark.astimezone(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")


# %% [markdown]
# ## Extract and publish one complete batch
#
# Recheck inventory eligibility before extracting. New repositories bootstrap
# fully; existing repositories use their own checkpoint and per-entity load mode.
# GitHub's issues feed includes PR markers, allowing us to fetch details only for
# PRs that changed instead of rereading the entire PR collection.
#
# The unique output directory prevents overwriting earlier captures. Publish the
# immutable manifest after closing the records file, and update `latest.json`
# last. A failed extraction therefore leaves the previous published batch intact.
# This change feed does not include deletion tombstones; use explicit `full` mode
# for periodic deletion reconciliation.
# %%
def ingest(inventory, owner, client, utilities, files, checkpoint=None, load_mode="incremental"):
    """Validate scope, land source records and publish a complete batch manifest."""
    if load_mode not in ("incremental", "full"):
        raise ValueError("load_mode must be incremental or full")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{0,38}", owner):
        raise ValueError("Invalid GitHub owner login")
    repositories = inventory["repositories"]
    if (inventory["owner"].lower() != owner.lower() or not repositories
            or inventory["repository_count"] != len(repositories)
            or len({r["id"] for r in repositories}) != len(repositories)):
        raise ValueError("Inventory is incomplete, duplicated or belongs to another owner")
    for descriptor in repositories:
        if not re.fullmatch(
            rf"{re.escape(owner)}/[a-zA-Z0-9_.-]+", descriptor["full_name"], re.I
        ):
            raise ValueError("Invalid repository name in inventory")

    # One fresh owner listing rechecks eligibility without a request per repository.
    current = {
        r["id"]: r for r in client.pages(
            f"https://api.github.com/users/{urllib.parse.quote(owner)}/repos"
            "?type=owner&per_page=100&sort=full_name"
        )
        if r["private"] is False and r["fork"] is False
        and r["owner"]["login"].lower() == owner.lower()
    }
    for descriptor in repositories:
        details = current.get(descriptor["id"])
        if not details or details["full_name"].lower() != descriptor["full_name"].lower():
            raise ValueError(f"{descriptor['full_name']} changed eligibility/name; rerun discovery")

    if not files.is_dir():
        raise ValueError("The default lakehouse Files mount is missing")
    batch = uuid.uuid4().hex
    # Use capture start, not finish, so changes arriving during extraction are replayed next time.
    captured = datetime.now(timezone.utc).isoformat()
    if checkpoint is not None and checkpoint["owner"].lower() != owner.lower():
        raise ValueError("Checkpoint belongs to another GitHub owner")
    previous_repositories = checkpoint["repositories"] if checkpoint else {}
    destination = files / "github" / batch
    destination.mkdir(parents=True, exist_ok=False)
    counts = dict.fromkeys(("repositories", "commits", "issues", "pull_requests"), 0)
    repository_counts = {}
    repository_loads = {}
    checkpoints = {}
    seen_keys = set()
    with (destination / "records.jsonl").open("x", encoding="utf-8") as output:
        for descriptor in repositories:
            details = current[descriptor["id"]]
            full_name = details["full_name"]
            repository_id = str(details["id"])
            previous = previous_repositories.get(repository_id, {})
            branch = details["default_branch"]
            full = (
                load_mode == "full" or not previous
                or previous["default_branch"] != branch
            )
            since = None if full else incremental_since(previous, captured)
            base = f"https://api.github.com/repos/{full_name}"
            commits, commits_url, commits_mode, head = changed_commits(
                client, base, branch, previous, full
            )
            modes = {
                "repositories": "full", "commits": commits_mode,
                "issues": "full" if full else "incremental",
                "pull_requests": "full" if full else "incremental",
            }
            issue_url = base + "/issues?" + urllib.parse.urlencode({
                "state": "all", "per_page": 100,
                **({"since": since, "sort": "updated", "direction": "desc"} if since else {}),
            })
            changed_pulls = set()
            per_repository = {}
            for entity, url, key in [
                ("repositories", base, "id"),
                ("commits", commits_url, "sha"),
                ("issues", issue_url, "id"),
                ("pull_requests", base + "/pulls?state=all&per_page=100", "id"),
            ]:
                if entity == "repositories":
                    objects = [details]
                elif entity == "commits":
                    objects = commits
                elif entity == "pull_requests" and not full:
                    # The issues loop runs first and has already collected changed PR numbers.
                    objects = (client.page(f"{base}/pulls/{number}")[0]
                               for number in sorted(changed_pulls))
                else:
                    objects = client.pages(url)
                count = 0
                for item in objects:
                    source_url = url
                    if entity == "issues" and item.get("pull_request") is not None:
                        # Keep the raw marker in Bronze; Silver separates true issues from PRs.
                        changed_pulls.add(int(item["number"]))
                    if entity == "pull_requests" and not full:
                        source_url = f"{base}/pulls/{item['number']}"
                    record_id = str(item[key])
                    natural_key = (details["id"], entity, record_id)
                    if natural_key in seen_keys:
                        raise ValueError(f"Duplicate GitHub record: {natural_key}")
                    seen_keys.add(natural_key)
                    output.write(json.dumps({
                        "repository_id": details["id"], "entity": entity,
                        "record_id": record_id,
                        "payload": json.dumps(item, separators=(",", ":")),
                        "source_url": source_url, "batch_id": batch, "ingested_at": captured,
                    }) + "\n")
                    count += 1
                per_repository[entity] = count
                counts[entity] += count
            if commits_mode == "full" and head and (details["id"], "commits", head) not in seen_keys:
                raise ValueError("Full commit snapshot does not contain its anchored branch head")
            repository_counts[repository_id] = per_repository
            repository_loads[repository_id] = modes
            checkpoints[repository_id] = {
                "default_branch": branch, "head_sha": head, "watermark": captured,
            }
            print(f"{full_name}: {json.dumps(per_repository)}", flush=True)

    # Store load modes alongside counts so Silver can distinguish deltas from replacement snapshots.
    manifest = {
        "batch_id": batch, "ingested_at": captured, "owner": owner,
        "discovery_run_id": inventory["discovery_run_id"],
        "repository_count": len(repositories), "repositories": repositories,
        "repository_counts": repository_counts, "counts": counts,
        "record_count": sum(counts.values()), "extractor": "fabric-notebook",
        "github_request_count": client.requests,
        "load_mode": load_mode, "repository_loads": repository_loads,
        "checkpoints": checkpoints,
    }
    content = json.dumps(manifest)
    if utilities.fs.put(f"Files/github/{batch}/manifest.json", content, False) is False:
        raise RuntimeError("Failed to persist the immutable manifest; previous batch retained")
    if utilities.fs.put("Files/github/latest.json", content, True) is False:
        raise RuntimeError("Failed to publish the latest completed batch pointer")
    return manifest


# %% [markdown]
# ## Run using the Fabric context
#
# Resolve credentials and the last committed checkpoint through NotebookUtils,
# then land files in the attached lakehouse's driver mount. This entry point
# is used by the refresh pipeline; helper functions remain independently testable.
# The next notebook, `01_bronze`, consumes only the published batch.
# %%
def main(owner, mode, vault_url, secret_name, load_mode):
    """Connect Fabric credentials and files to the extraction helpers."""
    import notebookutils

    token = github_token(mode, vault_url, secret_name, notebookutils)
    inventory = json.loads(notebookutils.fs.head("Files/github/repository_inventory.json", 4194304))
    checkpoint_path = "Files/github/checkpoint.json"
    checkpoint = (
        json.loads(notebookutils.fs.head(checkpoint_path, 4194304))
        if notebookutils.fs.exists(checkpoint_path) else None
    )
    result = ingest(
        inventory, owner, GitHubAPI(token), notebookutils, Path("/lakehouse/default/Files"),
        checkpoint, load_mode,
    )
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main(owner, github_auth_mode, key_vault_url, github_secret_name, load_mode)
