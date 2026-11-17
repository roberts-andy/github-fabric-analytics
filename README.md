# GitHub activity analytics in Microsoft Fabric

A working project for turning an owner's public, non-fork GitHub repositories into
Power BI-ready data. One Fabric workspace contains one **schema-enabled**
lakehouse, a repository discovery notebook, three transformation notebooks,
and a Direct Lake semantic model.

```mermaid
flowchart LR
    GitHub["GitHub REST API"] --> Registry["Public, owned, non-fork repository inventory"]
    Registry --> Landing["OneLake Files: immutable JSON batches"]
    Landing --> Bronze["bronze: raw JSON + ingestion metadata"]
    Bronze --> Silver["silver: typed, validated current state"]
    Silver --> Gold["gold: dimensions + activity facts"]
    Bronze --> Snapshots["gold: daily repository observations"]
    Gold --> Model["Power BI Direct Lake semantic model"]
    Snapshots --> Model
```

## Data assets

| Layer | Tables | Grain and behavior |
|---|---|---|
| Bronze | `repository_registry` | One row per discovered repository ID; preserves first/last seen times and marks no-longer-eligible repositories inactive |
| Bronze | `github_raw`, `ingestion_batches` | Original response objects and completed batch manifests; append-only with idempotent batch keys |
| Silver | `repositories`, `commits`, `issues`, `pull_requests`, `completed_batch` | Typed current source snapshot, deduplicated on natural keys, plus a completion gate |
| Gold | `dim_repository`, `dim_actor`, `dim_date` | Conformed repository, GitHub login, contiguous UTC date dimensions |
| Gold | `fact_commits`, `fact_issues`, `fact_pull_requests` | One row per commit, issue, or pull request |
| Gold | `fact_repository_daily` | Last observed stars/forks per repository per UTC day |

`silver.quality_rejects` is created only if invalid source records are encountered;
the run fails visibly rather than silently dropping them. Gold checks uniqueness,
referential integrity, source reconciliation, and persisted row counts.

The source is every **public, non-fork repository owned by `roberts-andy`**, including
archived repositories. Repositories merely collaborated on, private repositories,
and forks are excluded. GitHub does not expose a reliable original-creator field:
the operational definition is current ownership plus `fork = false`.
The inventory drives repository metadata, all default-branch commits, all accessible
issues, and all pull requests. Commit keys include repository ID, because even
non-fork repositories can share commit SHAs. Issues and PR numbers are likewise
scoped to their repository. The issues endpoint also returns PRs; Silver separates
them to avoid double counting. Empty repositories and empty issue/PR sources are valid.

## Run the project

Prerequisites: Python 3.10+, GitHub CLI signed in with access to the source repository,
Azure CLI signed in to the Fabric tenant, and an active Fabric capacity. No credentials
or tenant-specific deployment IDs are stored in the repository.

```powershell
python -m pip install -r requirements-dev.txt
az login
gh auth login
az rest --method get --resource https://api.fabric.microsoft.com --url https://api.fabric.microsoft.com/v1/capacities --headers x-ms-fabric-skill=e2e-medallion-architecture
python scripts\project.py provision --capacity-id <active-fabric-capacity-id>
python scripts\project.py bootstrap
python scripts\project.py deploy
python scripts\project.py discover
python scripts\project.py stage
python scripts\project.py run
python scripts\project.py model
```

Edit `config.json` before provisioning another environment. Deployment IDs, job IDs,
and notebook packages are stored locally under ignored `.local`. `bootstrap` creates
the medallion schemas through Livy and releases the session. `deploy` validates
Jupyter structure and binds all notebooks to the lakehouse before uploading.

Subsequent data refreshes:

```powershell
python scripts\project.py discover
python scripts\project.py stage
python scripts\project.py run
python scripts\project.py status
```

### Rescan repositories from Fabric

Execute **`00_discover_repositories`** in the Fabric workspace to rescan the public
repository list, add new eligible repositories to `bronze.repository_registry`,
preserve their first-seen timestamps, update renamed repositories by stable GitHub
ID, and deactivate entries no longer returned by the public non-fork owner scan.
Its `owner` parameter defaults to the owner in `config.json`.

The notebook follows every public API page and only publishes the inventory after
a complete successful scan. It uses anonymous HTTPS for this public metadata scan
(normally a few requests; GitHub's anonymous limit is typically 60/hour).
Rate-limit or API errors fail visibly and retain the previous published inventory.
Private repositories are never enumerated through the authenticated `/user/repos`
endpoint. This notebook discovers repositories; it does not fetch commit/issue
history or refresh the semantic model itself.

`python scripts\project.py discover` executes that same Fabric notebook remotely.
The `stage` command reads its published OneLake inventory and uses the GitHub CLI's
existing OAuth session to fetch every activity API page for **each** eligible
repository. It rechecks visibility, ownership, and fork status before fetching.
It uploads to OneLake using the Azure CLI's Storage-audience token. The latest-batch pointer is
published only after every repository, endpoint, and file succeeds; aggregate and
per-repository manifest counts gate downstream processing. Spark reads landed files;
it never reads an arbitrary HTTPS URL as a Spark data source.

Discovery changes take effect at the next `stage` and layer run. The default `run`
executes only Bronze, Silver, and Gold; discovery is an explicit separate operation
so it cannot change the inventory halfway through a batch.

Run discovery, staging, and transformations sequentially; concurrent refreshes are
not supported. An interrupted notebook run can be monitored by its saved job ID.
Do not blindly retry a job submission after a timeout.
To intentionally rerun only a failed layer after correcting it:

```powershell
python scripts\project.py run --notebook 02_silver --notebook 03_gold
```

## Modeling and dashboard semantics

The semantic model uses Gold-only Direct Lake entity partitions, single-direction
dimension-to-fact relationships, and explicit measures. Date filters apply to commit
author dates and issue/pull-request creation dates; they do not represent historical
backlog. Issue state is its current observed state. Merged pull requests are counted
within their creation-date cohort, not by merge date.

Stars and forks are **snapshots**, not additive event totals. Use latest-observed
measures; never sum daily snapshots across time. GitHub does not provide prior
stars/forks history through these endpoints, so history starts at the first capture.
Unlinked/deleted GitHub users use the `unlinked` member and are excluded from the
identified commit-author count. No issue, review, or delivery activity is fabricated.

Suggested report visuals: commit trend by Date, current issue state, pull-request
merge rate, average hours to merge, latest stars/forks, and source freshness. Report
visuals and a pinned Power BI dashboard are a separate step from this project's
initial four-stage data/model setup.

## Scope and operations

This is a small-estate proof of concept: each ingestion retrieves a full API
snapshot; Bronze retains history while Silver and Gold replace small, unpartitioned
current-state tables. This also handles source deletions and rebases correctly.
Each run fetches all default-branch commit history and the current state of all
issues/PRs, not just newly changed activity. It does not collect every branch,
comments, reviews, deleted commits, or historical issue/PR transitions.
Repositories that leave the eligible inventory are removed from the current Gold
model, including their repository snapshots; their raw capture history remains in Bronze.
For larger repositories, add tested incremental watermarks, reconciliation of
deletions, partition-aware writes, and a separately approved refresh schedule.
No recurring schedule is enabled and no paid capacity is started automatically.

Raw public GitHub payloads can contain contributor profile details and commit emails.
They remain in the lakehouse; they are not checked into Git. Gold keeps only the
GitHub login required for author analysis. Restrict workspace access appropriately
and define retention before production use. Switching to private repositories
requires explicit review of GitHub permissions and the Fabric sharing boundary.

The semantic model was authored and validated using the Fabric semantic-model
authoring skill and Power BI modeling MCP. `semantic_model\model.bim` is its
exported TMSL snapshot, with deployment IDs and generated lineage metadata removed
and OneLake workspace/lakehouse tokens parameterized. The `model` command deploys
that source snapshot through the Fabric definition API; it will not overwrite an
existing model unless `--replace` is supplied. Replacement is deliberate and can
affect downstream report bindings. Use modeling tools for routine live edits.
After a data refresh or model deployment, refresh/frame the semantic model using
Power BI Service or the modeling MCP, and validate measures before sharing reports.

Local checks:

```powershell
python -m unittest discover -s tests
```
