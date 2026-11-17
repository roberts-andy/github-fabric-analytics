# %% [markdown]
# ## Discover the repository inventory
#
# **Position in the refresh:** discovery -> API landing -> Bronze -> Silver ->
# Gold -> semantic-model refresh.
#
# This notebook defines the scope of the whole run: public repositories currently
# owned by the `owner` parameter, excluding forks and including archived projects.
# It does not retrieve activity history or access private repositories.
#
# **Outputs:** `bronze.repository_registry`, an immutable inventory file for this
# discovery run, and `Files/github/repository_inventory.json` for the next notebook.
# The attached schema-enabled lakehouse must already contain the Bronze schema.
#
# ## Read every public repository page before changing the registry
#
# Discovery deliberately uses anonymous public access. A partial response, API
# error, duplicate ID, or empty eligible inventory stops the run instead of
# publishing an inventory that could incorrectly remove repositories downstream.
# %%
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{0,38}", owner):
    raise ValueError("Invalid GitHub owner login")
run_id = uuid.uuid4().hex
url = (
    f"https://api.github.com/users/{urllib.parse.quote(owner)}/repos"
    "?type=owner&per_page=100&sort=full_name"
)
# Repository IDs remain stable across renames; display names are descriptive attributes.
repositories = []
seen_pages = set()
while url:
    if url in seen_pages or not url.startswith("https://api.github.com/"):
        raise ValueError("Invalid or cyclic GitHub pagination link")
    seen_pages.add(url)
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "github-fabric-analytics-repository-discovery",
    })
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.load(response)
            link = response.headers.get("Link", "")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Public GitHub discovery failed: HTTP {error.code}; "
            f"rate limit resets at Unix time {error.headers.get('X-RateLimit-Reset')}; "
            f"{detail}. The previous inventory has not been replaced."
        ) from error
    if not isinstance(page, list):
        raise ValueError("Expected a GitHub repository page")
    repositories.extend(page)
    next_links = re.findall(r'<([^>]+)>;\s*rel="next"', link)
    if len(next_links) > 1:
        raise ValueError("Ambiguous GitHub next-page link")
    url = next_links[0] if next_links else None

# %% [markdown]
# ## Apply eligibility and maintain the registry
#
# A complete owner scan lets us mark missing/ineligible repositories inactive.
# Preserve `first_seen_at` for existing IDs so recurring discovery does not erase
# the date we first observed a repository. Other owners' registry rows are untouched.
# %%
eligible = [
    repository for repository in repositories
    if repository["private"] is False
    and repository["fork"] is False
    and repository["owner"]["login"].lower() == owner.lower()
]
if len({repository["id"] for repository in eligible}) != len(eligible):
    raise ValueError("Duplicate repository IDs in GitHub inventory")
if not eligible:
    raise ValueError("No public non-fork repositories found; previous inventory retained")
records = [
    (
        repository["id"], repository["full_name"], repository["owner"]["login"],
        repository["default_branch"], repository["archived"],
        repository["size"], run_id,
    )
    for repository in eligible
]
current = (
    spark.createDataFrame(
        records,
        "repository_id LONG, full_name STRING, owner_login STRING, "
        "default_branch STRING, is_archived BOOLEAN, size_kb LONG, discovery_run_id STRING",
    )
    .withColumn("first_seen_at", F.current_timestamp())
    .withColumn("last_seen_at", F.current_timestamp())
    .withColumn("is_active", F.lit(True))
    .cache()
)
# Materialize the cached frame once so the merge shares one set of observation times.
current.count()
if not spark.catalog.tableExists("bronze.repository_registry"):
    current.write.format("delta").saveAsTable("bronze.repository_registry")
else:
    updates = {
        column: f"source.{column}" for column in current.columns if column != "first_seen_at"
    }
    (
        DeltaTable.forName(spark, "bronze.repository_registry").alias("target")
        .merge(current.alias("source"), "target.repository_id = source.repository_id")
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(
            condition=F.lower(F.col("target.owner_login")) == F.lit(owner.lower()),
            set={"is_active": "false"},
        )
        .execute()
    )
# %% [markdown]
# ## Check coverage and publish the inventory
#
# The active registry count must agree with the completed GitHub scan. The
# run-specific file records what was discovered; the shared inventory pointer
# tells `00_ingest_github` which repositories to fetch on this refresh.
# %%
active = spark.table("bronze.repository_registry").where(
    F.col("is_active") & (F.lower("owner_login") == owner.lower())
)
if active.count() != len(eligible):
    raise ValueError("Registry coverage differs from the completed GitHub discovery")
inventory = {
    "owner": owner,
    "discovery_run_id": run_id,
    "repository_count": len(eligible),
    "filter": "public, owned, non-fork; includes archived repositories",
    "repositories": [
        {"id": repository["id"], "full_name": repository["full_name"]}
        for repository in sorted(eligible, key=lambda value: value["full_name"].lower())
    ],
}
notebookutils.fs.mkdirs("Files/github/inventory")
notebookutils.fs.put(f"Files/github/inventory/{run_id}.json", json.dumps(inventory), True)
notebookutils.fs.put("Files/github/repository_inventory.json", json.dumps(inventory), True)
active.select("full_name", "is_archived", "first_seen_at", "last_seen_at").orderBy(
    "full_name"
).show(len(eligible), truncate=False)
print(json.dumps(inventory))
current.unpersist()
