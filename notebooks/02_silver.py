# %% [markdown]
# ## Silver: curate the latest known state of each GitHub item
#
# **Inputs:** the published landing manifest, its completed Bronze batch, and
# existing Silver history. **Outputs:** typed repository, commit, issue and PR
# tables plus `silver.completed_batch` after every entity succeeds.
#
# Incremental batches contain only changes, not complete history. Merge them
# into existing state; never replace the whole table with the current delta.
# Explicit full loads can reconcile deletions for the affected repository/entity.
#
# ## Gate on Bronze and parse the raw JSON
#
# Fixed schemas select the fields this model needs while Bronze retains the full
# payload. Source repository IDs travel with each object because item numbers
# and commit SHAs must be interpreted within a repository.
# %%
import json

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.vorder.default", "true")
latest = json.loads(notebookutils.fs.head("Files/github/latest.json", 16384))
batch_id = latest["batch_id"]
batches = spark.table("bronze.ingestion_batches").filter(F.col("batch_id") == batch_id)
if batches.count() != 1:
    raise ValueError("The published landing batch has not completed Bronze")
raw = spark.table("bronze.github_raw").where(F.col("batch_id") == batch_id)
if raw.count() != latest["record_count"]:
    raise ValueError("Incomplete Bronze batch")

schemas = {
    "repositories": (
        "id LONG, full_name STRING, html_url STRING, description STRING, "
        "default_branch STRING, language STRING, created_at TIMESTAMP, "
        "updated_at TIMESTAMP, stargazers_count LONG, forks_count LONG"
    ),
    "commits": (
        "sha STRING, html_url STRING, author STRUCT<login:STRING>, "
        "commit STRUCT<message:STRING,author:STRUCT<date:TIMESTAMP>>"
    ),
    "issues": (
        "id LONG, number LONG, title STRING, state STRING, created_at TIMESTAMP, "
        "updated_at TIMESTAMP, closed_at TIMESTAMP, user STRUCT<login:STRING>, "
        "pull_request STRUCT<url:STRING>"
    ),
    "pull_requests": (
        "id LONG, number LONG, title STRING, state STRING, draft BOOLEAN, "
        "created_at TIMESTAMP, updated_at TIMESTAMP, closed_at TIMESTAMP, "
        "merged_at TIMESTAMP, user STRUCT<login:STRING>"
    ),
}


def parse(entity):
    """Parse one entity's payload and retain repository and batch provenance."""
    return (
        raw.where(F.col("entity") == entity)
        .withColumn("data", F.from_json("payload", schemas[entity]))
        .select(
            "data.*", F.col("repository_id").alias("source_repository_id"),
            "batch_id", "ingested_at", "payload", "source_url",
        )
    )


# %% [markdown]
# ## Normalize entity-specific fields
#
# Separate true issues from PR markers returned by GitHub's issues endpoint.
# Use `unlinked` for activity without a linked GitHub account rather than losing
# those records in later dimension joins. Keep bots; this is activity reporting,
# not an inference about individual developer productivity.
# %%
repository = parse("repositories")
if repository.count() != latest["repository_count"]:
    raise ValueError("Silver source does not cover the repository inventory")

frames = {
    "repositories": repository.select(
        F.col("id").alias("repository_id"), "full_name", "html_url", "description",
        "default_branch", "language", "created_at", "updated_at",
        F.col("stargazers_count").alias("stars"), F.col("forks_count").alias("forks"),
        "batch_id", "ingested_at",
    ),
    "commits": parse("commits").select(
        F.col("sha").alias("commit_sha"),
        F.col("source_repository_id").alias("repository_id"),
        F.coalesce(F.col("author.login"), F.lit("unlinked")).alias("actor_login"),
        F.col("commit.author.date").alias("created_at"),
        F.col("commit.message").alias("message"), "html_url", "batch_id", "ingested_at",
    ),
}
for entity in ("issues", "pull_requests"):
    parsed = parse(entity)
    if entity == "issues":
        # GitHub's issues endpoint returns both issues and pull requests.
        parsed = parsed.where(F.col("pull_request").isNull())
    columns = [
        F.col("id").alias("item_id"),
        F.col("source_repository_id").alias("repository_id"),
        "number", "title", "state",
        F.coalesce(F.col("user.login"), F.lit("unlinked")).alias("actor_login"),
        "created_at", "updated_at", "closed_at",
    ]
    if entity == "pull_requests":
        columns += ["merged_at", "draft"]
    frames[entity] = parsed.select(*columns, "batch_id", "ingested_at")

# %% [markdown]
# ## Validate, deduplicate and merge according to the load plan
#
# Reject invalid records into `silver.quality_rejects` and fail visibly; do not
# quietly publish incomplete counts. Deduplicate by repository-scoped natural
# keys, preferring the newest source update/capture.
#
# Incremental merges retain unseen keys and prevent an older overlapping record
# from replacing newer state. Delete unseen rows only for explicit full
# reconciliations or repositories removed from the current inventory. A missing
# Silver table requires a full bootstrap, not an incremental-only seed.
# %%
rules = {
    "repositories": "repository_id IS NOT NULL AND full_name IS NOT NULL "
                    "AND stars >= 0 AND forks >= 0 AND created_at IS NOT NULL",
    "commits": "repository_id IS NOT NULL AND "
               "commit_sha RLIKE '^[a-f0-9]{40,64}$' AND created_at IS NOT NULL",
    "issues": "repository_id IS NOT NULL AND item_id IS NOT NULL "
              "AND number > 0 AND state IN ('open','closed') "
              "AND created_at IS NOT NULL AND updated_at >= created_at "
              "AND (closed_at IS NULL OR closed_at >= created_at)",
    "pull_requests": "repository_id IS NOT NULL AND item_id IS NOT NULL "
                     "AND number > 0 AND state IN ('open','closed') "
                     "AND created_at IS NOT NULL AND updated_at >= created_at "
                     "AND (closed_at IS NULL OR closed_at >= created_at) "
                     "AND (merged_at IS NULL OR merged_at >= created_at)",
}
key_columns = {
    "repositories": ["repository_id"], "commits": ["repository_id", "commit_sha"],
    "issues": ["repository_id", "item_id"],
    "pull_requests": ["repository_id", "item_id"],
}
repository_ids = [r["id"] for r in latest["repositories"]]
# Older locally staged manifests represent full snapshots and have no per-entity load plan.
load_plan = latest.get("repository_loads", {
    str(repository_id): dict.fromkeys(schemas, "full") for repository_id in repository_ids
})
if set(load_plan) != {str(repository_id) for repository_id in repository_ids}:
    raise ValueError("Incremental load plan does not match the repository inventory")
if any(set(plan) != set(schemas) or set(plan.values()) - {"full", "incremental"}
       for plan in load_plan.values()):
    raise ValueError("Invalid repository/entity load mode")
metrics = {}
for entity, frame in frames.items():
    # SQL NULL is not a quality pass; turn unknown rule results into explicit rejects.
    condition = F.coalesce(F.expr(rules[entity]), F.lit(False))
    rejected = frame.where(~condition)
    rejected_count = rejected.count()
    if rejected_count:
        (
            rejected.withColumn("entity", F.lit(entity))
            .withColumn("rule", F.lit(rules[entity]))
            .select(
                "entity", "batch_id", "ingested_at", "rule",
                F.to_json(F.struct(*frame.columns)).alias("record"),
            )
            .write.format("delta").mode("append").saveAsTable("silver.quality_rejects")
        )
        raise ValueError(f"{entity}: {rejected_count} invalid rows; see silver.quality_rejects")
    sort = [F.col("updated_at").desc()] if "updated_at" in frame.columns else []
    window = Window.partitionBy(*key_columns[entity]).orderBy(
        *sort, F.col("ingested_at").desc()
    )
    cleaned = (
        frame.withColumn("_row", F.row_number().over(window)).where("_row = 1").drop("_row")
    )
    full_ids = [int(key) for key, plan in load_plan.items() if plan[entity] == "full"]
    table = f"silver.{entity}"
    if spark.catalog.tableExists(table):
        retained = spark.table(table).where(
            F.col("repository_id").isin(repository_ids) & ~F.col("repository_id").isin(full_ids)
        )
        expected = retained.select(*key_columns[entity]).union(
            cleaned.select(*key_columns[entity])
        ).distinct().count()
        match = " AND ".join(f"target.{key} = source.{key}" for key in key_columns[entity])
        freshness = "updated_at" if "updated_at" in cleaned.columns else "ingested_at"
        (
            DeltaTable.forName(spark, table).alias("target")
            .merge(cleaned.alias("source"), match)
            .whenMatchedUpdateAll(condition=(
                F.col("source.repository_id").isin(full_ids)
                | (F.col(f"source.{freshness}") >= F.col(f"target.{freshness}"))
            ))
            .whenNotMatchedInsertAll()
            # Absence from a delta means unchanged, not deleted.
            .whenNotMatchedBySourceDelete(condition=(
                ~F.col("target.repository_id").isin(repository_ids)
                | F.col("target.repository_id").isin(full_ids)
            ))
            .execute()
        )
    else:
        if set(full_ids) != set(repository_ids):
            raise ValueError(f"{table} is missing; run a full bootstrap before incremental loads")
        expected = cleaned.count()
        cleaned.write.format("delta").saveAsTable(table)
    spark.sql(f"OPTIMIZE {table}")
    actual = spark.table(table).count()
    if actual != expected:
        raise ValueError(f"Silver {entity} persisted {actual}; expected {expected}")
    metrics[entity] = {"rows": actual, "rejected": rejected_count}
# %% [markdown]
# ## Publish the Silver completion marker
#
# Gold must see one marker for this batch and matching persisted table counts.
# This marker is written last because entity merges are separate Delta
# transactions. Retained rows keep their own originating batch IDs; they do not
# all need to be rewritten just to certify that this refresh completed.
# %%
spark.createDataFrame(
    [(batch_id, json.dumps(metrics))], "batch_id STRING, table_metrics STRING"
).write.format("delta").mode("overwrite").saveAsTable("silver.completed_batch")
print(json.dumps({"layer": "silver", "batch_id": batch_id, "tables": metrics}))
