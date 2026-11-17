# %% [markdown]
# ## Bronze: preserve the captured source and its provenance
#
# **Input:** the batch selected by `Files/github/latest.json`, including its
# immutable manifest and `records.jsonl` file.
# **Outputs:** append-only `bronze.github_raw` and `bronze.ingestion_batches`.
#
# Bronze keeps GitHub payloads as JSON strings rather than imposing business
# schemas. This preserves the source for replay and allows Silver rules to evolve.
# A repeated batch is insert-only and idempotent on its repository-scoped keys.
#
# ## Validate the landing contract before writing
#
# Check the pointer against the immutable manifest, then reconcile total,
# per-entity and per-repository counts. An incremental run can legitimately have
# zero activity rows, but must still contain metadata for every declared repository.
# %%
import json
import re

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
# Bronze is optimized for landing and replay; downstream layers enable V-Order for reads.
spark.conf.set("spark.sql.parquet.vorder.default", "false")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

latest = json.loads(notebookutils.fs.head("Files/github/latest.json", 16384))
batch_id = latest["batch_id"]
if not re.fullmatch(r"[a-f0-9]{32}", batch_id):
    raise ValueError("Invalid landing batch ID")
path = f"Files/github/{batch_id}"
manifest = json.loads(notebookutils.fs.head(f"{path}/manifest.json", 16384))
if manifest != latest:
    raise ValueError("Latest batch pointer does not match the immutable batch manifest")

# An explicit envelope schema avoids type inference changing IDs or timestamps between batches.
raw = (
    spark.read.schema(
        "repository_id LONG, entity STRING, record_id STRING, payload STRING, source_url STRING, "
        "batch_id STRING, ingested_at TIMESTAMP"
    )
    .json(f"{path}/records.jsonl")
    .withColumn("source_file", F.lit(f"{path}/records.jsonl"))
    .withColumn("ingestion_date", F.to_date("ingested_at"))
)
required = [
    "repository_id", "entity", "record_id", "payload", "source_url", "batch_id", "ingested_at"
]
invalid = raw.filter(
    F.expr(" OR ".join(f"{name} IS NULL" for name in required))
    | (F.col("batch_id") != batch_id)
)
if invalid.limit(1).count():
    raise ValueError("Landing records have missing metadata or inconsistent batch IDs")
counts = {row.entity: row["count"] for row in raw.groupBy("entity").count().collect()}
expected = manifest["counts"]
if set(counts) - set(expected) or any(
    counts.get(entity, 0) != count for entity, count in expected.items()
):
    raise ValueError(f"Landing count mismatch: actual={counts}, expected={expected}")
if counts.get("repositories") != manifest["repository_count"] or sum(
    counts.values()
) != manifest["record_count"]:
    raise ValueError("The batch does not contain every declared repository and source row")
repository_ids = {repository["id"] for repository in manifest["repositories"]}
actual_repository_ids = {
    row.repository_id for row in raw.where("entity = 'repositories'").select(
        "repository_id"
    ).collect()
}
if len(repository_ids) != manifest["repository_count"] or actual_repository_ids != repository_ids:
    raise ValueError("Repository coverage differs from the inventory manifest")
grouped = {
    (str(row.repository_id), row.entity): row["count"]
    for row in raw.groupBy("repository_id", "entity").count().collect()
}
for repository_id, per_repository in manifest["repository_counts"].items():
    for entity, count in per_repository.items():
        if grouped.get((repository_id, entity), 0) != count:
            raise ValueError(f"Incomplete source {entity} for repository {repository_id}")
if any(int(repository_id) not in repository_ids for repository_id, _ in grouped):
    raise ValueError("Landing rows include a repository outside the discovery inventory")
if raw.where(
    (F.col("entity") == "repositories")
    & ~F.col("repository_id").eqNullSafe(
        F.get_json_object("payload", "$.id").cast("long")
    )
).limit(1).count():
    raise ValueError("Repository payload ID does not match its provenance")
if raw.count() != raw.select(
    "batch_id", "repository_id", "entity", "record_id"
).distinct().count():
    raise ValueError("Duplicate natural keys within a source batch")

# %% [markdown]
# ## Persist raw records without rewriting earlier batches
#
# The merge key includes batch, repository, entity and record ID. It prevents a
# retry from duplicating a capture while retaining the same source item observed
# in a later batch. Repository scope also keeps shared commit SHAs distinct.
# %%
if not spark.catalog.tableExists("bronze.github_raw"):
    raw.write.format("delta").partitionBy("ingestion_date").saveAsTable("bronze.github_raw")
else:
    if "repository_id" not in spark.table("bronze.github_raw").columns:
        # Add provenance without rewriting the original single-repository captures.
        raw.limit(0).write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable("bronze.github_raw")
    (
        DeltaTable.forName(spark, "bronze.github_raw").alias("target")
        .merge(
            raw.alias("source"),
            "target.batch_id = source.batch_id AND target.entity = source.entity "
            "AND target.repository_id = source.repository_id "
            "AND target.record_id = source.record_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
# %% [markdown]
# ## Record the batch and check the persisted result
#
# Register the manifest metadata only after the raw merge succeeds. Silver uses
# this batch record together with the raw-row count as its readiness gate; the
# batch table alone is not a transaction covering all downstream layers.
# %%
batch = (
    spark.createDataFrame(
        [(batch_id, None, manifest["ingested_at"], manifest["record_count"],
          json.dumps(expected), manifest["owner"], manifest["repository_count"],
          json.dumps(sorted(repository_ids)))],
        "batch_id STRING, repository STRING, ingested_at STRING, "
        "record_count LONG, entity_counts STRING, owner STRING, "
        "repository_count LONG, repository_ids STRING",
    )
    .withColumn("ingested_at", F.to_timestamp("ingested_at"))
)
if not spark.catalog.tableExists("bronze.ingestion_batches"):
    batch.write.format("delta").saveAsTable("bronze.ingestion_batches")
else:
    (
        DeltaTable.forName(spark, "bronze.ingestion_batches").alias("target")
        .merge(batch.alias("source"), "target.batch_id = source.batch_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
actual = spark.table("bronze.github_raw").where(F.col("batch_id") == batch_id).count()
if actual != manifest["record_count"]:
    raise ValueError(f"Bronze persisted count mismatch: {actual}")
print(json.dumps({"layer": "bronze", "batch_id": batch_id, "rows": actual, "counts": counts}))
