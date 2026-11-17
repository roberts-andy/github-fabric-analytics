# %% [markdown]
# ## Gold: build the reporting star schema
#
# **Inputs:** the complete current Silver state and its completion marker, plus
# Bronze repository observations for stars/forks.
# **Outputs:** three dimensions, four fact tables and
# `Files/github/validation_gold.json` for the semantic-model refresh gate.
#
# Gold deliberately rebuilds these small tables in full even when extraction and
# Silver are incremental. Reading complete Silver state preserves historical
# activity and keeps the model's dimensions and facts consistent.
#
# ## Confirm Silver completed before shaping the model
#
# Check the completion marker and recorded row counts, not every row's batch ID:
# an unchanged Silver row correctly retains an older source batch.
# %%
import json

import notebookutils
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "1g")

latest = json.loads(notebookutils.fs.head("Files/github/latest.json", 16384))
batch_id = latest["batch_id"]
completion = spark.table("silver.completed_batch").collect()
if len(completion) != 1 or completion[0]["batch_id"] != batch_id:
    raise ValueError("The published batch has not completed all Silver transformations")
silver_metrics = json.loads(completion[0]["table_metrics"])
sources = {}
for entity in ("repositories", "commits", "issues", "pull_requests"):
    sources[entity] = spark.table(f"silver.{entity}")
    if sources[entity].count() != silver_metrics[entity]["rows"]:
        raise ValueError(f"Incomplete Silver table: {entity}")
if sources["repositories"].count() != latest["repository_count"]:
    raise ValueError("Gold source does not cover the complete repository inventory")

# %% [markdown]
# ## Build dimensions and activity facts
#
# Repository and actor dimensions provide shared filter paths. The date dimension
# is continuous from the earliest observed creation date to at least one year
# beyond today, so empty days can appear in trends.
#
# Activity facts use commit author date and issue/PR creation date in UTC.
# Close/merge durations describe creation cohorts, not merge-date throughput or
# DORA lead time. Those distinctions matter when interpreting the report.
# %%
repository = sources["repositories"]
tables = {
    "dim_repository": repository.select(
        "repository_id", F.col("full_name").alias("repository_name"),
        "html_url", "description", "default_branch", "language",
    ),
}
actors = sources["commits"].select("actor_login")
for entity in ("issues", "pull_requests"):
    actors = actors.union(sources[entity].select("actor_login"))
# Always provide the fallback dimension member used by unlinked source activity.
tables["dim_actor"] = actors.union(
    spark.createDataFrame([("unlinked",)], "actor_login STRING")
).distinct()

activity_dates = repository.select(F.to_date("created_at").alias("date"))
for entity in ("commits", "issues", "pull_requests"):
    activity_dates = activity_dates.union(
        sources[entity].select(F.to_date("created_at").alias("date"))
    )
date_bounds = activity_dates.agg(
    F.min("date").alias("start"), F.max("date").alias("end")
).first()
start, end = date_bounds["start"], date_bounds["end"]
tables["dim_date"] = (
    spark.sql(
        f"SELECT explode(sequence(DATE '{start}', "
        f"greatest(DATE '{end}', date_add(current_date(), 365)), interval 1 day)) AS date"
    )
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month_number", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("day_of_week", F.date_format("date", "EEEE"))
    .withColumn("day_of_week_number", F.dayofweek("date"))
)
tables["fact_commits"] = sources["commits"].select(
    "repository_id", "actor_login", F.to_date("created_at").alias("date"),
    "commit_sha",
)
for entity in ("issues", "pull_requests"):
    source = sources[entity]
    columns = [
        "repository_id", "actor_login", F.to_date("created_at").alias("date"),
        "number", "title", "state",
        F.to_date("closed_at").alias("closed_date"),
        (
            (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / 3600
        ).cast("decimal(18,4)").alias("hours_to_close"),
    ]
    if entity == "pull_requests":
        columns += [
            "draft", F.col("merged_at").isNotNull().alias("is_merged"),
            F.to_date("merged_at").alias("merged_date"),
            (
                (F.col("merged_at").cast("long") - F.col("created_at").cast("long")) / 3600
            ).cast("decimal(18,4)").alias("hours_to_merge"),
        ]
    tables[f"fact_{entity}"] = source.select(*columns)

# %% [markdown]
# ## Retain daily observations of repository popularity
#
# Stars and forks are snapshots, not historical event counts. Read prior Bronze
# captures and keep the latest observation per repository per UTC day. Twice-daily
# refreshes therefore produce one daily row; do not sum snapshot values over dates.
# %%
snapshots = (
    spark.table("bronze.github_raw").where("entity = 'repositories'")
    .withColumn("data", F.from_json(
        "payload", "id LONG, stargazers_count LONG, forks_count LONG"
    ))
    .select(
        F.col("data.id").alias("repository_id"),
        F.to_date("ingested_at").alias("date"), "ingested_at", "batch_id",
        F.col("data.stargazers_count").alias("stars"),
        F.col("data.forks_count").alias("forks"),
    )
    .join(repository.select("repository_id"), "repository_id", "inner")
)
window = Window.partitionBy("repository_id", "date").orderBy(
    F.col("ingested_at").desc(), F.col("batch_id").desc()
)
tables["fact_repository_daily"] = (
    snapshots.withColumn("_row", F.row_number().over(window))
    .where("_row = 1").drop("_row", "batch_id")
)

# %% [markdown]
# ## Validate the star schema before publishing
#
# Enforce each table's natural-key grain, non-null keys and fact-to-dimension
# referential integrity. Reconcile activity counts back to Silver so a join or
# projection cannot silently drop history.
# %%
keys = {
    "dim_repository": ["repository_id"], "dim_actor": ["actor_login"],
    "dim_date": ["date"], "fact_commits": ["repository_id", "commit_sha"],
    "fact_issues": ["repository_id", "number"],
    "fact_pull_requests": ["repository_id", "number"],
    "fact_repository_daily": ["repository_id", "date"],
}
metrics = {}
for name, frame in tables.items():
    count = frame.count()
    if count != frame.select(*keys[name]).distinct().count():
        raise ValueError(f"Duplicate Gold keys in {name}")
    if frame.where(F.expr(" OR ".join(f"{key} IS NULL" for key in keys[name]))).limit(1).count():
        raise ValueError(f"Null Gold key in {name}")
    if name.startswith("fact_"):
        for key, dimension in [
            ("repository_id", "dim_repository"), ("date", "dim_date"),
            ("actor_login", "dim_actor"),
        ]:
            if key in frame.columns and frame.join(
                tables[dimension].select(key), key, "left_anti"
            ).limit(1).count():
                raise ValueError(f"Orphan {key} in {name}")
    metrics[name] = count
for entity in ("commits", "issues", "pull_requests"):
    if metrics[f"fact_{entity}"] != sources[entity].count():
        raise ValueError(f"Silver-to-Gold reconciliation failed for {entity}")

# %% [markdown]
# ## Write the reporting tables and validation artifact
#
# These small tables are unpartitioned to avoid needless partition-management
# overhead. Delta writes are atomic per table, not across the whole star schema.
# Publish validation only after all persisted counts agree; the next notebook
# refuses to refresh the semantic model without a validation artifact for this batch.
# %%
for name, frame in tables.items():
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"gold.{name}")
    spark.sql(f"OPTIMIZE gold.{name}")
    if spark.table(f"gold.{name}").count() != metrics[name]:
        raise ValueError(f"Persisted count mismatch in gold.{name}")
    print(f"gold.{name}: {metrics[name]} rows")

report = {
    "batch_id": batch_id, "layer": "gold", "tables": metrics,
    "schemas": {
        name: [{"name": field.name, "type": field.dataType.simpleString()}
               for field in frame.schema.fields]
        for name, frame in tables.items()
    },
}
notebookutils.fs.put("Files/github/validation_gold.json", json.dumps(report), True)
print(json.dumps(report))
