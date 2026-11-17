import json

import notebookutils
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
    return (
        raw.where(F.col("entity") == entity)
        .withColumn("data", F.from_json("payload", schemas[entity]))
        .select("data.*", "batch_id", "ingested_at", "payload", "source_url")
    )


repository = parse("repositories")
repository_id = repository.select("id").first()["id"]
if repository_id is None:
    raise ValueError("Repository ID is missing")

frames = {
    "repositories": repository.select(
        F.col("id").alias("repository_id"), "full_name", "html_url", "description",
        "default_branch", "language", "created_at", "updated_at",
        F.col("stargazers_count").alias("stars"), F.col("forks_count").alias("forks"),
        "batch_id", "ingested_at",
    ),
    "commits": parse("commits").select(
        F.col("sha").alias("commit_sha"), F.lit(repository_id).alias("repository_id"),
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
        F.col("id").alias("item_id"), F.lit(repository_id).alias("repository_id"),
        "number", "title", "state",
        F.coalesce(F.col("user.login"), F.lit("unlinked")).alias("actor_login"),
        "created_at", "updated_at", "closed_at",
    ]
    if entity == "pull_requests":
        columns += ["merged_at", "draft"]
    frames[entity] = parsed.select(*columns, "batch_id", "ingested_at")

rules = {
    "repositories": "repository_id IS NOT NULL AND full_name IS NOT NULL "
                    "AND stars >= 0 AND forks >= 0 AND created_at IS NOT NULL",
    "commits": "commit_sha RLIKE '^[a-f0-9]{40,64}$' AND created_at IS NOT NULL",
    "issues": "item_id IS NOT NULL AND number > 0 AND state IN ('open','closed') "
              "AND created_at IS NOT NULL AND updated_at >= created_at "
              "AND (closed_at IS NULL OR closed_at >= created_at)",
    "pull_requests": "item_id IS NOT NULL AND number > 0 AND state IN ('open','closed') "
                     "AND created_at IS NOT NULL AND updated_at >= created_at "
                     "AND (closed_at IS NULL OR closed_at >= created_at) "
                     "AND (merged_at IS NULL OR merged_at >= created_at)",
}
key_columns = {
    "repositories": "repository_id", "commits": "commit_sha",
    "issues": "item_id", "pull_requests": "item_id",
}
metrics = {}
for entity, frame in frames.items():
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
    window = Window.partitionBy(key_columns[entity]).orderBy(
        *sort, F.col("ingested_at").desc()
    )
    cleaned = (
        frame.withColumn("_row", F.row_number().over(window)).where("_row = 1").drop("_row")
    )
    # This small-repository POC replaces the complete current snapshot, including
    # valid empty tables; Bronze retains every historical capture.
    cleaned.write.format("delta").mode("overwrite").saveAsTable(f"silver.{entity}")
    spark.sql(f"OPTIMIZE silver.{entity}")
    actual = spark.table(f"silver.{entity}").count()
    expected = cleaned.count()
    if actual != expected:
        raise ValueError(f"Silver {entity} persisted {actual}; expected {expected}")
    metrics[entity] = {"rows": actual, "rejected": rejected_count}
spark.createDataFrame(
    [(batch_id, json.dumps(metrics))], "batch_id STRING, table_metrics STRING"
).write.format("delta").mode("overwrite").saveAsTable("silver.completed_batch")
print(json.dumps({"layer": "silver", "batch_id": batch_id, "tables": metrics}))
