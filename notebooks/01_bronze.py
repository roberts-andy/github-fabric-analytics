import json
import re

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.vorder.default", "false")

latest = json.loads(notebookutils.fs.head("Files/github/latest.json", 16384))
batch_id = latest["batch_id"]
if not re.fullmatch(r"[a-f0-9]{32}", batch_id):
    raise ValueError("Invalid landing batch ID")
path = f"Files/github/{batch_id}"
manifest = json.loads(notebookutils.fs.head(f"{path}/manifest.json", 16384))
if manifest != latest:
    raise ValueError("Latest batch pointer does not match the immutable batch manifest")

raw = (
    spark.read.schema(
        "entity STRING, record_id STRING, payload STRING, source_url STRING, "
        "batch_id STRING, ingested_at TIMESTAMP"
    )
    .json(f"{path}/records.jsonl")
    .withColumn("source_file", F.lit(f"{path}/records.jsonl"))
    .withColumn("ingestion_date", F.to_date("ingested_at"))
)
required = ["entity", "record_id", "payload", "source_url", "batch_id", "ingested_at"]
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
if counts.get("repositories") != 1 or sum(counts.values()) != manifest["record_count"]:
    raise ValueError("A batch must contain exactly one repository and the declared row count")
if raw.count() != raw.select("batch_id", "entity", "record_id").distinct().count():
    raise ValueError("Duplicate natural keys within a source batch")

if not spark.catalog.tableExists("bronze.github_raw"):
    raw.write.format("delta").partitionBy("ingestion_date").saveAsTable("bronze.github_raw")
else:
    (
        DeltaTable.forName(spark, "bronze.github_raw").alias("target")
        .merge(
            raw.alias("source"),
            "target.batch_id = source.batch_id AND target.entity = source.entity "
            "AND target.record_id = source.record_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
batch = (
    spark.createDataFrame(
        [(batch_id, manifest["repository"], manifest["ingested_at"],
          manifest["record_count"], json.dumps(expected))],
        "batch_id STRING, repository STRING, ingested_at STRING, "
        "record_count LONG, entity_counts STRING",
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
