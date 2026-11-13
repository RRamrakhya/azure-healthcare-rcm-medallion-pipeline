"""SCD Type 2 on Delta using hash-diff change detection (one comparison instead of N column comparisons).
Idempotent: re-running with the same source adds no rows. Limitation: assumes in-order arrival (no late-arriving
history rewrite); see docs/DESIGN_DECISIONS.md."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def with_hash_diff(df: DataFrame, tracked_cols) -> DataFrame:
    return df.withColumn("hash_diff", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in tracked_cols]), 256))


def scd2_merge(spark: SparkSession, source: DataFrame, target_table: str, key: str, tracked_cols) -> None:
    now = F.current_timestamp()
    src = (with_hash_diff(source, tracked_cols)
           .withColumn("inserted_date", now).withColumn("modified_date", now).withColumn("is_current", F.lit(True)))
    if not spark.catalog.tableExists(target_table):
        src.write.format("delta").saveAsTable(target_table)
        return
    tgt = DeltaTable.forName(spark, target_table)
    cur = spark.table(target_table).where("is_current")
    changed = (src.alias("s").join(cur.alias("t"), key).where("s.hash_diff <> t.hash_diff").select("s.*"))
    # rows with NULL merge key never match -> inserted as the new current version
    staged = changed.withColumn("merge_key", F.lit(None).cast(src.schema[key].dataType)).unionByName(
        src.withColumn("merge_key", F.col(key)))
    cols = [c for c in src.columns]
    (tgt.alias("t").merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
     .whenMatchedUpdate(condition="t.hash_diff <> s.hash_diff",
                        set={"is_current": "false", "modified_date": "s.modified_date"})
     .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
     .execute())
