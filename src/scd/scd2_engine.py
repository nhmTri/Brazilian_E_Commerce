"""
SCD Type 2 Engine — reusable cho mọi dim table.

Thay vì copy paste apply_scd2() vào mỗi notebook,
import từ đây để maintain 1 chỗ duy nhất.

Usage:
    from src.scd.scd2_engine import SCD2Engine
    engine = SCD2Engine(spark, "olist_ecommerce")
    result = engine.apply(
        source_df     = dim_customer_src,
        target_table  = "olist_ecommerce.gold.dim_customer",
        natural_key   = "customer_unique_id",
        tracked_cols  = ["customer_city", "customer_state"],
        surrogate_col = "customer_key"
    )
    print(result)
    # {"mode": "incremental", "inserted": 5, "updated": 0}
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, lit, expr
)
from delta.tables import DeltaTable
from typing import List, Dict


class SCD2Engine:
    """
    Production-grade SCD Type 2 implementation
    using Delta Lake MERGE INTO.

    Patterns implemented:
    - Initial load: overwrite với metadata columns
    - Incremental: expire old → insert new versions
    - Idempotent: chạy lại N lần = same result
    """

    def __init__(self, spark: SparkSession,
                 catalog: str):
        self.spark   = spark
        self.catalog = catalog

    def _add_scd2_metadata(
        self,
        df: DataFrame,
        surrogate_col: str
    ) -> DataFrame:
        """Thêm SCD2 metadata columns vào source DataFrame."""
        return (df
            .withColumn(surrogate_col,
                        expr("uuid()"))
            .withColumn("valid_from",
                        current_timestamp())
            .withColumn("valid_to",
                        lit("9999-12-31 00:00:00")
                        .cast("timestamp"))
            .withColumn("is_current",
                        lit(True))
            .withColumn("_updated_at",
                        current_timestamp()))

    def _build_change_condition(
        self,
        tracked_cols: List[str]
    ) -> str:
        """Build SQL condition để detect thay đổi."""
        conditions = []
        for c in tracked_cols:
            conditions.append(
                f"target.{c} != source.{c} OR "
                f"(target.{c} IS NULL "
                f"AND source.{c} IS NOT NULL)"
            )
        return " OR ".join(conditions)

    def apply(
        self,
        source_df:     DataFrame,
        target_table:  str,
        natural_key:   str,
        tracked_cols:  List[str],
        surrogate_col: str
    ) -> Dict:
        """
        Apply SCD Type 2 MERGE INTO.

        Args:
            source_df:     DataFrame với data mới nhất
            target_table:  Full table name (catalog.schema.table)
            natural_key:   Business key column name
            tracked_cols:  Columns cần track history
            surrogate_col: Surrogate key column name

        Returns:
            Dict với metrics: mode, inserted, unchanged
        """
        source = self._add_scd2_metadata(
            source_df, surrogate_col)

        # ── Initial load ──────────────────────────────────
        if not DeltaTable.isDeltaTable(
                self.spark, target_table):
            (source.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table))

            count = (self.spark.read
                     .table(target_table).count())
            print(
                f"[SCD2] Initial load {target_table}: "
                f"{count:,} records"
            )
            return {
                "mode":      "initial_load",
                "inserted":  count,
                "updated":   0,
                "unchanged": 0,
            }

        # ── Incremental MERGE ─────────────────────────────
        change_cond = self._build_change_condition(
            tracked_cols)
        delta_tbl   = DeltaTable.forName(
            self.spark, target_table)

        # Step 1: Expire bản ghi cũ khi có thay đổi
        (delta_tbl.alias("target")
            .merge(
                source.alias("source"),
                f"target.{natural_key} = "
                f"source.{natural_key} "
                f"AND target.is_current = true"
            )
            .whenMatchedUpdate(
                condition=change_cond,
                set={
                    "valid_to":   "current_timestamp()",
                    "is_current": "false",
                }
            )
            .execute())

        # Step 2: Insert version mới
        existing_current = (
            self.spark.read.table(target_table)
            .filter("is_current = true")
            .select(natural_key)
        )
        new_versions = source.join(
            existing_current, natural_key, "left_anti")

        inserted = new_versions.count()
        if inserted > 0:
            (new_versions.write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table))
            print(
                f"[SCD2] {target_table}: "
                f"inserted {inserted:,} new versions"
            )
        else:
            print(
                f"[SCD2] {target_table}: "
                f"no changes detected"
            )

        return {
            "mode":      "incremental",
            "inserted":  inserted,
            "updated":   0,
            "unchanged": source_df.count() - inserted,
        }