"""
Utility functions dùng chung cho toàn bộ pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import List


def assert_no_nulls(df: DataFrame,
                    columns: List[str],
                    table_name: str) -> None:
    """
    Assert không có null trong critical columns.
    Raise ValueError nếu có null.
    """
    for c in columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"[{table_name}] Column '{c}' has "
                f"{null_count:,} null values!"
            )


def get_row_count(spark, table_name: str) -> int:
    """Đếm rows của Delta table."""
    return spark.read.table(table_name).count()


def table_exists(spark, table_name: str) -> bool:
    """Check Delta table tồn tại không."""
    try:
        spark.read.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def print_separator(title: str = "", width: int = 55):
    """In separator line cho dễ đọc log."""
    if title:
        print(f"\n{'─'*width}")
        print(f"  {title}")
        print(f"{'─'*width}")
    else:
        print(f"{'─'*width}")