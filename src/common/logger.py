"""
Structured logger — production standard.
Dùng thay cho print() thông thường.
Mọi notebook import từ đây thay vì define lại.
"""

from datetime import datetime


class PipelineLogger:
    """
    Structured logger với run_id tracking.

    Usage:
        logger = PipelineLogger()
        logger.log("INFO", "silver_orders", "Starting transform",
                   {"input_rows": "99,441"})
        logger.summary()
    """

    ICONS = {
        "INFO":     "ℹ",
        "WARN":     "⚠",
        "CRITICAL": "✖",
        "SUCCESS":  "✔",
        "ERROR":    "✖",
    }

    def __init__(self, run_id: str = None):
        self.run_id  = run_id or datetime.now().strftime(
            "%Y%m%d_%H%M%S")
        self.logs    = []
        self.metrics = {}

    def log(self, level: str, table: str,
            message: str, metrics: dict = None):
        """Log một event với structured format."""
        entry = {
            "run_id":    self.run_id,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "level":     level,
            "table":     table,
            "message":   message,
            "metrics":   metrics or {},
        }
        self.logs.append(entry)

        icon       = self.ICONS.get(level, "·")
        metric_str = " | ".join([
            f"{k}={v}"
            for k, v in (metrics or {}).items()
        ])

        print(
            f"[{entry['timestamp']}] {icon} "
            f"{level:<8} {table:<25} {message}"
            + (f"  ({metric_str})" if metric_str else "")
        )

    def summary(self):
        """In summary toàn bộ pipeline run."""
        print(f"\n{'='*55}")
        print(f"PIPELINE SUMMARY — Run ID: {self.run_id}")
        print(f"{'='*55}")

        criticals = [l for l in self.logs
                     if l["level"] == "CRITICAL"]
        warns     = [l for l in self.logs
                     if l["level"] == "WARN"]
        successes = [l for l in self.logs
                     if l["level"] == "SUCCESS"]

        print(f"  Total events: {len(self.logs)}")
        print(f"  ✔ Success:    {len(successes)}")
        print(f"  ⚠ Warnings:   {len(warns)}")
        print(f"  ✖ Criticals:  {len(criticals)}")

        if criticals:
            print("\n  Critical events:")
            for c in criticals:
                print(f"    → {c['table']}: {c['message']}")

        print(f"{'='*55}\n")