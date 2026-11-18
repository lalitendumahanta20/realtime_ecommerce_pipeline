
"""Scheduled data-quality monitoring for the e-commerce pipeline."""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

from src.data_quality import run_quality_checks


# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Monitoring log directory and file
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "data_quality.log"


def write_monitoring_log(report):
    """Append one quality report as a JSON line."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(report, default=str) + "\n")


def run_monitor():
    """Run quality checks, log results, and return an exit code."""

    try:
        report = run_quality_checks()
        write_monitoring_log(report)

        print("=" * 50)
        print("DATA QUALITY MONITORING REPORT")
        print("=" * 50)
        print(f"Overall status : {report['overall_status']}")
        print(f"Total checks   : {report['total_checks']}")
        print(f"Total issues   : {report['total_issues']}")
        print(f"Checked at     : {report['checked_at']}")
        print(f"Log file       : {LOG_FILE}")

        for check in report["checks"]:
            print(
                f"{check['check']}: "
                f"{check['status']} "
                f"({check['issue_count']} issues)"
            )

        if report["overall_status"] == "FAIL":
            print("\nALERT: Data-quality issues detected!")
            return 1

        print("\nAll data-quality checks passed.")
        return 0

    except Exception as exc:
        error_report = {
            "overall_status": "ERROR",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc)
        }

        write_monitoring_log(error_report)
        print(f"MONITORING ERROR: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(run_monitor())
