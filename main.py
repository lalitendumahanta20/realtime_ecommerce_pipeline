
import sys
import time
import logging
from pathlib import Path


# =====================================================
# PROJECT PATH CONFIGURATION
# =====================================================

PROJECT_DIR = Path(__file__).resolve().parent
SRC_DIR = PROJECT_DIR / "src"

# Add src folder before importing project modules.
sys.path.insert(0, str(SRC_DIR))


# =====================================================
# IMPORT PIPELINE MODULES
# =====================================================

from load_events import load_events
from process_batches import process_batches
from reconcile import reconcile_data

from run_history import (
    start_pipeline_run,
    complete_pipeline_run,
    fail_pipeline_run
)


# =====================================================
# LOGGING CONFIGURATION
# =====================================================

(PROJECT_DIR / "logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            PROJECT_DIR / "logs" / "pipeline.log"
        ),
        logging.StreamHandler()
    ]
)


# =====================================================
# PIPELINE STAGE EXECUTION
# =====================================================

def run_stage(stage_name, stage_function):

    logging.info("Starting %s...", stage_name)

    start_time = time.perf_counter()

    result = stage_function()

    duration = time.perf_counter() - start_time

    logging.info(
        "%s completed in %.4f seconds",
        stage_name,
        duration
    )

    return duration, result


# =====================================================
# MAIN PIPELINE
# =====================================================

def run_pipeline():

    pipeline_start = time.perf_counter()
    run_id = None

    # Default ingestion metrics.
    load_stats = {
        "records_read": 0,
        "records_inserted": 0,
        "duplicates_skipped": 0,
        "records_rejected": 0
    }

    logging.info("========== PIPELINE STARTED ==========")

    try:

        # Create pipeline run history record.
        run_id = start_pipeline_run()

        logging.info("Pipeline run ID: %s", run_id)

        # -------------------------------------------------
        # STAGE 1: EVENT INGESTION
        # -------------------------------------------------

        load_time, load_stats = run_stage(
            "Event ingestion",
            load_events
        )

        logging.info("========== INGESTION METRICS ==========")

        logging.info(
            "Records read: %s",
            load_stats["records_read"]
        )

        logging.info(
            "Records inserted: %s",
            load_stats["records_inserted"]
        )

        logging.info(
            "Ingestion duplicates skipped: %s",
            load_stats["duplicates_skipped"]
        )

        logging.info(
            "Records rejected: %s",
            load_stats["records_rejected"]
        )

        # -------------------------------------------------
        # STAGE 2: MICRO-BATCH PROCESSING
        # -------------------------------------------------

        processing_time, processing_stats = run_stage(
            "Micro-batch processing",
            process_batches
        )

        logging.info("========== BATCH PROCESSING METRICS ==========")

        logging.info(
            "Batches processed: %s",
            processing_stats["batches_processed"]
        )

        logging.info(
            "Events processed: %s",
            processing_stats["events_processed"]
        )

        logging.info(
            "Processing duplicates skipped: %s",
            processing_stats["duplicates_skipped"]
        )

        logging.info(
            "Late events detected: %s",
            processing_stats["late_events_detected"]
        )

        logging.info(
            "Final checkpoint: %s",
            processing_stats["final_checkpoint"]
        )

        logging.info(
            "Final watermark: %s",
            processing_stats["final_watermark"]
        )

        logging.info(
            "Processing throughput: %.2f events/second",
            processing_stats["events_per_second"]
        )

        # -------------------------------------------------
        # STAGE 3: DATA RECONCILIATION
        # -------------------------------------------------

        reconciliation_time, reconciliation_success = run_stage(
            "Data reconciliation",
            reconcile_data
        )

        if not reconciliation_success:
            raise RuntimeError("Data reconciliation failed")

        # -------------------------------------------------
        # CALCULATE TOTAL EXECUTION TIME
        # -------------------------------------------------

        total_time = time.perf_counter() - pipeline_start

        # -------------------------------------------------
        # UPDATE PIPELINE RUN HISTORY
        # -------------------------------------------------

        complete_pipeline_run(
            run_id,
            load_stats["records_read"],
            load_stats["records_inserted"],
            load_stats["records_rejected"],

            batches_processed=processing_stats["batches_processed"],
            processing_events=processing_stats["events_processed"],
            duplicates_skipped=processing_stats["duplicates_skipped"],
            late_events_detected=processing_stats["late_events_detected"],
            final_checkpoint=processing_stats["final_checkpoint"],
            final_watermark=processing_stats["final_watermark"],
            processing_duration_seconds=processing_stats[
                "processing_duration_seconds"
            ],
            events_per_second=processing_stats["events_per_second"],

            ingestion_duration_seconds=load_time,
            reconciliation_duration_seconds=reconciliation_time,
            total_duration_seconds=total_time
        )

        # -------------------------------------------------
        # FINAL PERFORMANCE SUMMARY
        # -------------------------------------------------

        logging.info("========== PERFORMANCE SUMMARY ==========")

        logging.info(
            "Ingestion time: %.4f seconds",
            load_time
        )

        logging.info(
            "Processing time: %.4f seconds",
            processing_time
        )

        logging.info(
            "Reconciliation time: %.4f seconds",
            reconciliation_time
        )

        logging.info(
            "Total execution time: %.4f seconds",
            total_time
        )

        logging.info("Pipeline run ID: %s", run_id)
        logging.info("Pipeline status: SUCCESS")

        logging.info("Pipeline metrics saved to SQL Server")
        logging.info("==========================================")

    except Exception as error:

        logging.exception("Pipeline failed")

        # Record pipeline failure in SQL Server.
        if run_id is not None:

            try:
                fail_pipeline_run(run_id, str(error))

                logging.info(
                    "Pipeline failure recorded in SQL Server"
                )

            except Exception:
                logging.exception(
                    "Could not update pipeline run history"
                )

        raise


# =====================================================
# APPLICATION ENTRY POINT
# =====================================================

if __name__ == "__main__":
    run_pipeline()
