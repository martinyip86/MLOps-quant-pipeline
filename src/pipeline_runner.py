import sys
from src.workers.daily_patcher import patcher
from src.workers.consolidator import consolidator
from src.utils.logger import setup_logger

logger = setup_logger('pipeline.runner')

def run_daily_pipeline():
    try:
        logger.info("🚀 [STEP 1] Starting Daily Patcher...")
        patcher()

        logger.info("📊 [STEP 2] Starting Feature Consolidation...")
        consolidator()

        logger.info("✅ [SUCCESS] Daily Pipeline finished updates.")
    except Exception as e:
        logger.error(f"❌ [PIPELINE FAILED] Critical error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_daily_pipeline()  