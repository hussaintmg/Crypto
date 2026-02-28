import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from datetime import datetime
import schedule
import signal
import sys
from src.extract import CryptoExtractor
from src.transform import CryptoTransformer
from src.load import CryptoLoader
from config import Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{Config.LOG_PATH}etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """Orchestrate the ETL process"""
    
    def __init__(self):
        self.extractor = CryptoExtractor()
        self.transformer = CryptoTransformer()
        self.loader = CryptoLoader()
        self.is_running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal. Stopping ETL pipeline...")
        self.is_running = False
    
    def run_etl(self):
        """Execute the complete ETL pipeline"""
        start_time = time.time()
        pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"Starting ETL Pipeline {pipeline_id}")
        
        try:
            # Extract
            logger.info("Step 1: Extracting data...")
            raw_data = self.extractor.extract()
            
            if not raw_data:
                logger.warning("No data extracted. Skipping ETL cycle.")
                return
            
            # Transform
            logger.info("Step 2: Transforming data...")
            transformed_data = self.transformer.transform(raw_data)
            
            # Load
            logger.info("Step 3: Loading data...")
            records_loaded = self.loader.load(transformed_data)
            
            # Log metrics
            elapsed_time = time.time() - start_time
            logger.info(f"ETL Pipeline {pipeline_id} completed successfully")
            logger.info(f"Records loaded: {records_loaded}")
            logger.info(f"Time taken: {elapsed_time:.2f} seconds")
            
            # Optional: Log summary statistics
            self._log_summary_stats()
            
        except Exception as e:
            logger.error(f"ETL Pipeline {pipeline_id} failed: {e}", exc_info=True)
    
    def _log_summary_stats(self):
        """Log summary statistics from the database"""
        try:
            latest_ts = self.loader.get_latest_timestamp()
            if latest_ts:
                logger.info(f"Latest data timestamp: {latest_ts}")
            
            record_count = self.loader.get_record_count()
            logger.info(f"Total records in database: {record_count}")
            
        except Exception as e:
            logger.error(f"Error logging summary stats: {e}")
    
    def run_scheduled(self):
        """Run ETL on a schedule"""
        interval = Config.ETL_INTERVAL_MINUTES
        
        logger.info(f"Scheduling ETL to run every {interval} minutes")
        
        # Schedule the ETL job
        schedule.every(interval).minutes.do(self.run_etl)
        
        # Run once immediately on startup
        self.run_etl()
        
        # Keep running
        while self.is_running:
            schedule.run_pending()
            time.sleep(1)
        
        logger.info("ETL Pipeline stopped")

def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    
    try:
        pipeline.run_scheduled()
    except KeyboardInterrupt:
        logger.info("ETL Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()