import logging
from src.database import DatabaseManager
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoLoader:
    """Load transformed data into database"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def load(self, transformed_data, batch_size=100):
        """
        Load transformed data into PostgreSQL database
        
        Args:
            transformed_data: List of dictionaries containing transformed data
            batch_size: Number of records to insert per batch
        
        Returns:
            int: Number of records loaded
        """
        try:
            logger.info(f"Starting to load {len(transformed_data)} records")
            
            # Add timestamp if not present
            for record in transformed_data:
                if 'extracted_at' not in record:
                    record['extracted_at'] = datetime.now()
            
            # Load data in batches
            total_loaded = 0
            for i in range(0, len(transformed_data), batch_size):
                batch = transformed_data[i:i + batch_size]
                loaded = self.db_manager.upsert_market_data(batch)
                total_loaded += loaded
                logger.debug(f"Loaded batch {i//batch_size + 1}: {loaded} records")
            
            logger.info(f"Successfully loaded {total_loaded} records")
            return total_loaded
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def get_latest_timestamp(self):
        """Get the latest extraction timestamp"""
        query = """
        SELECT MAX(extracted_at) as latest_timestamp 
        FROM crypto_market
        """
        result = self.db_manager.execute_query(query)
        if result and result[0]['latest_timestamp']:
            return result[0]['latest_timestamp']
        return None
    
    def get_record_count(self):
        """Get total number of records in database"""
        query = "SELECT COUNT(*) as count FROM crypto_market"
        result = self.db_manager.execute_query(query)
        return result[0]['count'] if result else 0