import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
import logging
import time
from datetime import datetime
import os
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoExtractor:
    """Extract cryptocurrency data from CoinGecko API"""
    
    def __init__(self):
        self.base_url = Config.COINGECKO_API_URL
        self.params = Config.API_PARAMS
        self.raw_data_path = Config.RAW_DATA_PATH
        
        # Create raw data directory if it doesn't exist
        os.makedirs(self.raw_data_path, exist_ok=True)
    
    def extract(self, retry_count=0):
        """
        Extract data from CoinGecko API
        """
        try:
            logger.info(f"Extracting data from CoinGecko API (Attempt {retry_count + 1})")
            
            response = requests.get(
                self.base_url, 
                params=self.params,
                timeout=30,
                headers={'Accept': 'application/json'}
            )
            
            # Check for rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds")
                time.sleep(retry_after)
                if retry_count < Config.MAX_RETRIES:
                    return self.extract(retry_count + 1)
                else:
                    raise Exception("Max retries exceeded due to rate limiting")
            
            response.raise_for_status()
            data = response.json()
            
            # Validate response
            if not self._validate_data(data):
                raise ValueError("Invalid data format received from API")
            
            # Save raw JSON for logging/debugging
            self._save_raw_data(data)
            
            logger.info(f"Successfully extracted {len(data)} coins")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            if retry_count < Config.MAX_RETRIES:
                wait_time = Config.RETRY_DELAY * (retry_count + 1)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return self.extract(retry_count + 1)
            else:
                raise Exception(f"Failed to extract data after {Config.MAX_RETRIES} retries: {e}")
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def _validate_data(self, data):
        """Validate the extracted data"""
        if not isinstance(data, list):
            logger.error("Data is not a list")
            return False
        
        if len(data) == 0:
            logger.error("Empty data received")
            return False
        
        required_fields = ['id', 'symbol', 'name', 'current_price', 
                          'market_cap', 'total_volume', 'price_change_percentage_24h']
        
        for item in data:
            if not all(field in item for field in required_fields):
                logger.error(f"Missing required fields in item: {item.get('id', 'unknown')}")
                return False
        
        return True
    
    def _save_raw_data(self, data):
        """Save raw JSON data for logging purposes"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.raw_data_path}/raw_crypto_data_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'data': data
                }, f, indent=2)
            logger.debug(f"Raw data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving raw data: {e}")