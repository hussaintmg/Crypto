import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'crypto_db')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    
    # API configuration
    COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
    API_PARAMS = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 20,
        'page': 1,
        'sparkline': 'false'
    }
    
    # ETL configuration
    ETL_INTERVAL_MINUTES = 5
    MAX_RETRIES = 3
    RETRY_DELAY = 60  # seconds
    
    # Dashboard configuration
    DASHBOARD_REFRESH_SECONDS = 60
    
    # Paths
    RAW_DATA_PATH = 'data/raw/'
    LOG_PATH = 'logs/'