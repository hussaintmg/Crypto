import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_values
import logging
from contextlib import contextmanager
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database connection pool manager"""
    
    def __init__(self):
        self.connection_pool = None
        self._create_connection_pool()
        self._create_tables()
        self._create_indexes()
    
    def _get_db_connection_string(self):
        """Get database connection string from environment variables"""
        
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            return database_url
        
        # For custom configuration
        return f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', '')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'crypto_db')}"
    
    def _create_connection_pool(self):
        """Create a connection pool for cloud database"""
        try:
            # Use min connections = 1, max connections = 10 for cloud
            self.connection_pool = pool.SimpleConnectionPool(
                1, 10,
                self._get_db_connection_string()
            )
            logger.info("Cloud database connection pool created successfully")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            raise
    
    def _create_connection_pool(self):
        """Create a connection pool"""
        try:
            self.connection_pool = pool.SimpleConnectionPool(
                1, 20,  # min connections, max connections
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn and self.connection_pool:
                self.connection_pool.putconn(conn)
    
    def _create_tables(self):
        """Create crypto_market table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_market (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(100) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            name VARCHAR(100) NOT NULL,
            current_price DECIMAL(20, 8),
            market_cap BIGINT,
            total_volume BIGINT,
            price_change_24h DECIMAL(10, 2),
            market_cap_rank INTEGER,
            volatility_score DECIMAL(20, 2),
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(coin_id, extracted_at)
        );
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                logger.info("Table 'crypto_market' created or already exists")
    
    def _create_indexes(self):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_coin_id ON crypto_market(coin_id);",
            "CREATE INDEX IF NOT EXISTS idx_extracted_at ON crypto_market(extracted_at);",
            "CREATE INDEX IF NOT EXISTS idx_market_cap_rank ON crypto_market(market_cap_rank);"
        ]
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for index_query in indexes:
                    cur.execute(index_query)
                logger.info("Database indexes created successfully")
    
    def upsert_market_data(self, data):
        """Insert or update market data using UPSERT"""
        insert_query = """
        INSERT INTO crypto_market 
        (coin_id, symbol, name, current_price, market_cap, total_volume, 
         price_change_24h, market_cap_rank, volatility_score, extracted_at)
        VALUES %s
        ON CONFLICT (coin_id, extracted_at) 
        DO UPDATE SET
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            price_change_24h = EXCLUDED.price_change_24h,
            market_cap_rank = EXCLUDED.market_cap_rank,
            volatility_score = EXCLUDED.volatility_score;
        """
        
        records = [(d['coin_id'], d['symbol'], d['name'], d['current_price'],
                   d['market_cap'], d['total_volume'], d['price_change_24h'],
                   d['market_cap_rank'], d['volatility_score'], d['extracted_at']) 
                   for d in data]
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, records)
                logger.info(f"Inserted/Updated {len(records)} records")
                return len(records)
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    results = cur.fetchall()
                    return [dict(zip(columns, row)) for row in results]
                return None
    
    def close_all_connections(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("All database connections closed")