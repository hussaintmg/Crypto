import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_values
import logging
import decimal
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
    
    def _create_connection_pool(self):
        """Create a connection pool with fallback to Config values if DATABASE_URL is not set"""
        try:
            database_url = os.getenv('DATABASE_URL')
            
            if database_url:
                logger.info("Using DATABASE_URL for connection pool")
                self.connection_pool = pool.SimpleConnectionPool(
                    1, 20,
                    database_url
                )
            else:
                logger.info("Using individual config parameters for connection pool")
                self.connection_pool = pool.SimpleConnectionPool(
                    1, 20,
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
        """Create crypto_market table if it doesn't exist and ensure schema is up to date"""
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
            image_url TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(coin_id, extracted_at)
        );
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                
                # Check if image_url column exists (Migration logic)
                cur.execute("""
                    SELECT count(*) 
                    FROM information_schema.columns 
                    WHERE table_name='crypto_market' AND column_name='image_url'
                """)
                if cur.fetchone()[0] == 0:
                    logger.info("Adding missing 'image_url' column to 'crypto_market'")
                    cur.execute("ALTER TABLE crypto_market ADD COLUMN image_url TEXT")
                
                logger.info("Table 'crypto_market' verified and ready")

    
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
         price_change_24h, market_cap_rank, volatility_score, image_url, extracted_at)
        VALUES %s
        ON CONFLICT (coin_id, extracted_at) 
        DO UPDATE SET
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            price_change_24h = EXCLUDED.price_change_24h,
            market_cap_rank = EXCLUDED.market_cap_rank,
            volatility_score = EXCLUDED.volatility_score,
            image_url = EXCLUDED.image_url;
        """
        
        records = [(d['coin_id'], d['symbol'], d['name'], d['current_price'],
                   d['market_cap'], d['total_volume'], d['price_change_24h'],
                   d['market_cap_rank'], d['volatility_score'], d['image_url'], d['extracted_at']) 
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
                    
                    processed_results = []
                    for row in results:
                        dict_row = dict(zip(columns, row))
                        # Convert Decimals to floats for JSON/Dashboard compatibility
                        for key, value in dict_row.items():
                            if isinstance(value, decimal.Decimal):
                                dict_row[key] = float(value)
                        processed_results.append(dict_row)
                        
                    return processed_results
                return None
    
    def close_all_connections(self):
        """Close all database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("All database connections closed")

if __name__ == "__main__":
    try:
        import streamlit as st
        st.warning("⚠️ You are running 'src/database.py'. Please run 'streamlit_app.py' instead.")
        st.info("💡 To fix: Change the 'Main file path' in Streamlit Cloud settings to 'streamlit_app.py'")
        
        if st.button("🚀 Load Dashboard Now"):
            from src.dashboard import CryptoDashboard
            dashboard = CryptoDashboard()
            dashboard.run()
    except Exception:
        pass
