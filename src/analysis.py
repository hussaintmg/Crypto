import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import logging
from src.database import DatabaseManager
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoAnalyzer:
    """Perform data analysis on cryptocurrency data"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def get_top_5_gainers(self):
        """Get top 5 gainers in the last 24 hours"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                name,
                symbol,
                price_change_24h,
                current_price,
                extracted_at
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT 
            name,
            symbol,
            price_change_24h,
            current_price
        FROM latest_data
        WHERE price_change_24h IS NOT NULL
        ORDER BY price_change_24h DESC
        LIMIT 5;
        """
        
        return self.db_manager.execute_query(query)
    
    def get_top_5_by_market_cap(self):
        """Get top 5 cryptocurrencies by market cap"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                name,
                symbol,
                market_cap,
                market_cap_rank,
                current_price,
                extracted_at
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT 
            name,
            symbol,
            market_cap,
            market_cap_rank,
            current_price
        FROM latest_data
        ORDER BY market_cap_rank
        LIMIT 5;
        """
        
        return self.db_manager.execute_query(query)
    
    def get_average_market_cap(self):
        """Get average market cap of all cryptocurrencies"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                market_cap
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT 
            AVG(market_cap) as avg_market_cap,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap) as median_market_cap,
            MIN(market_cap) as min_market_cap,
            MAX(market_cap) as max_market_cap
        FROM latest_data
        WHERE market_cap > 0;
        """
        
        return self.db_manager.execute_query(query)
    
    def get_total_market_value(self):
        """Get total cryptocurrency market value"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                market_cap
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT SUM(market_cap) as total_market_cap
        FROM latest_data;
        """
        
        result = self.db_manager.execute_query(query)
        return result[0]['total_market_cap'] if result else 0
    
    def get_volatility_ranking(self):
        """Get cryptocurrencies ranked by volatility score"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                name,
                symbol,
                volatility_score,
                price_change_24h,
                total_volume,
                extracted_at
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT 
            name,
            symbol,
            volatility_score,
            price_change_24h,
            total_volume,
            RANK() OVER (ORDER BY volatility_score DESC) as volatility_rank
        FROM latest_data
        WHERE volatility_score IS NOT NULL
        ORDER BY volatility_score DESC
        LIMIT 10;
        """
        
        return self.db_manager.execute_query(query)
    
    def get_volume_comparison(self):
        """Get volume comparison across cryptocurrencies"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                name,
                symbol,
                total_volume,
                current_price,
                extracted_at
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        )
        SELECT 
            name,
            symbol,
            total_volume,
            current_price,
            (total_volume * current_price) as volume_usd_value
        FROM latest_data
        WHERE total_volume > 0
        ORDER BY total_volume DESC
        LIMIT 10;
        """
        
        return self.db_manager.execute_query(query)
    
    def get_price_trends(self, hours=24):
        """Get price trends for the last X hours"""
        query = """
        WITH hourly_prices AS (
            SELECT 
                coin_id,
                name,
                symbol,
                date_trunc('hour', extracted_at) as hour,
                AVG(current_price) as avg_price,
                FIRST_VALUE(current_price) OVER (PARTITION BY coin_id, date_trunc('hour', extracted_at) ORDER BY extracted_at) as hour_open,
                LAST_VALUE(current_price) OVER (PARTITION BY coin_id, date_trunc('hour', extracted_at) ORDER BY extracted_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as hour_close
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '%s hours'
            GROUP BY coin_id, name, symbol, date_trunc('hour', extracted_at), extracted_at
        )
        SELECT 
            name,
            symbol,
            hour,
            avg_price,
            hour_open,
            hour_close,
            ((hour_close - hour_open) / hour_open * 100) as hourly_change_percent
        FROM hourly_prices
        ORDER BY hour DESC, name;
        """
        
        return self.db_manager.execute_query(query, (hours,))
    
    def get_market_summary(self):
        """Get comprehensive market summary"""
        query = """
        WITH latest_data AS (
            SELECT DISTINCT ON (coin_id) 
                coin_id,
                name,
                symbol,
                current_price,
                market_cap,
                total_volume,
                price_change_24h,
                volatility_score
            FROM crypto_market
            WHERE extracted_at >= NOW() - INTERVAL '1 hour'
            ORDER BY coin_id, extracted_at DESC
        ),
        market_stats AS (
            SELECT 
                COUNT(*) as total_coins,
                SUM(market_cap) as total_market_cap,
                AVG(market_cap) as avg_market_cap,
                SUM(total_volume) as total_volume_24h,
                AVG(price_change_24h) as avg_price_change,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_change_24h) as median_price_change,
                SUM(CASE WHEN price_change_24h > 0 THEN 1 ELSE 0 END) as gainers_count,
                SUM(CASE WHEN price_change_24h < 0 THEN 1 ELSE 0 END) as losers_count
            FROM latest_data
        ),
        top_gainer AS (
            SELECT name || ' (' || symbol || ')' as top_gainer
            FROM latest_data
            ORDER BY price_change_24h DESC
            LIMIT 1
        ),
        most_volatile AS (
            SELECT name || ' (' || symbol || ')' as most_volatile
            FROM latest_data
            ORDER BY volatility_score DESC
            LIMIT 1
        )
        SELECT 
            ms.*,
            tg.top_gainer,
            mv.most_volatile
        FROM market_stats ms
        CROSS JOIN top_gainer tg
        CROSS JOIN most_volatile mv;
        """
        
        result = self.db_manager.execute_query(query)
        return result[0] if result else {}