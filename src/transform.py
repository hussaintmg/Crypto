import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoTransformer:
    """Transform cryptocurrency data"""
    
    def transform(self, raw_data):
        """
        Transform raw API data into clean, structured format
        """
        try:
            logger.info("Starting data transformation")
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(raw_data)
            
            # Remove null values
            df = self._remove_nulls(df)
            
            # Convert and clean numeric fields
            df = self._clean_numeric_fields(df)
            
            # Add feature engineering
            df = self._add_features(df)
            
            # Add timestamp
            df['extracted_at'] = datetime.now()
            
            # Convert to list of dictionaries
            transformed_data = self._df_to_dict_list(df)
            
            logger.info(f"Successfully transformed {len(transformed_data)} records")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise
    
    def _remove_nulls(self, df):
        """Remove rows with critical null values"""
        critical_fields = ['id', 'symbol', 'name', 'current_price']
        
        # Check for nulls in critical fields
        for field in critical_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in {field}")
                    df = df.dropna(subset=[field])
        
        # Fill other nulls with appropriate values
        fill_values = {
            'market_cap': 0,
            'total_volume': 0,
            'price_change_percentage_24h': 0.0,
            'market_cap_rank': 999
        }
        
        df = df.fillna(fill_values)
        return df
    
    def _clean_numeric_fields(self, df):
        """Clean and convert numeric fields"""
        
        # Convert numeric columns
        numeric_columns = ['current_price', 'market_cap', 'total_volume', 'price_change_24h']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Handle large numbers (convert to int for big integers)
        df['market_cap'] = df['market_cap'].astype('int64')
        df['total_volume'] = df['total_volume'].astype('int64')
        
        # Round price to 8 decimal places
        df['current_price'] = df['current_price'].round(8)
        
        return df
    
    def _add_features(self, df):
        """Add engineered features"""
        # Calculate volatility score: absolute price change * total volume
        df['volatility_score'] = abs(df['price_change_24h']) * df['total_volume']
        
        # Price to volume ratio
        df['price_to_volume_ratio'] = df['current_price'] / (df['total_volume'] + 1)
        
        # Market cap to volume ratio
        df['market_cap_to_volume'] = df['market_cap'] / (df['total_volume'] + 1)
        
        # Log transform of market cap (for better visualization)
        df['log_market_cap'] = np.log1p(df['market_cap'])
        
        logger.info(f"Added engineered features: volatility_score, price_to_volume_ratio, market_cap_to_volume")
        return df
    
    def _df_to_dict_list(self, df):
        """Convert DataFrame to list of dictionaries with proper field names"""
        field_mapping = {
            'id': 'coin_id',
            'symbol': 'symbol',
            'name': 'name',
            'current_price': 'current_price',
            'market_cap': 'market_cap',
            'total_volume': 'total_volume',
            'price_change_24h': 'price_change_24h',
            'market_cap_rank': 'market_cap_rank',
            'volatility_score': 'volatility_score',
            'extracted_at': 'extracted_at'
        }
        
        # Select and rename columns
        available_cols = [col for col in field_mapping.keys() if col in df.columns]
        df_selected = df[available_cols].copy()
        df_selected = df_selected.rename(columns=field_mapping)
        
        return df_selected.to_dict('records')