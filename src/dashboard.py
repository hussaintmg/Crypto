import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from src.analysis import CryptoAnalyzer
from src.database import DatabaseManager
from config import Config

# Page configuration
st.set_page_config(
    page_title="Real-Time Crypto Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .kpi-label {
        font-size: 1rem;
        color: #ffffff;
        opacity: 0.9;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff;
    }
    .positive-change {
        color: #00ff00;
        font-weight: 600;
    }
    .negative-change {
        color: #ff0000;
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

class CryptoDashboard:
    """Real-time cryptocurrency dashboard"""
    
    def __init__(self):
        self.analyzer = CryptoAnalyzer()
        self.db_manager = DatabaseManager()
        self.refresh_rate = Config.DASHBOARD_REFRESH_SECONDS
        
    def run(self):
        """Run the dashboard"""
        # Header
        st.markdown('<h1 class="main-header">🚀 Real-Time Crypto Analytics Platform</h1>', 
                   unsafe_allow_html=True)
        
        # Auto-refresh
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        
        # Sidebar
        with st.sidebar:
            st.header("⚙️ Dashboard Controls")
            
            # Refresh button
            if st.button("🔄 Refresh Now"):
                st.session_state.last_refresh = datetime.now()
                st.rerun()
            
            # Auto-refresh toggle
            auto_refresh = st.checkbox("Auto-refresh every 60 seconds", value=True)
            
            # Last update time
            st.info(f"Last updated: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Data source info
            st.markdown("---")
            st.subheader("📡 Data Source")
            st.markdown("CoinGecko API - Top 20 Cryptocurrencies")
            
            # ETL Status
            st.subheader("🔄 ETL Pipeline")
            latest_ts = self.get_latest_timestamp()
            if latest_ts:
                st.success(f"Last ETL: {latest_ts.strftime('%H:%M:%S')}")
            else:
                st.warning("No data available")
        
        # Main content
        self.display_kpis()
        self.display_charts()
        
        # Auto-refresh logic
        if auto_refresh:
            time.sleep(self.refresh_rate)
            st.rerun()
    
    def get_latest_timestamp(self):
        """Get the latest data timestamp"""
        query = "SELECT MAX(extracted_at) as latest FROM crypto_market"
        result = self.db_manager.execute_query(query)
        if result and result[0]['latest']:
            return result[0]['latest']
        return None
    
    def safe_convert_to_dataframe(self, data):
        """Safely convert data to pandas DataFrame"""
        if data is None or len(data) == 0:
            return pd.DataFrame()
        
        # Convert to DataFrame and ensure proper data types
        df = pd.DataFrame(data)
        
        # Convert numeric columns to float/int
        numeric_columns = ['current_price', 'market_cap', 'total_volume', 
                          'price_change_24h', 'volatility_score']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def display_kpis(self):
        """Display KPI cards"""
        st.subheader("📈 Key Performance Indicators")
        
        # Get market summary
        summary = self.analyzer.get_market_summary()
        
        if summary:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                self.create_kpi_card(
                    "Total Market Cap",
                    f"${summary.get('total_market_cap', 0):,.0f}",
                    "💰"
                )
            
            with col2:
                self.create_kpi_card(
                    "Total 24h Volume",
                    f"${summary.get('total_volume_24h', 0):,.0f}",
                    "📊"
                )
            
            with col3:
                gainers = summary.get('gainers_count', 0)
                losers = summary.get('losers_count', 0)
                total = gainers + losers
                ratio = (gainers / total * 100) if total > 0 else 0
                self.create_kpi_card(
                    "Gainers/Losers",
                    f"{gainers} / {losers} ({ratio:.1f}% positive)",
                    "📈"
                )
            
            with col4:
                self.create_kpi_card(
                    "Avg Price Change",
                    f"{summary.get('avg_price_change', 0):.2f}%",
                    "📉"
                )
            
            # Additional KPI row
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                self.create_kpi_card(
                    "Total Coins Tracked",
                    f"{summary.get('total_coins', 0)}",
                    "🪙"
                )
            
            with col2:
                self.create_kpi_card(
                    "Highest Gainer",
                    summary.get('top_gainer', 'N/A'),
                    "🏆"
                )
            
            with col3:
                self.create_kpi_card(
                    "Most Volatile",
                    summary.get('most_volatile', 'N/A'),
                    "⚡"
                )
            
            with col4:
                self.create_kpi_card(
                    "Avg Market Cap",
                    f"${summary.get('avg_market_cap', 0):,.0f}",
                    "📦"
                )
    
    def create_kpi_card(self, label, value, icon):
        """Create a styled KPI card"""
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{icon} {label}</div>
                <div class="kpi-value">{value}</div>
            </div>
        """, unsafe_allow_html=True)
    
    def display_charts(self):
        """Display all charts"""
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Market Overview", 
            "📈 Price Analysis", 
            "📉 Volume Analysis",
            "⚡ Volatility Analysis"
        ])
        
        with tab1:
            self.display_market_overview()
        
        with tab2:
            self.display_price_analysis()
        
        with tab3:
            self.display_volume_analysis()
        
        with tab4:
            self.display_volatility_analysis()
    
    def display_market_overview(self):
        """Display market overview charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 10 by Market Cap
            top_mcap = self.analyzer.get_top_5_by_market_cap()
            if top_mcap:
                df = self.safe_convert_to_dataframe(top_mcap)
                if not df.empty:
                    fig = px.bar(
                        df.head(10),
                        x='name',
                        y='market_cap',
                        title='Top 10 Cryptocurrencies by Market Cap',
                        labels={'market_cap': 'Market Cap (USD)', 'name': 'Cryptocurrency'},
                        color='market_cap',
                        color_continuous_scale='Viridis'
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No market cap data available")
        
        with col2:
            # Market Cap Distribution
            if top_mcap and len(top_mcap) > 0:
                df = self.safe_convert_to_dataframe(top_mcap[:10])
                if not df.empty:
                    fig = px.pie(
                        df,
                        values='market_cap',
                        names='name',
                        title='Market Cap Distribution (Top 10)'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No distribution data available")
    
    def display_price_analysis(self):
        """Display price analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            # Top Gainers
            gainers = self.analyzer.get_top_5_gainers()
            if gainers:
                df = self.safe_convert_to_dataframe(gainers)
                if not df.empty:
                    fig = px.bar(
                        df,
                        x='name',
                        y='price_change_24h',
                        title='Top 5 Gainers (24h)',
                        labels={'price_change_24h': 'Price Change %', 'name': 'Cryptocurrency'},
                        color='price_change_24h',
                        color_continuous_scale=['red', 'yellow', 'green']
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No gainers data available")
        
        with col2:
            # Current Prices of Top Gainers
            if gainers and len(gainers) > 0:
                df = self.safe_convert_to_dataframe(gainers)
                if not df.empty:
                    # Ensure current_price is float and not NaN
                    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                    
                    # Create scatter plot without size parameter or use normalized sizes
                    fig = px.scatter(
                        df,
                        x='name',
                        y='current_price',
                        title='Current Prices of Top Gainers',
                        labels={'current_price': 'Price (USD)', 'name': 'Cryptocurrency'},
                        color='current_price',
                        size_max=20  # Set maximum marker size
                    )
                    # Add marker size based on price (normalized)
                    if len(df) > 0 and df['current_price'].max() > 0:
                        sizes = 10 + 30 * (df['current_price'] / df['current_price'].max())
                        fig.update_traces(marker=dict(size=sizes))
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No price data available")
    
    def display_volume_analysis(self):
        """Display volume analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            # Volume Comparison
            volume_data = self.analyzer.get_volume_comparison()
            if volume_data:
                df = self.safe_convert_to_dataframe(volume_data)
                if not df.empty:
                    fig = px.bar(
                        df.head(10),
                        x='name',
                        y='total_volume',
                        title='Top 10 by Trading Volume',
                        labels={'total_volume': 'Volume (USD)', 'name': 'Cryptocurrency'},
                        color='total_volume',
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No volume data available")
        
        with col2:
            # Volume vs Price
            if volume_data and len(volume_data) > 0:
                df = self.safe_convert_to_dataframe(volume_data)
                if not df.empty:
                    # Ensure numeric columns
                    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce').fillna(0)
                    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                    
                    fig = px.scatter(
                        df,
                        x='total_volume',
                        y='current_price',
                        text='symbol',
                        title='Volume vs Price Relationship',
                        labels={'total_volume': 'Volume (USD)', 'current_price': 'Price (USD)'},
                        color='current_price',
                        size_max=20
                    )
                    fig.update_traces(textposition='top center')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No volume vs price data available")
    
    def display_volatility_analysis(self):
        """Display volatility analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            # Volatility Ranking
            volatility = self.analyzer.get_volatility_ranking()
            if volatility:
                df = self.safe_convert_to_dataframe(volatility)
                if not df.empty:
                    fig = px.bar(
                        df.head(10),
                        x='name',
                        y='volatility_score',
                        title='Top 10 Most Volatile Cryptocurrencies',
                        labels={'volatility_score': 'Volatility Score', 'name': 'Cryptocurrency'},
                        color='volatility_score',
                        color_continuous_scale='Reds'
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No volatility data available")
        
        with col2:
            # Volatility vs Volume
            if volatility and len(volatility) > 0:
                df = self.safe_convert_to_dataframe(volatility)
                if not df.empty:
                    # Ensure numeric columns
                    df['total_volume'] = pd.to_numeric(df['total_volume'], errors='coerce').fillna(0)
                    df['volatility_score'] = pd.to_numeric(df['volatility_score'], errors='coerce').fillna(0)
                    df['price_change_24h'] = pd.to_numeric(df['price_change_24h'], errors='coerce').fillna(0)
                    
                    fig = px.scatter(
                        df,
                        x='total_volume',
                        y='volatility_score',
                        text='symbol',
                        title='Volatility vs Volume Relationship',
                        labels={'total_volume': 'Volume (USD)', 'volatility_score': 'Volatility Score'},
                        color='price_change_24h',
                        color_continuous_scale='RdYlGn',
                        size_max=20
                    )
                    fig.update_traces(textposition='top center')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No volatility vs volume data available")

def main():
    """Main function to run the dashboard"""
    dashboard = CryptoDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()