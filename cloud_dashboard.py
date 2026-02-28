import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time
import os
import sys

# Add the current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your modules
from src.analysis import CryptoAnalyzer
from src.database import DatabaseManager

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
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stApp {
        background-color: #0E1117;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize database connection
@st.cache_resource
def init_database():
    """Initialize database connection (cached)"""
    return DatabaseManager()

# Initialize analyzer
@st.cache_resource
def init_analyzer():
    """Initialize crypto analyzer (cached)"""
    return CryptoAnalyzer()

# Main app
def main():
    st.markdown('<h1 class="main-header">🚀 Real-Time Crypto Analytics Platform</h1>', 
                unsafe_allow_html=True)
    
    # Initialize connections
    db_manager = init_database()
    analyzer = init_analyzer()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Dashboard Controls")
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh (60s)", value=True)
        
        # Last update time
        st.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Database info
        st.markdown("---")
        st.subheader("📊 Database Status")
        
        # Get record count
        try:
            result = db_manager.execute_query("SELECT COUNT(*) as count FROM crypto_market")
            if result:
                st.success(f"Records: {result[0]['count']}")
        except:
            st.warning("Database connection pending...")
    
    # Main content area
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Top Gainers (24h)")
        try:
            gainers = analyzer.get_top_5_gainers()
            if gainers:
                df = pd.DataFrame(gainers)
                fig = px.bar(df, x='name', y='price_change_24h', 
                            title='Top 5 Gainers',
                            color='price_change_24h',
                            color_continuous_scale=['red', 'yellow', 'green'])
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading gainers: {e}")
    
    with col2:
        st.subheader("💰 Top by Market Cap")
        try:
            top_mcap = analyzer.get_top_5_by_market_cap()
            if top_mcap:
                df = pd.DataFrame(top_mcap)
                fig = px.bar(df, x='name', y='market_cap',
                            title='Top 5 by Market Cap',
                            color='market_cap',
                            color_continuous_scale='Viridis')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading market cap: {e}")
    
    # Second row
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("⚡ Most Volatile")
        try:
            volatility = analyzer.get_volatility_ranking()
            if volatility:
                df = pd.DataFrame(volatility[:5])
                fig = px.bar(df, x='name', y='volatility_score',
                            title='Top 5 Most Volatile',
                            color='volatility_score',
                            color_continuous_scale='Reds')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading volatility: {e}")
    
    with col4:
        st.subheader("📊 Volume Comparison")
        try:
            volume_data = analyzer.get_volume_comparison()
            if volume_data:
                df = pd.DataFrame(volume_data[:5])
                fig = px.pie(df, values='total_volume', names='name',
                            title='Volume Distribution')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading volume: {e}")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(60)
        st.rerun()

if __name__ == "__main__":
    main()