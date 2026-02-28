import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import numpy as np
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

# Custom CSS with improved styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(90deg, transparent, rgba(30,136,229,0.1), transparent);
        border-radius: 10px;
    }
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 1.5rem;
        box-shadow: 0 8px 12px rgba(0, 0, 0, 0.15);
        transition: transform 0.3s ease;
        margin: 0.5rem 0;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 20px rgba(0, 0, 0, 0.2);
    }
    .kpi-label {
        font-size: 1rem;
        color: #ffffff;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff;
        margin-top: 0.5rem;
    }
    .positive-change {
        color: #00ff00;
        font-weight: 600;
        background: rgba(0,255,0,0.1);
        padding: 0.2rem 0.5rem;
        border-radius: 5px;
    }
    .negative-change {
        color: #ff0000;
        font-weight: 600;
        background: rgba(255,0,0,0.1);
        padding: 0.2rem 0.5rem;
        border-radius: 5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        background-color: #f0f2f6;
        padding: 0.5rem;
        border-radius: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

class CryptoDashboard:
    """Real-time cryptocurrency dashboard"""
    
    def __init__(self):
        """Initialize dashboard with database connections"""
        try:
            self.analyzer = CryptoAnalyzer()
            self.db_manager = DatabaseManager()
            self.refresh_rate = Config.DASHBOARD_REFRESH_SECONDS
        except Exception as e:
            st.error(f"Failed to initialize dashboard: {e}")
            st.stop()
        
    def run(self):
        """Run the dashboard"""
        # Header
        st.markdown('<h1 class="main-header">🚀 Real-Time Crypto Analytics Platform</h1>', 
                   unsafe_allow_html=True)
        
        # Initialize session state
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False
        
        # Sidebar
        with st.sidebar:
            st.image("https://img.icons8.com/color/96/000000/cryptocurrency.png", width=80)
            st.header("⚙️ Dashboard Controls")
            
            # Refresh button with spinner
            if st.button("🔄 Refresh Now", use_container_width=True):
                with st.spinner("Refreshing data..."):
                    st.cache_data.clear()
                    st.session_state.last_refresh = datetime.now()
                    st.rerun()
            
            # Auto-refresh toggle
            auto_refresh = st.toggle("Auto-refresh (60s)", value=True)
            
            # Last update time with formatting
            st.info(f"🕐 Last updated: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Data source info
            with st.expander("📡 Data Source", expanded=True):
                st.markdown("""
                - **API:** CoinGecko
                - **Coins:** Top 20
                - **Currency:** USD
                - **Update:** Every 5 min
                """)
            
            # ETL Status with better visualization
            st.subheader("🔄 ETL Pipeline")
            latest_ts = self.get_latest_timestamp()
            if latest_ts:
                time_diff = datetime.now() - latest_ts
                if time_diff.seconds < 300:  # Less than 5 minutes
                    st.success(f"✅ Active (Last: {latest_ts.strftime('%H:%M:%S')})")
                else:
                    st.warning(f"⚠️ Delayed (Last: {latest_ts.strftime('%H:%M:%S')})")
            else:
                st.error("❌ No data available")
        
        # Main content
        try:
            self.display_kpis()
            self.display_charts()
        except Exception as e:
            st.error(f"Error displaying data: {e}")
        
        # Auto-refresh logic
        if auto_refresh:
            time.sleep(self.refresh_rate)
            st.rerun()
    
    def get_latest_timestamp(self):
        """Get the latest data timestamp"""
        try:
            query = "SELECT MAX(extracted_at) as latest FROM crypto_market"
            result = self.db_manager.execute_query(query)
            if result and result[0]['latest']:
                return result[0]['latest']
        except Exception as e:
            st.warning(f"Could not fetch timestamp: {e}")
        return None
    
    def safe_convert_to_dataframe(self, data):
        """Safely convert data to pandas DataFrame"""
        if data is None or len(data) == 0:
            return pd.DataFrame()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Convert numeric columns to proper types
            numeric_columns = ['current_price', 'market_cap', 'total_volume', 
                              'price_change_24h', 'volatility_score']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Fill NaN values
            df = df.fillna(0)
            
            return df
        except Exception as e:
            st.warning(f"Error converting data: {e}")
            return pd.DataFrame()
    
    def format_large_number(self, num):
        """Format large numbers with K, M, B suffixes"""
        if num >= 1e9:
            return f"${num/1e9:.2f}B"
        elif num >= 1e6:
            return f"${num/1e6:.2f}M"
        elif num >= 1e3:
            return f"${num/1e3:.2f}K"
        else:
            return f"${num:.2f}"
    
    def display_kpis(self):
        """Display KPI cards"""
        st.subheader("📈 Key Performance Indicators")
        
        with st.spinner("Loading KPIs..."):
            summary = self.analyzer.get_market_summary()
        
        if summary:
            # First row of KPIs
            cols = st.columns(4)
            
            with cols[0]:
                self.create_kpi_card(
                    "Total Market Cap",
                    self.format_large_number(summary.get('total_market_cap', 0)),
                    "💰"
                )
            
            with cols[1]:
                self.create_kpi_card(
                    "Total 24h Volume",
                    self.format_large_number(summary.get('total_volume_24h', 0)),
                    "📊"
                )
            
            with cols[2]:
                gainers = summary.get('gainers_count', 0)
                losers = summary.get('losers_count', 0)
                total = gainers + losers
                ratio = (gainers / total * 100) if total > 0 else 0
                
                # Create colored ratio text
                ratio_color = "positive-change" if ratio >= 50 else "negative-change"
                ratio_text = f"<span class='{ratio_color}'>{ratio:.1f}% positive</span>"
                
                self.create_kpi_card(
                    "Market Sentiment",
                    f"{gainers} 📈 / {losers} 📉",
                    "📈",
                    html_value=ratio_text
                )
            
            with cols[3]:
                self.create_kpi_card(
                    "Avg Price Change",
                    f"{summary.get('avg_price_change', 0):.2f}%",
                    "📉"
                )
            
            # Second row of KPIs
            st.markdown("<br>", unsafe_allow_html=True)
            cols = st.columns(4)
            
            with cols[0]:
                self.create_kpi_card(
                    "Total Coins",
                    f"{summary.get('total_coins', 0)}",
                    "🪙"
                )
            
            with cols[1]:
                self.create_kpi_card(
                    "Highest Gainer",
                    summary.get('top_gainer', 'N/A'),
                    "🏆"
                )
            
            with cols[2]:
                self.create_kpi_card(
                    "Most Volatile",
                    summary.get('most_volatile', 'N/A'),
                    "⚡"
                )
            
            with cols[3]:
                self.create_kpi_card(
                    "Avg Market Cap",
                    self.format_large_number(summary.get('avg_market_cap', 0)),
                    "📦"
                )
    
    def create_kpi_card(self, label, value, icon, html_value=None):
        """Create a styled KPI card"""
        display_value = html_value if html_value else value
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{icon} {label}</div>
                <div class="kpi-value">{display_value}</div>
            </div>
        """, unsafe_allow_html=True)
    
    def display_charts(self):
        """Display all charts in tabs"""
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
            with st.spinner("Loading market cap data..."):
                top_mcap = self.analyzer.get_top_5_by_market_cap()
                if top_mcap:
                    df = self.safe_convert_to_dataframe(top_mcap)
                    if not df.empty:
                        fig = px.bar(
                            df.head(10),
                            x='name',
                            y='market_cap',
                            title='Top 10 Cryptocurrencies by Market Cap',
                            labels={'market_cap': 'Market Cap (USD)', 'name': ''},
                            color='market_cap',
                            color_continuous_scale='Viridis',
                            text_auto='.2s'
                        )
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=500,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        fig.update_traces(textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("📊 No market cap data available")
        
        with col2:
            with st.spinner("Loading distribution data..."):
                if top_mcap and len(top_mcap) > 0:
                    df = self.safe_convert_to_dataframe(top_mcap[:10])
                    if not df.empty:
                        fig = px.pie(
                            df,
                            values='market_cap',
                            names='name',
                            title='Market Cap Distribution (Top 10)',
                            hole=0.3
                        )
                        fig.update_layout(
                            height=500,
                            showlegend=True,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("🥧 No distribution data available")
    
    def display_price_analysis(self):
        """Display price analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            with st.spinner("Loading gainers data..."):
                gainers = self.analyzer.get_top_5_gainers()
                if gainers:
                    df = self.safe_convert_to_dataframe(gainers)
                    if not df.empty:
                        # Create color scale based on values
                        colors = ['#ff4444' if x < 0 else '#ffbb33' if x < 5 else '#00C851' 
                                 for x in df['price_change_24h']]
                        
                        fig = px.bar(
                            df,
                            x='name',
                            y='price_change_24h',
                            title='Top 5 Gainers (24h)',
                            labels={'price_change_24h': 'Price Change %', 'name': ''},
                            color='price_change_24h',
                            color_continuous_scale=['red', 'yellow', 'green'],
                            text_auto='.2f'
                        )
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        fig.update_traces(texttemplate='%{y:.2f}%', textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("📈 No gainers data available")
        
        with col2:
            if gainers and len(gainers) > 0:
                with st.spinner("Loading price data..."):
                    df = self.safe_convert_to_dataframe(gainers)
                    if not df.empty:
                        df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                        
                        fig = px.scatter(
                            df,
                            x='name',
                            y='current_price',
                            title='Current Prices of Top Gainers',
                            labels={'current_price': 'Price (USD)', 'name': ''},
                            color='current_price',
                            color_continuous_scale='Plasma',
                            size=[20] * len(df)  # Fixed size for all points
                        )
                        
                        # Add text labels
                        fig.update_traces(
                            textposition='top center',
                            texttemplate='%{y:$.2f}',
                            marker=dict(size=30, line=dict(width=2, color='white'))
                        )
                        
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("💰 No price data available")
    
    def display_volume_analysis(self):
        """Display volume analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            with st.spinner("Loading volume data..."):
                volume_data = self.analyzer.get_volume_comparison()
                if volume_data:
                    df = self.safe_convert_to_dataframe(volume_data)
                    if not df.empty:
                        fig = px.bar(
                            df.head(10),
                            x='name',
                            y='total_volume',
                            title='Top 10 by Trading Volume',
                            labels={'total_volume': 'Volume (USD)', 'name': ''},
                            color='total_volume',
                            color_continuous_scale='Blues',
                            text_auto='.2s'
                        )
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        fig.update_traces(textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("📊 No volume data available")
        
        with col2:
            if volume_data and len(volume_data) > 0:
                with st.spinner("Loading volume-price relationship..."):
                    df = self.safe_convert_to_dataframe(volume_data)
                    if not df.empty:
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
                            color_continuous_scale='Viridis',
                            size=[30] * len(df)  # Fixed size
                        )
                        
                        fig.update_traces(
                            textposition='top center',
                            marker=dict(line=dict(width=2, color='white'))
                        )
                        
                        fig.update_layout(
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📈 No volume vs price data available")
    
    def display_volatility_analysis(self):
        """Display volatility analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            with st.spinner("Loading volatility data..."):
                volatility = self.analyzer.get_volatility_ranking()
                if volatility:
                    df = self.safe_convert_to_dataframe(volatility)
                    if not df.empty:
                        fig = px.bar(
                            df.head(10),
                            x='name',
                            y='volatility_score',
                            title='Top 10 Most Volatile Cryptocurrencies',
                            labels={'volatility_score': 'Volatility Score', 'name': ''},
                            color='volatility_score',
                            color_continuous_scale='Reds',
                            text_auto='.2s'
                        )
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        fig.update_traces(textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("⚡ No volatility data available")
        
        with col2:
            if volatility and len(volatility) > 0:
                with st.spinner("Loading volatility-volume relationship..."):
                    df = self.safe_convert_to_dataframe(volatility)
                    if not df.empty:
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
                            size=[30] * len(df)  # Fixed size
                        )
                        
                        fig.update_traces(
                            textposition='top center',
                            marker=dict(line=dict(width=2, color='white'))
                        )
                        
                        fig.update_layout(
                            height=400,
                            showlegend=False,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("📊 No volatility vs volume data available")

def main():
    """Main function to run the dashboard"""
    try:
        dashboard = CryptoDashboard()
        dashboard.run()
    except Exception as e:
        st.error(f"Failed to start dashboard: {e}")
        st.stop()

if __name__ == "__main__":
    main()