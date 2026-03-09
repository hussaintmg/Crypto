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

# Custom CSS for Premium Look
st.markdown("""
    <style>
    /* Main background */
    .stApp {
        background: radial-gradient(circle at 10% 20%, rgb(15, 20, 30) 0%, rgb(10, 10, 15) 90.2%);
        color: #E0E2E6;
    }
    
    /* Header styling */
    .main-header {
        font-size: 3.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #00C9FF 0%, #92FE9D 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2.5rem;
        padding: 1.5rem;
        text-shadow: 0 10px 20px rgba(0,201,255,0.2);
    }
    
    /* KPI Card styling with Glassmorphism */
    .kpi-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 20px;
        padding: 1.5rem;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
        margin: 0.8rem 0;
        overflow: hidden;
        position: relative;
    }
    
    .kpi-card:hover {
        transform: translateY(-10px) scale(1.02);
        box-shadow: 0 15px 40px rgba(0, 201, 255, 0.2);
        border: 1px solid rgba(0, 201, 255, 0.4);
    }
    
    .kpi-card::after {
        content: '';
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(circle, rgba(0, 201, 255, 0.1) 0%, transparent 70%);
        opacity: 0;
        transition: opacity 0.4s;
    }
    
    .kpi-card:hover::after {
        opacity: 1;
    }

    .kpi-label {
        font-size: 0.9rem;
        color: #A0AEC0;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 1.5px;
        margin-bottom: 0.5rem;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 800;
        color: #FFFFFF;
        text-shadow: 0 0 10px rgba(255,255,255,0.2);
    }
    
    /* Price changes */
    .positive-change {
        color: #00E676;
        font-weight: 700;
        text-shadow: 0 0 8px rgba(0,230,118,0.4);
    }
    .negative-change {
        color: #FF5252;
        font-weight: 700;
        text-shadow: 0 0 8px rgba(255,82,82,0.4);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: rgba(15, 20, 30, 0.95);
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1.5rem;
        background-color: rgba(255, 255, 255, 0.05);
        padding: 0.7rem;
        border-radius: 15px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        color: #A0AEC0;
        transition: all 0.3s;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        color: #00C9FF;
        background-color: rgba(0, 201, 255, 0.1);
    }
    
    .stTabs [aria-selected="true"] {
        background-color: rgba(0, 201, 255, 0.2) !important;
        color: #00C9FF !important;
        font-weight: 700 !important;
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
            
            # ETL Status with better visualization
            st.subheader("🔄 ETL Status")
            latest_ts = self.get_latest_timestamp()
            if latest_ts:
                time_diff = datetime.now() - latest_ts
                if time_diff.seconds < 330:  # ~5.5 minutes
                    st.success(f"✅ Active (Last: {latest_ts.strftime('%H:%M:%S')})")
                else:
                    st.warning(f"⚠️ Latent (Last: {latest_ts.strftime('%H:%M:%S')})")
            else:
                st.error("❌ No data available")

            st.divider()
            st.markdown("### 📡 API DataSource")
            st.caption("Powerd by CoinGecko Public API")
        
        # Main content
        try:
            self.display_kpis()
            st.markdown("---")
            self.display_charts()
            st.markdown("---")
            self.display_asset_table()
        except Exception as e:
            st.error(f"Error displaying data: {e}")
            st.exception(e)
        
        # Auto-refresh logic
        if auto_refresh:
            time.sleep(self.refresh_rate)
            st.rerun()

    def display_asset_table(self):
        """Display a beautiful assets table"""
        st.subheader("💎 Top Assets Breakdown")
        
        data = self.analyzer.get_top_5_by_market_cap()
        if data:
            df = self.safe_convert_to_dataframe(data)
            if not df.empty:
                # Custom HTML table for premium look
                st.markdown("""
                <style>
                .asset-thumb { width: 30px; border-radius: 50%; margin-right: 10px; vertical-align: middle; }
                .price-val { font-family: 'Courier New', monospace; font-weight: bold; }
                </style>
                """, unsafe_allow_html=True)
                
                # We can use st.dataframe but for "WOW" factor, let's use a nice custom list or styled dataframe
                # Formatting price and market cap for display
                df_display = df.copy()
                df_display['price'] = df_display['current_price'].apply(lambda x: f"${x:,.2f}" if x >= 1 else f"${x:,.6f}")
                df_display['market_cap_fmt'] = df_display['market_cap'].apply(self.format_large_number)
                
                # Using columns for a pseudo-table with images
                cols = st.columns([0.5, 2, 1.5, 1.5, 1.5])
                cols[0].write("**#**")
                cols[1].write("**Asset**")
                cols[2].write("**Price**")
                cols[3].write("**Market Cap**")
                cols[4].write("**24h Change**")
                
                st.divider()
                
                for _, row in df_display.iterrows():
                    c = st.columns([0.5, 2, 1.5, 1.5, 1.5])
                    c[0].write(str(int(row['market_cap_rank'])))
                    
                    # Convert to float for safe processing
                    try:
                        price = float(row['current_price'])
                        change = float(row['price_change_24h'])
                    except (TypeError, ValueError):
                        price = 0.0
                        change = 0.0

                    # Image + Name
                    asset_html = f'''
                    <div style="display: flex; align-items: center;">
                        <span style="font-weight: 600;">{row['name']}</span>
                        <span style="color: #A0AEC0; margin-left: 8px; font-size: 0.8em;">{row['symbol'].upper()}</span>
                    </div>
                    '''
                    c[1].markdown(asset_html, unsafe_allow_html=True)
                    
                    price_fmt = f"${price:,.2f}" if price >= 1 else f"${price:,.6f}"
                    c[2].write(price_fmt)
                    c[3].write(row['market_cap_fmt'])
                    
                    # Price change with color
                    change_cls = "positive-change" if change >= 0 else "negative-change"
                    change_sign = "+" if change >= 0 else ""
                    c[4].markdown(f'<span class="{change_cls}">{change_sign}{change:.2f}%</span>', unsafe_allow_html=True)

        else:
            st.info("No asset data found in database.")

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
        try:
            num = float(num)
        except (TypeError, ValueError):
            return "$0.00"
            
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
                    "Average Price",
                    f"${summary.get('avg_price', 0):,.2f}",
                    "💵"
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
                            title='Top Cryptocurrencies by Market Cap',
                            labels={'market_cap': 'Market Cap (USD)', 'name': ''},
                            color='market_cap',
                            color_continuous_scale='Viridis',
                            text_auto='.2s'
                        )
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=500,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='white')
                        )
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
                            title='Market Share (Top 10)',
                            hole=0.4
                        )
                        fig.update_layout(
                            height=500,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='white')
                        )
                        st.plotly_chart(fig, use_container_width=True)
    
    def display_price_analysis(self):
        """Display price analysis charts"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚀 Top 5 Gainers (24h)")
            gainers = self.analyzer.get_top_5_gainers()
            if gainers:
                df = self.safe_convert_to_dataframe(gainers)
                fig = px.bar(
                    df, x='name', y='price_change_24h',
                    color='price_change_24h',
                    color_continuous_scale='RdYlGn',
                    text_auto='.2f'
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white')
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📈 Price Trend (Last 24h)")
            trends = self.analyzer.get_price_trends(hours=24)
            if trends:
                df = self.safe_convert_to_dataframe(trends)
                if not df.empty:
                    # Filter for top 5 coins to keep it clean
                    top_coins = df['name'].unique()[:5]
                    df_filtered = df[df['name'].isin(top_coins)]
                    
                    fig = px.line(
                        df_filtered, x='hour', y='avg_price', color='name',
                        labels={'avg_price': 'Price (USD)', 'hour': 'Time'},
                        render_mode='svg'
                    )
                    fig.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='white'),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No historical trend data yet. Wait for more ETL cycles.")
            else:
                st.info("No trend data available.")

    
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