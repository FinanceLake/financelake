"""
Advanced Visualizations Page
Charts and technical indicators
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os

st.set_page_config(page_title="Visualizations", page_icon="📈", layout="wide")

st.title("📈 Advanced Visualizations")
st.markdown("Interactive charts and technical indicators")

# Load data
@st.cache_data(ttl=30)
def load_viz_data():
    """Load data for visualizations"""
    try:
        if os.path.exists("./output/stock_data"):
            parquet_files = [f for f in os.listdir("./output/stock_data") 
                           if f.endswith('.parquet')]
            if parquet_files:
                dfs = []
                for file in parquet_files[:10]:
                    df = pd.read_parquet(f"./output/stock_data/{file}")
                    dfs.append(df)
                return pd.concat(dfs, ignore_index=True)
        
        # Generate sample data
        np.random.seed(42)
        dates = pd.date_range(start='2024-01-01', periods=1000, freq='1min')
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        data = []
        
        for symbol in symbols:
            base_price = {'AAPL': 150, 'GOOGL': 130, 'MSFT': 350, 'AMZN': 140, 'TSLA': 200}[symbol]
            prices = base_price + np.cumsum(np.random.randn(1000) * 2)
            volumes = np.random.randint(500000, 2000000, 1000)
            
            for i, date in enumerate(dates):
                data.append({
                    'symbol': symbol,
                    'price': prices[i],
                    'volume': volumes[i],
                    'timestamp': date,
                    'price_change': prices[i] - prices[i-1] if i > 0 else 0
                })
        
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def calculate_rsi(prices, period=14):
    """Calculate RSI (Relative Strength Index)"""
    deltas = np.diff(prices)
    seed = deltas[:period+1]
    up = seed[seed >= 0].sum()/period
    down = -seed[seed < 0].sum()/period
    rs = up/down if down != 0 else 0
    rsi = np.zeros_like(prices)
    rsi[:period] = 100. - 100./(1. + rs)
    
    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up/down if down != 0 else 0
        rsi[i] = 100. - 100./(1. + rs)
    
    return rsi

df = load_viz_data()

if not df.empty:
    # Visualization selector
    viz_type = st.selectbox(
        "Select Visualization:",
        ["📊 Price Evolution", "📦 Volume Analysis", "⚡ Volatility Comparison", 
         "🔗 Correlation Matrix", "📈 RSI Indicator"]
    )
    
    st.markdown("---")
    
    if viz_type == "📊 Price Evolution":
        st.markdown("### 📊 Stock Price Evolution Over Time")
        
        # Symbol selector
        selected_symbols = st.multiselect(
            "Select symbols to display:",
            df['symbol'].unique().tolist(),
            default=df['symbol'].unique().tolist()[:3]
        )
        
        if selected_symbols:
            filtered_df = df[df['symbol'].isin(selected_symbols)]
            
            fig = px.line(
                filtered_df,
                x='timestamp',
                y='price',
                color='symbol',
                title='Stock Price Trends',
                labels={'price': 'Price ($)', 'timestamp': 'Time'},
                template='plotly_white'
            )
            
            fig.update_layout(
                height=500,
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistics
            st.markdown("#### 📊 Price Statistics")
            stats_df = filtered_df.groupby('symbol').agg({
                'price': ['mean', 'std', 'min', 'max']
            }).round(2)
            stats_df.columns = ['Average', 'Std Dev', 'Minimum', 'Maximum']
            st.dataframe(stats_df, use_container_width=True)
    
    elif viz_type == "📦 Volume Analysis":
        st.markdown("### 📦 Trading Volume Analysis")
        
        # Volume by symbol
        volume_df = df.groupby('symbol')['volume'].sum().reset_index()
        volume_df = volume_df.sort_values('volume', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                volume_df,
                x='symbol',
                y='volume',
                title='Total Trading Volume by Symbol',
                color='volume',
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(
                volume_df,
                values='volume',
                names='symbol',
                title='Volume Distribution',
                hole=0.4
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Volume over time
        st.markdown("#### Volume Trends Over Time")
        
        selected_symbol = st.selectbox("Select a symbol:", df['symbol'].unique())
        symbol_df = df[df['symbol'] == selected_symbol].copy()
        
        fig = px.area(
            symbol_df,
            x='timestamp',
            y='volume',
            title=f'{selected_symbol} Trading Volume Over Time',
            labels={'volume': 'Volume', 'timestamp': 'Time'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    elif viz_type == "⚡ Volatility Comparison":
        st.markdown("### ⚡ Stock Volatility Comparison")
        
        volatility_df = df.groupby('symbol')['price'].std().reset_index()
        volatility_df.columns = ['symbol', 'volatility']
        volatility_df = volatility_df.sort_values('volatility', ascending=True)
        
        fig = px.bar(
            volatility_df,
            y='symbol',
            x='volatility',
            orientation='h',
            title='Volatility (Price Standard Deviation)',
            color='volatility',
            color_continuous_scale='Reds',
            labels={'volatility': 'Volatility ($)', 'symbol': 'Symbol'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Box plot for price distribution
        st.markdown("#### Price Distribution by Symbol")
        
        fig = px.box(
            df,
            x='symbol',
            y='price',
            color='symbol',
            title='Price Distribution (Box Plot)',
            labels={'price': 'Price ($)'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Volatility metrics
        st.markdown("#### Volatility Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            most_volatile = volatility_df.iloc[-1]
            st.metric("Most Volatile", most_volatile['symbol'], 
                     f"${most_volatile['volatility']:.2f}")
        
        with col2:
            least_volatile = volatility_df.iloc[0]
            st.metric("Least Volatile", least_volatile['symbol'],
                     f"${least_volatile['volatility']:.2f}")
        
        with col3:
            avg_volatility = volatility_df['volatility'].mean()
            st.metric("Average Volatility", f"${avg_volatility:.2f}")
    
    elif viz_type == "🔗 Correlation Matrix":
        st.markdown("### 🔗 Price Correlation Matrix")
        
        # Pivot data for correlation
        pivot_df = df.pivot_table(
            values='price',
            index='timestamp',
            columns='symbol'
        )
        
        # Calculate correlation
        corr_matrix = pivot_df.corr()
        
        # Heatmap
        fig = px.imshow(
            corr_matrix,
            text_auto='.2f',
            aspect='auto',
            title='Stock Price Correlation Matrix',
            color_continuous_scale='RdBu',
            zmin=-1,
            zmax=1
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        st.info("""
        💡 **Interpreting Correlation:**
        - **+1**: Perfect positive correlation (prices move together)
        - **0**: No correlation
        - **-1**: Perfect negative correlation (prices move oppositely)
        """)
        
        # Scatter matrix
        st.markdown("#### Pairwise Price Relationships")
        
        fig = px.scatter_matrix(
            pivot_df,
            dimensions=pivot_df.columns,
            title='Pairwise Stock Price Relationships',
            height=700
        )
        fig.update_traces(diagonal_visible=False, showupperhalf=False)
        st.plotly_chart(fig, use_container_width=True)
    
    elif viz_type == "📈 RSI Indicator":
        st.markdown("### 📈 RSI (Relative Strength Index) Indicator")
        
        st.info("""
        **RSI Interpretation:**
        - **RSI > 70**: Overbought (potential sell signal)
        - **RSI < 30**: Oversold (potential buy signal)
        - **RSI 30-70**: Neutral zone
        """)
        
        selected_symbol = st.selectbox("Select a symbol:", df['symbol'].unique(), key='rsi_symbol')
        rsi_period = st.slider("RSI Period:", 7, 21, 14)
        
        symbol_df = df[df['symbol'] == selected_symbol].sort_values('timestamp').copy()
        
        if len(symbol_df) > rsi_period:
            # Calculate RSI
            symbol_df['rsi'] = calculate_rsi(symbol_df['price'].values, period=rsi_period)
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=(f'{selected_symbol} Price', 'RSI Indicator')
            )
            
            # Price chart
            fig.add_trace(
                go.Scatter(x=symbol_df['timestamp'], y=symbol_df['price'],
                          name='Price', line=dict(color='blue')),
                row=1, col=1
            )
            
            # RSI chart
            fig.add_trace(
                go.Scatter(x=symbol_df['timestamp'], y=symbol_df['rsi'],
                          name='RSI', line=dict(color='purple')),
                row=2, col=1
            )
            
            # RSI levels
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=2, col=1)
            
            fig.update_xaxes(title_text="Time", row=2, col=1)
            fig.update_yaxes(title_text="Price ($)", row=1, col=1)
            fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
            
            fig.update_layout(height=600, hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            
            # Current RSI status
            current_rsi = symbol_df['rsi'].iloc[-1]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Current RSI", f"{current_rsi:.2f}")
            
            with col2:
                if current_rsi > 70:
                    st.warning("⚠️ OVERBOUGHT")
                elif current_rsi < 30:
                    st.success("✅ OVERSOLD")
                else:
                    st.info("➡️ NEUTRAL")
            
            with col3:
                signal = "SELL" if current_rsi > 70 else "BUY" if current_rsi < 30 else "HOLD"
                st.metric("Signal", signal)
        
        else:
            st.warning(f"⚠️ Need at least {rsi_period} data points to calculate RSI")

else:
    st.warning("⚠️ No data available. Please start the pipeline first.")
    st.info("👉 Go to the **Home** page and click **Start Pipeline**")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>📈 Advanced visualizations powered by Plotly</p>
</div>
""", unsafe_allow_html=True)

