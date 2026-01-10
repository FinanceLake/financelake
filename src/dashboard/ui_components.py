import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def render_kpis(df, signal_row):
    # Get latest row
    latest = df.sort_values("window_end", ascending=False).iloc[0]
    
    k1, k2, k3, k4 = st.columns(4)
    
    with k1:
        # FIX: Use 'avg_price_change_pct' and .get() for safety
        pct_change = latest.get('avg_price_change_pct', 0.0)
        st.metric(
            label="Price", 
            value=f"${latest['avg_price']:.2f}", 
            delta=f"{pct_change:.2f}%"
        )
    
    with k2:
        if signal_row is not None:
            # Handle cases where signal might be missing in older rows
            sig = signal_row.get('signal', "WAIT")
            prob = signal_row.get('probability', 0.0)
            
            # Simple logic if signal string is missing but prob exists
            if sig == "WAIT" and prob > 0:
                sig = "BUY" if prob > 0.5 else "SELL"

            color = "normal" if sig == "BUY" else "inverse"
            st.metric("AI Signal", f"{sig}", f"{prob:.1%} Conf.", delta_color=color)
        else:
            st.metric("AI Signal", "WAITING", "No Data", delta_color="off")
            
    with k3:
        vol = latest.get('volatility', 0)
        st.metric("Volatility", f"{vol:.4f}", delta="High Risk" if vol > 1.0 else "Stable", delta_color="inverse")
        
    with k4:
        st.metric("Volume (Window)", f"{latest['total_volume']:,}")

def render_advanced_chart(df, symbol):
    df = df.sort_values("window_end")
    
    # Calculate Volume Delta
    # Use fillna(0) to prevent NaN errors on the first row
    if 'total_volume' in df.columns:
        df['volume_delta'] = df['total_volume'].diff().fillna(0).clip(lower=0)
    else:
        df['volume_delta'] = 0

    # Subplots
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.15,
        row_heights=[0.7, 0.3],
        subplot_titles=(f"{symbol} Price Action", "Trading Volume")
    )

    # Candle
    fig.add_trace(go.Candlestick(
        x=df['window_end'],
        open=df['avg_price'], high=df['max_price'],
        low=df['min_price'], close=df['avg_price'],
        name="Price"
    ), row=1, col=1)

    # Bar
    fig.add_trace(go.Bar(
        x=df['window_end'], y=df['volume_delta'],
        name="Volume", marker_color='rgba(100, 150, 250, 0.6)'
    ), row=2, col=1)

    fig.update_layout(
        height=650, 
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        showlegend=False,
        margin=dict(l=10, r=10, t=40, b=10)
    )
    
    st.plotly_chart(fig, use_container_width=True)