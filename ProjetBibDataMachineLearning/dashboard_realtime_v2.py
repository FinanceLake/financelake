# Dashboard Temps Réel - Version Corrigée
import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
from pathlib import Path
import time
import threading

st.set_page_config(page_title="AMZN Streaming", layout="wide")

DATA_DIR = Path(__file__).parent / "data" / "gold"
PREDICTIONS_FILE = DATA_DIR / "redis_predictions.json"

st.title("📊 AMZN - Prédictions en Temps Réel")
st.subheader("Streaming via Redis + LSTM")

# Placeholder pour les graphiques et métriques
metric_placeholder = st.empty()
chart_placeholder = st.empty()
error_placeholder = st.empty()
table_placeholder = st.empty()
time_placeholder = st.empty()

def load_predictions():
    """Charge les prédictions"""
    if PREDICTIONS_FILE.exists():
        try:
            with open(PREDICTIONS_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def update_dashboard():
    """Met à jour le dashboard en continu"""
    last_count = 0
    
    while True:
        predictions = load_predictions()
        current_count = len(predictions)
        
        # Mettre à jour seulement si nouvelles données
        if current_count != last_count or current_count == 0:
            
            # Métriques
            with metric_placeholder.container():
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Prédictions", current_count)
                
                with col2:
                    if predictions:
                        last = predictions[-1]
                        st.metric("Dernier Prix", f"${last['current_price']:.2f}")
                    else:
                        st.metric("Dernier Prix", "N/A")
                
                with col3:
                    if predictions:
                        last = predictions[-1]
                        st.metric("Prédiction", f"${last['predicted_price']:.2f}")
                    else:
                        st.metric("Prédiction", "N/A")
                
                with col4:
                    if predictions:
                        last = predictions[-1]
                        color = "🔴" if last['percent_change'] < 0 else "🟢"
                        st.metric("Erreur", f"{last['percent_change']:+.2f}%", color)
                    else:
                        st.metric("Erreur", "N/A")
            
            # Graphique des prix
            if predictions:
                with chart_placeholder.container():
                    df = pd.DataFrame(predictions)
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df.index, y=df['current_price'],
                        name='Prix Actuel', mode='lines',
                        line=dict(color='blue', width=2)
                    ))
                    fig.add_trace(go.Scatter(
                        x=df.index, y=df['predicted_price'],
                        name='Prédiction LSTM', mode='lines',
                        line=dict(color='red', width=2, dash='dash')
                    ))
                    fig.update_layout(
                        title="Prix AMZN vs Prédictions",
                        xaxis_title="Message #", yaxis_title="Prix ($)",
                        height=500, template='plotly_dark'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Graphique erreurs
                with error_placeholder.container():
                    colors = ['green' if x < 0 else 'red' for x in df['percent_change']]
                    fig_error = go.Figure()
                    fig_error.add_trace(go.Bar(
                        x=df.index, y=df['percent_change'],
                        name='Erreur %', marker=dict(color=colors)
                    ))
                    fig_error.update_layout(
                        title="Erreur de Prédiction (%)",
                        xaxis_title="Message #", yaxis_title="Erreur (%)",
                        height=400, template='plotly_dark'
                    )
                    st.plotly_chart(fig_error, use_container_width=True)
                
                # Tableau
                with table_placeholder.container():
                    st.subheader("📋 Dernières Prédictions")
                    df_recent = pd.DataFrame(predictions[-15:])
                    st.dataframe(df_recent, use_container_width=True, height=400)
            else:
                with chart_placeholder.container():
                    st.info("En attente des données...")
            
            # Heure
            with time_placeholder.container():
                st.markdown(f"**Dernière mise à jour:** {time.strftime('%H:%M:%S')} | **Total:** {current_count} prédictions")
            
            last_count = current_count
        
        # Attendre 2 secondes avant de vérifier à nouveau
        time.sleep(2)

# Lancer la mise à jour en continu
update_dashboard()
