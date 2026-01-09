"""
Machine Learning Models Page
Displays ML model results and comparisons
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

st.set_page_config(page_title="ML Models", page_icon="🤖", layout="wide")

st.title("🤖 Machine Learning Models")
st.markdown("Predictive models using Spark MLlib")

# Sample ML results (in real app, load from actual model output)
@st.cache_data
def get_ml_results():
    """Get ML model results"""
    return {
        'logistic_regression': {
            'auc': 0.4307,
            'accuracy': 0.4493,
            'precision': 0.4532,
            'recall': 0.4493,
            'f1': 0.4497
        },
        'random_forest': {
            'auc': 0.6149,
            'accuracy': 0.5217,
            'precision': 0.5326,
            'recall': 0.5217,
            'f1': 0.5181
        }
    }

results = get_ml_results()

# Model selector
st.markdown("### 🎯 Model Selection")

model_choice = st.radio(
    "Choose a model to view details:",
    ["📊 Model Comparison", "🔵 Logistic Regression", "🌲 Random Forest"],
    horizontal=True
)

st.markdown("---")

if model_choice == "📊 Model Comparison":
    st.markdown("### ⚖️ Model Performance Comparison")
    
    # Metrics comparison
    metrics = ['AUC', 'Accuracy', 'Precision', 'Recall', 'F1-Score']
    lr_values = list(results['logistic_regression'].values())
    rf_values = list(results['random_forest'].values())
    
    comparison_df = pd.DataFrame({
        'Metric': metrics,
        'Logistic Regression': lr_values,
        'Random Forest': rf_values
    })
    
    # Display metrics cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    for idx, (col, metric) in enumerate(zip([col1, col2, col3, col4, col5], metrics)):
        with col:
            lr_val = lr_values[idx]
            rf_val = rf_values[idx]
            winner = "RF" if rf_val > lr_val else "LR"
            delta = rf_val - lr_val
            
            st.metric(
                label=metric,
                value=f"{rf_val:.4f}",
                delta=f"{delta:+.4f}",
                delta_color="normal" if delta > 0 else "inverse"
            )
            st.caption(f"🏆 Winner: {winner}")
    
    st.markdown("---")
    
    # Comparison chart
    st.markdown("#### 📊 Detailed Metrics Comparison")
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Logistic Regression',
        x=metrics,
        y=lr_values,
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name='Random Forest',
        x=metrics,
        y=rf_values,
        marker_color='green'
    ))
    
    fig.update_layout(
        barmode='group',
        title='Model Performance Metrics',
        xaxis_title='Metric',
        yaxis_title='Score',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Winner announcement
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🏆 Final Verdict")
        st.success(f"""
        **Winner: Random Forest** 🌲
        
        - Average Score: {np.mean(rf_values):.4f}
        - Best for this dataset due to better handling of non-linear patterns
        - Outperforms Logistic Regression in all metrics
        """)
    
    with col2:
        # Radar chart
        categories = metrics
        
        fig_radar = go.Figure()
        
        fig_radar.add_trace(go.Scatterpolar(
            r=lr_values,
            theta=categories,
            fill='toself',
            name='Logistic Regression'
        ))
        
        fig_radar.add_trace(go.Scatterpolar(
            r=rf_values,
            theta=categories,
            fill='toself',
            name='Random Forest'
        ))
        
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            showlegend=True,
            height=300
        )
        
        st.plotly_chart(fig_radar, use_container_width=True)

elif model_choice == "🔵 Logistic Regression":
    st.markdown("### 🔵 Logistic Regression Results")
    
    lr_results = results['logistic_regression']
    
    # Metrics display
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("AUC", f"{lr_results['auc']:.4f}")
    with col2:
        st.metric("Accuracy", f"{lr_results['accuracy']:.4f}")
    with col3:
        st.metric("Precision", f"{lr_results['precision']:.4f}")
    with col4:
        st.metric("Recall", f"{lr_results['recall']:.4f}")
    with col5:
        st.metric("F1-Score", f"{lr_results['f1']:.4f}")
    
    st.markdown("---")
    
    # Model coefficients
    st.markdown("#### 📊 Model Coefficients")
    
    coefficients = pd.DataFrame({
        'Feature': ['price_lag', 'volume_normalized', 'hour_sin', 
                   'hour_cos', 'day_of_week_sin', 'day_of_week_cos'],
        'Coefficient': [0.00804, -0.00483, 0.11717, 0.09309, 0.0, 0.0]
    })
    
    fig = px.bar(coefficients, x='Feature', y='Coefficient',
                title='Feature Coefficients',
                color='Coefficient',
                color_continuous_scale='RdBu',
                color_continuous_midpoint=0)
    st.plotly_chart(fig, use_container_width=True)
    
    # Confusion matrix
    st.markdown("#### 🎯 Sample Predictions")
    
    predictions = pd.DataFrame({
        'Actual': [0, 1, 1, 0, 0, 1, 1, 0, 1, 0],
        'Predicted': [0, 1, 1, 1, 0, 1, 0, 0, 0, 0],
        'Probability_0': [0.533, 0.482, 0.465, 0.482, 0.517, 0.453, 0.537, 0.507, 0.536, 0.518],
        'Probability_1': [0.467, 0.518, 0.535, 0.518, 0.483, 0.547, 0.463, 0.493, 0.464, 0.482]
    })
    
    st.dataframe(predictions, use_container_width=True)
    
    st.info("""
    **About Logistic Regression:**
    - Binary classification for price increase prediction
    - Uses sigmoid function for probability estimation
    - Good baseline model, but limited for non-linear patterns
    - Training time: Fast (~1-2 seconds)
    """)

elif model_choice == "🌲 Random Forest":
    st.markdown("### 🌲 Random Forest Results")
    
    rf_results = results['random_forest']
    
    # Metrics display
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("AUC", f"{rf_results['auc']:.4f}", delta="+0.1842")
    with col2:
        st.metric("Accuracy", f"{rf_results['accuracy']:.4f}", delta="+0.0724")
    with col3:
        st.metric("Precision", f"{rf_results['precision']:.4f}", delta="+0.0794")
    with col4:
        st.metric("Recall", f"{rf_results['recall']:.4f}", delta="+0.0724")
    with col5:
        st.metric("F1-Score", f"{rf_results['f1']:.4f}", delta="+0.0684")
    
    st.markdown("---")
    
    # Feature importance
    st.markdown("#### 🎯 Feature Importance")
    
    importance = pd.DataFrame({
        'Feature': ['day_of_week_cos', 'hour_cos', 'hour_sin', 'price_lag', 
                   'volume_normalized', 'day_of_week_sin'],
        'Importance': [0.2874, 0.2857, 0.2382, 0.1888, 0.0, 0.0]
    })
    importance = importance.sort_values('Importance', ascending=True)
    
    fig = px.bar(importance, y='Feature', x='Importance',
                title='Feature Importance',
                orientation='h',
                color='Importance',
                color_continuous_scale='Viridis')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Tree visualization (conceptual)
    st.markdown("#### 🌳 Random Forest Structure")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Number of Trees", "20")
    with col2:
        st.metric("Max Depth", "5")
    with col3:
        st.metric("Min Instances per Node", "1")
    
    st.markdown("---")
    
    # Sample predictions
    st.markdown("#### 🎯 Sample Predictions")
    
    predictions = pd.DataFrame({
        'Actual': [0, 1, 1, 0, 0, 1, 1, 0, 1, 0],
        'Predicted': [1, 0, 1, 1, 0, 1, 0, 1, 0, 0],
        'Probability_0': [0.275, 0.509, 0.457, 0.271, 0.624, 0.336, 0.531, 0.466, 0.522, 0.702],
        'Probability_1': [0.725, 0.491, 0.543, 0.729, 0.376, 0.664, 0.469, 0.534, 0.478, 0.298]
    })
    
    st.dataframe(predictions, use_container_width=True)
    
    st.success("""
    **Why Random Forest Wins:**
    - Ensemble of 20 decision trees reduces overfitting
    - Handles non-linear relationships better
    - More robust to outliers
    - Better generalization on test data
    - Training time: Moderate (~5-10 seconds)
    """)

# Additional information
st.markdown("---")
st.markdown("### 📚 Model Information")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    #### 🔵 Logistic Regression
    - **Type:** Linear classification
    - **Algorithm:** Gradient descent
    - **Features:** 6 engineered features
    - **Training data:** 407 records
    - **Test data:** 69 records
    - **Use case:** Quick baseline predictions
    """)

with col2:
    st.markdown("""
    #### 🌲 Random Forest
    - **Type:** Ensemble classification
    - **Algorithm:** Bootstrap aggregating
    - **Trees:** 20 decision trees
    - **Training data:** 407 records
    - **Test data:** 69 records
    - **Use case:** Production predictions
    """)

st.info("""
💡 **Model Features:**
- `price_lag`: Previous price value
- `volume_normalized`: Normalized trading volume
- `hour_sin/cos`: Time of day (cyclical encoding)
- `day_of_week_sin/cos`: Day of week (cyclical encoding)
""")

