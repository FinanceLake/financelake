# Visualisations
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import json
import pandas as pd
from datetime import datetime
import numpy as np
import config

class Visualization:
    def __init__(self):
        self.config = config.Config
        # Palette de couleurs professionnelle
        self.colors = {
            'actual': '#1f77b4',
            'predicted': '#ff7f0e',
            'future': '#2ca02c',
            'train': '#9467bd',
            'test': '#8c564b',
            'background': '#ffffff',
            'grid': '#e6e6e6',
            'text': '#2c3e50'
        }
        
        # Template personnalisé
        self.template = go.layout.Template(
            layout=go.Layout(
                plot_bgcolor=self.colors['background'],
                paper_bgcolor=self.colors['background'],
                font=dict(color=self.colors['text'], family="Arial, sans-serif"),
                title=dict(x=0.5, xanchor='center', font=dict(size=20)),
                xaxis=dict(
                    gridcolor=self.colors['grid'],
                    gridwidth=1,
                    showgrid=True,
                    zeroline=False,
                    showline=True,
                    linewidth=2,
                    linecolor=self.colors['grid'],
                    mirror=True
                ),
                yaxis=dict(
                    gridcolor=self.colors['grid'],
                    gridwidth=1,
                    showgrid=True,
                    zeroline=False,
                    showline=True,
                    linewidth=2,
                    linecolor=self.colors['grid'],
                    mirror=True
                )
            )
        )
    
    def create_main_visualization(self, historical_data, predictions, model_metrics=None):
        """Crée la visualisation principale avec historique et prédictions"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Historique des prix et prédictions',
                'Performance du modèle',
                'Prédictions des 3 prochains jours',
                'Résidus'
            ),
            specs=[
                [{"colspan": 2}, None],
                [{}, {}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.15
        )
        
        # 1. Historique des prix
        fig.add_trace(
            go.Scatter(
                x=historical_data[self.config.DATE_COL],
                y=historical_data[self.config.PRICE_COL],
                mode='lines',
                name='Prix historique',
                line=dict(color=self.colors['actual'], width=2),
                hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br>' +
                            '<b>Prix:</b> $%{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Ajout des prédictions futures
        pred_dates = [datetime.strptime(p['date'], '%Y-%m-%d') for p in predictions]
        pred_prices = [p['predicted_price'] for p in predictions]
        
        fig.add_trace(
            go.Scatter(
                x=pred_dates,
                y=pred_prices,
                mode='lines+markers',
                name='Prédictions',
                line=dict(color=self.colors['future'], width=3, dash='dash'),
                marker=dict(size=10, symbol='diamond'),
                hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br>' +
                            '<b>Prédiction:</b> $%{y:.2f}<br>' +
                            '<b>Jour:</b> %{customdata}<extra></extra>',
                customdata=[p['day'] for p in predictions]
            ),
            row=1, col=1
        )
        
        # Ajout des points avec annotations
        for i, pred in enumerate(predictions):
            fig.add_annotation(
                x=pred_dates[i],
                y=pred_prices[i],
                text=f"${pred_prices[i]:.2f}",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor=self.colors['future'],
                ax=0,
                ay=-30,
                font=dict(size=11, color=self.colors['future']),
                row=1, col=1
            )
        
        # 2. Graphique des prédictions (zoom)
        fig.add_trace(
            go.Scatter(
                x=pred_dates,
                y=pred_prices,
                mode='lines+markers+text',
                name='Prédictions détaillées',
                line=dict(color=self.colors['future'], width=3),
                marker=dict(size=12, color=self.colors['future']),
                text=[f"${p:.2f}" for p in pred_prices],
                textposition="top center",
                hovertemplate='<b>Jour %{customdata}</b><br>' +
                            '<b>Date:</b> %{x|%Y-%m-%d}<br>' +
                            '<b>Prédiction:</b> $%{y:.2f}<extra></extra>',
                customdata=[p['day'] for p in predictions]
            ),
            row=2, col=2
        )
        
        # Ajout d'une zone de confiance (simulée)
        for i, pred in enumerate(predictions):
            confidence_interval = pred_prices[i] * 0.02  # 2% d'intervalle
            fig.add_trace(
                go.Scatter(
                    x=[pred_dates[i], pred_dates[i]],
                    y=[pred_prices[i] - confidence_interval, pred_prices[i] + confidence_interval],
                    mode='lines',
                    line=dict(width=8, color='rgba(44, 160, 44, 0.3)'),
                    showlegend=False,
                    hoverinfo='skip'
                ),
                row=2, col=2
            )
        
        # 3. Métriques du modèle (graphique à barres)
        if model_metrics:
            metrics_names = list(model_metrics.keys())
            metrics_values = list(model_metrics.values())
            
            fig.add_trace(
                go.Bar(
                    x=metrics_names,
                    y=metrics_values,
                    name='Métriques',
                    marker_color=[self.colors['predicted']] * len(metrics_names),
                    text=[f'{v:.4f}' for v in metrics_values],
                    textposition='auto',
                    hovertemplate='<b>%{x}</b><br>Valeur: %{y:.4f}<extra></extra>'
                ),
                row=2, col=1
            )
        
        # Mise à jour du layout
        fig.update_layout(
            title=dict(
                text="<b>Analyse prédictive - AMZN (Amazon)</b><br>" +
                     "<span style='font-size:14px; color:#666'>Prédictions des 3 prochains jours avec modèle LSTM</span>",
                x=0.5,
                y=0.98
            ),
            template=self.template,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor='rgba(255, 255, 255, 0.8)',
                bordercolor='#e6e6e6',
                borderwidth=1
            ),
            height=900,
            width=1200,
            hovermode='x unified'
        )
        
        # Mise à jour des axes
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Prix ($)", row=1, col=1)
        fig.update_xaxes(title_text="Métriques", row=2, col=1)
        fig.update_yaxes(title_text="Valeur", row=2, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=2)
        fig.update_yaxes(title_text="Prix ($)", row=2, col=2)
        
        # Ajout d'annotations informatives
        fig.add_annotation(
            text=f"Généré le {datetime.now().strftime('%Y-%m-%d %H:%M')} | Modèle LSTM ({self.config.LOOKBACK} jours lookback)",
            xref="paper", yref="paper",
            x=0.5, y=-0.08,
            showarrow=False,
            font=dict(size=10, color="#7f8c8d")
        )
        
        return fig
    
    def save_visualization(self, fig, filename=None):
        """Sauvegarde la visualisation"""
        if filename is None:
            filename = self.config.VISUALIZATION_FILE
        
        output_path = self.config.GOLD_DIR / filename
        
        # Sauvegarde en HTML (interactif)
        fig.write_html(str(output_path))
        
        # Sauvegarde en PNG (image statique)
        png_path = self.config.GOLD_DIR / "amzn_predictions.png"
        fig.write_image(str(png_path), width=1200, height=900)
        
        print(f"Visualisation sauvegardée:")
        print(f"  - HTML: {output_path}")
        print(f"  - PNG: {png_path}")
        
        return output_path
    
    def display_visualization(self, fig):
        """Affiche la visualisation"""
        fig.show()

# Pour activer l'environnement virtuel (Windows)
# .\.venv\Scripts\activate