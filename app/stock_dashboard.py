import dash
from dash import dcc, html, Input, Output, callback, dash_table
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from data_fetcher import fetch_stock_data
import logging
from typing import Optional, Dict, Any
import traceback
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDashboard:
    """
    FinanceLake Stock Visualization Dashboard
    
    Interactive dashboard for displaying stock prices and key financial indicators
    with support for multiple symbols and customizable date ranges.
    """
    
    def __init__(self):
        self.app = dash.Dash(__name__, external_stylesheets=[
            'https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css',
            'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
        ])
        
        # Default configuration
        self.default_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META']
        self.default_date_range = [
            (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
            datetime.now().strftime('%Y-%m-%d')
        ]
        
        self.setup_layout()
        self.setup_callbacks()
    
    def setup_layout(self):
        """Setup the dashboard layout with Bootstrap styling"""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1([
                    html.I(className="fas fa-chart-line me-3"),
                    "FinanceLake Stock Dashboard"
                ], className="text-center text-primary mb-0"),
                html.P("Real-time Financial Data Visualization & Analysis", 
                      className="text-center text-muted")
            ], className="bg-light py-4 mb-4"),
            
            # Controls Section
            html.Div([
                html.Div([
                    html.H4("Controls", className="mb-3"),
                    
                    # Stock Symbol Input
                    html.Div([
                        html.Label("Stock Symbol:", className="form-label fw-bold"),
                        dcc.Dropdown(
                            id='stock-symbol-dropdown',
                            options=[{'label': symbol, 'value': symbol} for symbol in self.default_symbols],
                            value='AAPL',
                            searchable=True,
                            placeholder="Enter or select stock symbol (e.g., AAPL)",
                            className="mb-3"
                        )
                    ]),
                    
                    # Date Range Picker
                    html.Div([
                        html.Label("Date Range:", className="form-label fw-bold"),
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date=self.default_date_range[0],
                            end_date=self.default_date_range[1],
                            display_format='YYYY-MM-DD',
                            min_date_allowed=datetime(2000, 1, 1),
                            max_date_allowed=datetime.now(),
                            className="mb-3"
                        )
                    ]),
                    
                    # Indicator Toggles
                    html.Div([
                        html.Label("Indicators:", className="form-label fw-bold"),
                        dcc.Checklist(
                            id='indicator-checklist',
                            options=[
                                {'label': ' 50-Day MA', 'value': 'ma50'},
                                {'label': ' 200-Day MA', 'value': 'ma200'},
                                {'label': ' Daily Returns', 'value': 'returns'},
                                {'label': ' Volatility', 'value': 'volatility'},
                                {'label': ' Volume', 'value': 'volume'}
                            ],
                            value=['ma50', 'ma200', 'volume'],
                            className="mb-3",
                            inputClassName="me-2"
                        )
                    ]),
                    
                    # Update Button (optional manual refresh)
                    html.Button(
                        [html.I(className="fas fa-sync-alt me-2"), "Update Dashboard"],
                        id='update-button',
                        className="btn btn-primary w-100",
                        n_clicks=0
                    )
                    
                ], className="col-md-3"),
                
                # Charts Section
                html.Div([
                    # Loading Indicator
                    dcc.Loading(
                        id="loading",
                        children=[
                            # Alert for errors/messages
                            html.Div(id='alert-message'),
                            
                            # Main Price Chart
                            dcc.Graph(id='price-chart', style={'height': '60vh'}),
                            
                            # Secondary Charts Row
                            html.Div([
                                html.Div([
                                    dcc.Graph(id='returns-chart', style={'height': '30vh'})
                                ], className="col-md-6"),
                                
                                html.Div([
                                    dcc.Graph(id='volume-chart', style={'height': '30vh'})
                                ], className="col-md-6")
                            ], className="row mt-3"),
                            
                            # Statistics Table
                            html.Div([
                                html.H5("Key Statistics", className="mt-4 mb-3"),
                                html.Div(id='stats-table')
                            ])
                        ],
                        type="circle",
                        color="#0d6efd"
                    )
                ], className="col-md-9")
                
            ], className="row"),
            
        ], className="container-fluid")
    
    def setup_callbacks(self):
        """Setup dashboard callbacks for interactivity"""
        
        @self.app.callback(
            [Output('alert-message', 'children'),
             Output('price-chart', 'figure'),
             Output('returns-chart', 'figure'),
             Output('volume-chart', 'figure'),
             Output('stats-table', 'children')],
            [Input('stock-symbol-dropdown', 'value'),
             Input('date-picker-range', 'start_date'),
             Input('date-picker-range', 'end_date'),
             Input('indicator-checklist', 'value'),
             Input('update-button', 'n_clicks')]
        )
        def update_dashboard(symbol, start_date, end_date, indicators, n_clicks):
            """Main callback to update all dashboard components"""
            try:
                if not symbol:
                    return self._create_error_alert("Please select a stock symbol"), {}, {}, {}, ""
                
                # Fetch and process data
                data = self._fetch_stock_data(symbol, start_date, end_date)
                if data is None or data.empty:
                    return self._create_error_alert(f"No data available for {symbol} in the selected date range"), {}, {}, {}, ""
                
                # Calculate indicators
                processed_data = self._calculate_indicators(data)
                
                # Create charts
                price_fig = self._create_price_chart(processed_data, symbol, indicators)
                returns_fig = self._create_returns_chart(processed_data, symbol, indicators)
                volume_fig = self._create_volume_chart(processed_data, symbol, indicators)
                
                # Create statistics table
                stats_table = self._create_stats_table(processed_data, symbol)
                
                success_alert = html.Div([
                    html.I(className="fas fa-check-circle me-2"),
                    f"Successfully loaded data for {symbol}"
                ], className="alert alert-success alert-dismissible")
                
                return success_alert, price_fig, returns_fig, volume_fig, stats_table
                
            except Exception as e:
                logger.error(f"Error updating dashboard: {str(e)}")
                logger.error(traceback.format_exc())
                error_msg = f"Error loading data: {str(e)}"
                return self._create_error_alert(error_msg), {}, {}, {}, ""
    
    def _fetch_stock_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch stock data from the FinanceLake backend API"""
        try:
            # Convert dates to string format if needed
            start = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            end = pd.to_datetime(end_date).strftime('%Y-%m-%d')
            
            # Validate date range
            if pd.to_datetime(start) >= pd.to_datetime(end):
                raise ValueError("Start date must be before end date")
            
            if pd.to_datetime(end) > datetime.now():
                end = datetime.now().strftime('%Y-%m-%d')
            
            # Fetch data using data_fetcher
            data = fetch_stock_data(symbol, start, end)
            
            if data.empty:
                logger.warning(f"No data returned for {symbol} from {start} to {end}")
                return None
            
            # Ensure required columns
            required_columns = ['date', 'close', 'volume']
            if not all(col in data.columns for col in required_columns):
                raise ValueError("API response missing required columns")
            
            # Rename columns to match expected format
            data = data.rename(columns={'date': 'Date', 'close': 'Close', 'volume': 'Volume'})
            logger.info(f"Successfully fetched {len(data)} records for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    def _calculate_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        df = data.copy()
        
        # Moving Averages
        df['MA50'] = df['Close'].rolling(window=50).mean()
        df['MA200'] = df['Close'].rolling(window=200).mean()
        
        # Daily Returns
        df['Daily_Return'] = df['Close'].pct_change() * 100
        
        # Volatility (30-day rolling standard deviation)
        df['Volatility'] = df['Daily_Return'].rolling(window=30).std()
        
        # Price change
        df['Price_Change'] = df['Close'].diff()
        df['Price_Change_Pct'] = df['Price_Change'] / df['Close'].shift(1) * 100
        
        return df
    
    def _create_price_chart(self, data: pd.DataFrame, symbol: str, indicators: list) -> go.Figure:
        """Create the main price chart with moving averages and volatility"""
        fig = make_subplots(
            rows=1, cols=1,
            subplot_titles=[f'{symbol} Stock Price & Indicators'],
            specs=[[{"secondary_y": True}]],
            vertical_spacing=0.1
        )
        
        # Main price line
        fig.add_trace(
            go.Scatter(
                x=data['Date'],
                y=data['Close'],
                mode='lines',
                name='Close Price',
                line=dict(color='#0d6efd', width=2),
                hovertemplate='<b>Date:</b> %{x}<br><b>Price:</b> $%{y:.2f}<extra></extra>'
            ),
            secondary_y=False
        )
        
        # Moving averages
        if 'ma50' in indicators and 'MA50' in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data['Date'],
                    y=data['MA50'],
                    mode='lines',
                    name='50-Day MA',
                    line=dict(color='orange', width=1, dash='dash'),
                    hovertemplate='<b>50-Day MA:</b> $%{y:.2f}<extra></extra>'
                ),
                secondary_y=False
            )
        
        if 'ma200' in indicators and 'MA200' in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data['Date'],
                    y=data['MA200'],
                    mode='lines',
                    name='200-Day MA',
                    line=dict(color='red', width=1, dash='dot'),
                    hovertemplate='<b>200-Day MA:</b> $%{y:.2f}<extra></extra>'
                ),
                secondary_y=False
            )
        
        if 'volatility' in indicators and 'Volatility' in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data['Date'],
                    y=data['Volatility'],
                    mode='lines',
                    name='Volatility (30-day)',
                    line=dict(color='purple', width=1),
                    hovertemplate='<b>Volatility:</b> %{y:.2f}%<extra></extra>'
                ),
                secondary_y=True
            )
        
        fig.update_layout(
            title=dict(text=f'{symbol} Stock Price Analysis', x=0.5),
            xaxis_title='Date',
            yaxis_title='Price ($)',
            yaxis2_title='Volatility (%)',
            hovermode='x unified',
            showlegend=True,
            template='plotly_white'
        )
        
        return fig
    
    def _create_returns_chart(self, data: pd.DataFrame, symbol: str, indicators: list) -> go.Figure:
        """Create daily returns chart"""
        fig = go.Figure()
        
        if 'returns' in indicators and 'Daily_Return' in data.columns:
            # Color bars based on positive/negative returns
            colors = ['green' if x >= 0 else 'red' for x in data['Daily_Return']]
            
            fig.add_trace(
                go.Bar(
                    x=data['Date'],
                    y=data['Daily_Return'],
                    name='Daily Returns',
                    marker_color=colors,
                    hovertemplate='<b>Date:</b> %{x}<br><b>Return:</b> %{y:.2f}%<extra></extra>'
                )
            )
        
        fig.update_layout(
            title=dict(text=f'{symbol} Daily Returns (%)', x=0.5),
            xaxis_title='Date',
            yaxis_title='Return (%)',
            template='plotly_white',
            showlegend=False
        )
        
        return fig
    
    def _create_volume_chart(self, data: pd.DataFrame, symbol: str, indicators: list) -> go.Figure:
        """Create volume chart"""
        fig = go.Figure()
        
        if 'volume' in indicators and 'Volume' in data.columns:
            fig.add_trace(
                go.Bar(
                    x=data['Date'],
                    y=data['Volume'],
                    name='Volume',
                    marker_color='lightblue',
                    hovertemplate='<b>Date:</b> %{x}<br><b>Volume:</b> %{y:,.0f}<extra></extra>'
                )
            )
        
        fig.update_layout(
            title=dict(text=f'{symbol} Trading Volume', x=0.5),
            xaxis_title='Date',
            yaxis_title='Volume',
            template='plotly_white',
            showlegend=False
        )
        
        return fig
    
    def _create_stats_table(self, data: pd.DataFrame, symbol: str) -> html.Div:
        """Create statistics summary table"""
        if data.empty:
            return html.Div("No data available for statistics")
        
        latest_data = data.iloc[-1]
        
        stats = {
            'Metric': [
                'Current Price', 'Daily Change', 'Daily Change %', 
                '52-Week High', '52-Week Low', 'Average Volume',
                'Current Volatility', '50-Day MA', '200-Day MA'
            ],
            'Value': [
                f"${latest_data.get('Close', 0):.2f}",
                f"${latest_data.get('Price_Change', 0):.2f}",
                f"{latest_data.get('Price_Change_Pct', 0):.2f}%",
                f"${data['Close'].max():.2f}",
                f"${data['Close'].min():.2f}",
                f"{data['Volume'].mean():,.0f}",
                f"{latest_data.get('Volatility', 0):.2f}%",
                f"${latest_data.get('MA50', 0):.2f}" if pd.notna(latest_data.get('MA50')) else 'N/A',
                f"${latest_data.get('MA200', 0):.2f}" if pd.notna(latest_data.get('MA200')) else 'N/A'
            ]
        }
        
        stats_df = pd.DataFrame(stats)
        
        return dash_table.DataTable(
            data=stats_df.to_dict('records'),
            columns=[{"name": i, "id": i} for i in stats_df.columns],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': '#0d6efd', 'color': 'white', 'fontWeight': 'bold'},
            style_data_conditional=[
                {
                    'if': {'row_index': 1, 'column_id': 'Value'},
                    'backgroundColor': '#d4edda' if latest_data.get('Price_Change', 0) >= 0 else '#f8d7da',
                    'color': 'green' if latest_data.get('Price_Change', 0) >= 0 else 'red'
                }
            ]
        )
    
    def _create_error_alert(self, message: str) -> html.Div:
        """Create error alert component"""
        return html.Div([
            html.I(className="fas fa-exclamation-triangle me-2"),
            message
        ], className="alert alert-danger alert-dismissible")
    
    def run_server(self, debug=True, host='0.0.0.0', port=8050):
        """Run the dashboard server"""
        logger.info(f"Starting FinanceLake Dashboard on http://{host}:{port}")
        self.app.run(debug=debug, host=host, port=port)

# Main execution
if __name__ == '__main__':
    dashboard = StockDashboard()
    dashboard.run_server(debug=True)