import requests
import pandas as pd
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_stock_data(symbol, start_date, end_date):
    """
    Fetch stock data from the FinanceLake backend API.
    
    Args:
        symbol (str): Stock symbol (e.g., AAPL)
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        pandas.DataFrame: Stock data with date, close, and volume columns
    """
    try:
        # Convert dates to proper format
        start_date = datetime.strptime(start_date[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(end_date[:10], '%Y-%m-%d').strftime('%Y-%m-%d')

        # Get API base URL from environment
        api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000/api/v1')
        url = f"{api_base_url}/stock_data?symbol={symbol}&start_date={start_date}&end_date={end_date}"
        
        logger.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            logger.warning(f"No data returned for {symbol} from {start_date} to {end_date}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Ensure required columns
        required_columns = ['date', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"API response missing required columns: {required_columns}")
        
        return df[['date', 'close', 'volume']]
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data: {str(e)}")
        raise Exception(f"Failed to fetch data: {str(e)}")
    except ValueError as e:
        logger.error(f"Invalid data format: {str(e)}")
        raise Exception(f"Invalid data format: {str(e)}")