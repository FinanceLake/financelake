import yfinance as yf

def fetch_stock_data(ticker_symbol: str, period="1d"):
    ticker = yf.Ticker(ticker_symbol)
    return ticker.history(period=period)
