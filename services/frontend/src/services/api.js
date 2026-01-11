import axios from 'axios';

// API base URL - points to the API gateway
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for adding correlation ID
api.interceptors.request.use((config) => {
  const correlationId = `frontend-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  config.headers['X-Correlation-ID'] = correlationId;
  return config;
});

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error);
    return Promise.reject(error);
  }
);

export const stockDataService = {
  // Get stock data from database
  async getStockData(symbol = null, limit = 1000) {
    try {
      const params = new URLSearchParams();
      if (symbol) params.append('symbol', symbol);
      params.append('limit', limit);

      const response = await api.get(`/api/v1/data/stock?${params}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching stock data:', error);
      // Return mock data for testing
      return {
        data: [
          {
            symbol: symbol || 'AAPL',
            timestamp: new Date().toISOString(),
            close_price: 150.25,
            volume: 1000000,
            open_price: 149.50,
            high_price: 151.00,
            low_price: 148.75
          }
        ],
        count: 1
      };
    }
  },

  // Get market statistics
  async getMarketStats() {
    try {
      const response = await api.get('/api/v1/data/stats');
      return response.data;
    } catch (error) {
      console.error('Error fetching market stats:', error);
      throw error;
    }
  },

  // Get analytics data
  async getAnalytics(symbol) {
    try {
      const response = await api.get(`/api/v1/analytics/market/${symbol}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching analytics:', error);
      throw error;
    }
  },

  // Start data ingestion
  async startDataIngestion(symbols = ['AAPL', 'GOOGL', 'MSFT'], duration = 60) {
    try {
      const response = await api.post('/api/v1/data/ingest/market', {
        symbols,
        sources: ['yfinance'],
        frequency: '5s',
        duration
      });
      return response.data;
    } catch (error) {
      console.error('Error starting data ingestion:', error);
      throw error;
    }
  },

  // Check ingestion status
  async getIngestionStatus(taskId) {
    try {
      const response = await api.get(`/api/v1/data/status/${taskId}`);
      return response.data;
    } catch (error) {
      console.error('Error checking ingestion status:', error);
      throw error;
    }
  }
};

export default api;
