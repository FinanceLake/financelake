import React, { useState, useEffect } from 'react';
import { stockDataService } from '../services/api';
import StockChart from '../components/StockChart';
import StockStats from '../components/StockStats';
import DataControls from '../components/DataControls';
import '../styles/Dashboard.css';

const Dashboard = () => {
  const [stockData, setStockData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedSymbol, setSelectedSymbol] = useState('AAPL');
  const [ingestionStatus, setIngestionStatus] = useState(null);

  useEffect(() => {
    fetchStockData();
  }, [selectedSymbol]);

  const fetchStockData = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await stockDataService.getStockData(selectedSymbol);
      setStockData(data.data || []);
    } catch (err) {
      setError('Failed to load stock data. Please try again.');
      console.error('Error fetching stock data:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleStartIngestion = async () => {
    try {
      const response = await stockDataService.startDataIngestion(['AAPL', 'GOOGL', 'MSFT'], 60);
      setIngestionStatus({ taskId: response.task_id, status: 'running' });

      // Poll for status
      const pollStatus = async () => {
        try {
          const status = await stockDataService.getIngestionStatus(response.task_id);
          setIngestionStatus(status);

          if (status.status === 'completed' || status.status === 'failed') {
            // Refresh data after ingestion completes
            setTimeout(() => fetchStockData(), 2000);
          } else if (status.status === 'running') {
            setTimeout(pollStatus, 2000);
          }
        } catch (err) {
          console.error('Error polling status:', err);
        }
      };

      setTimeout(pollStatus, 2000);
    } catch (err) {
      setError('Failed to start data ingestion.');
      console.error('Error starting ingestion:', err);
    }
  };

  if (loading && stockData.length === 0) {
    return (
      <div className="dashboard">
        <div className="loading">Loading stock data...</div>
      </div>
    );
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Stock Market Dashboard</h2>
        <DataControls
          selectedSymbol={selectedSymbol}
          onSymbolChange={setSelectedSymbol}
          onStartIngestion={handleStartIngestion}
          ingestionStatus={ingestionStatus}
        />
      </div>

      {error && (
        <div className="error">
          {error}
          <button onClick={fetchStockData} className="retry-btn">
            Retry
          </button>
        </div>
      )}

      {stockData.length > 0 && (
        <>
          <StockStats data={stockData} />

          <div className="dashboard-grid">
            <StockChart
              data={stockData}
              title={`${selectedSymbol} Price History`}
              dataKey="close_price"
              color="#8884d8"
            />

            <StockChart
              data={stockData}
              title={`${selectedSymbol} Volume`}
              dataKey="volume"
              color="#82ca9d"
              type="bar"
            />

            <StockChart
              data={stockData}
              title={`${selectedSymbol} Price Range (High-Low)`}
              dataKey="high_price"
              dataKey2="low_price"
              color="#ffc658"
              type="area"
            />
          </div>
        </>
      )}

      {stockData.length === 0 && !loading && !error && (
        <div className="no-data">
          <h3>No Data Available</h3>
          <p>No stock data found for the selected symbol. Try starting data ingestion.</p>
          <button onClick={handleStartIngestion} className="start-btn">
            Start Data Ingestion
          </button>
        </div>
      )}
    </div>
  );
};

export default Dashboard;
