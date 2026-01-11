import React from 'react';
import '../styles/DataControls.css';

const DataControls = ({ selectedSymbol, onSymbolChange, onStartIngestion, ingestionStatus }) => {
  const symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'];

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return '#f59e0b'; // amber
      case 'completed': return '#10b981'; // green
      case 'failed': return '#ef4444'; // red
      default: return '#6b7280'; // gray
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'running': return 'Running...';
      case 'completed': return 'Completed';
      case 'failed': return 'Failed';
      default: return 'Ready';
    }
  };

  return (
    <div className="data-controls">
      <div className="control-group">
        <label htmlFor="symbol-select">Stock Symbol:</label>
        <select
          id="symbol-select"
          value={selectedSymbol}
          onChange={(e) => onSymbolChange(e.target.value)}
          className="symbol-select"
        >
          {symbols.map(symbol => (
            <option key={symbol} value={symbol}>{symbol}</option>
          ))}
        </select>
      </div>

      <div className="control-group">
        <button
          onClick={onStartIngestion}
          disabled={ingestionStatus?.status === 'running'}
          className="ingest-btn"
        >
          {ingestionStatus?.status === 'running' ? 'Ingesting...' : 'Start Live Data'}
        </button>

        {ingestionStatus && (
          <div className="status-indicator">
            <div
              className="status-dot"
              style={{ backgroundColor: getStatusColor(ingestionStatus.status) }}
            ></div>
            <span className="status-text">
              {getStatusText(ingestionStatus.status)}
            </span>
          </div>
        )}
      </div>
    </div>
  );
};

export default DataControls;
