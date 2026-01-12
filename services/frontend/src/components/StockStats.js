import React from 'react';
import '../styles/App.css';

const StockStats = ({ data }) => {
  if (!data || data.length === 0) return null;

  // Calculate statistics
  const prices = data.map(item => item.close_price).filter(price => price != null);
  const volumes = data.map(item => item.volume).filter(vol => vol != null);

  const latestPrice = prices[0]; // Data is sorted descending
  const oldestPrice = prices[prices.length - 1];
  const priceChange = latestPrice - oldestPrice;
  const priceChangePercent = oldestPrice !== 0 ? ((priceChange / oldestPrice) * 100) : 0;

  const avgVolume = volumes.length > 0 ? volumes.reduce((a, b) => a + b, 0) / volumes.length : 0;
  const maxVolume = volumes.length > 0 ? Math.max(...volumes) : 0;

  const stats = [
    {
      label: 'Current Price',
      value: latestPrice ? `$${latestPrice.toFixed(2)}` : 'N/A',
      change: null
    },
    {
      label: 'Price Change',
      value: priceChange ? `${priceChange >= 0 ? '+' : ''}$${priceChange.toFixed(2)}` : 'N/A',
      change: priceChangePercent ? `${priceChangePercent >= 0 ? '+' : ''}${priceChangePercent.toFixed(2)}%` : null
    },
    {
      label: 'Average Volume',
      value: avgVolume ? Math.round(avgVolume).toLocaleString() : 'N/A',
      change: null
    },
    {
      label: 'Peak Volume',
      value: maxVolume ? maxVolume.toLocaleString() : 'N/A',
      change: null
    },
    {
      label: 'Data Points',
      value: data.length.toLocaleString(),
      change: null
    }
  ];

  return (
    <div className="stats-grid">
      {stats.map((stat, index) => (
        <div key={index} className="stat-card">
          <div className="stat-value">{stat.value}</div>
          <div className="stat-label">{stat.label}</div>
          {stat.change && (
            <div className={`stat-change ${stat.change.startsWith('+') ? 'positive' : 'negative'}`}>
              {stat.change}
            </div>
          )}
        </div>
      ))}
    </div>
  );
};

export default StockStats;
