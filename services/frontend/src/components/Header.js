import React from 'react';
import '../styles/Header.css';

const Header = () => {
  return (
    <header className="header">
      <div className="header-content">
        <div className="logo">
          <h1>🚀 FinanceLake</h1>
          <span className="subtitle">Real-Time Financial Analytics Platform</span>
        </div>
        <nav className="nav">
          <a href="/" className="nav-link active">Dashboard</a>
          <a href="/analytics" className="nav-link">Analytics</a>
          <a href="/settings" className="nav-link">Settings</a>
        </nav>
        <div className="header-status">
          <div className="status-indicator online"></div>
          <span className="status-text">Live Data</span>
        </div>
      </div>
    </header>
  );
};

export default Header;
