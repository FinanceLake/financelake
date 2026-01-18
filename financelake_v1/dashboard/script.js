/**
 * FinanceLake Pro - Dashboard Controller
 * Logic: Dropdown filters Chart ONLY. Ticker cards remain standard.
 */

const CONFIG = {
    apiPrices: '/api/latest',
    apiMetrics: '/api/metrics',
    refreshRate: 3000,
    colors: {
        primary: '#3b82f6',
        grid: 'rgba(255,255,255,0.05)',
        text: '#64748b'
    }
};

// State Management
const STATE = {
    selectedSymbol: null, 
    symbolsLoaded: false  
};

// --- HELPER: Time Conversion ---
function toLocalTime(utcString) {
    if (!utcString) return "--:--:--";
    const date = new Date(utcString.replace(" ", "T") + "Z");
    return date.toLocaleTimeString(); 
}

// --- 1. CHART MANAGER (Filters Data) ---
const ChartManager = {
    instance: null,

    init() {
        const ctx = document.getElementById('mainChart').getContext('2d');
        const gradient = ctx.createLinearGradient(0, 0, 0, 400);
        gradient.addColorStop(0, 'rgba(59, 130, 246, 0.5)'); 
        gradient.addColorStop(1, 'rgba(59, 130, 246, 0.0)');

        this.instance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Price',
                    data: [],
                    borderColor: CONFIG.colors.primary,
                    backgroundColor: gradient,
                    borderWidth: 2,
                    pointRadius: 2, 
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: { 
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(15, 23, 42, 0.9)',
                        titleColor: '#fff',
                        bodyColor: '#cbd5e1'
                    }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { color: CONFIG.colors.text } },
                    y: { grid: { color: CONFIG.colors.grid }, ticks: { color: CONFIG.colors.text } }
                }
            }
        });
    },

    update(data) {
        if (!this.instance) return;

        // --- FILTER LOGIC (Chart Only) ---
        if (!STATE.selectedSymbol && data.length > 0) {
            STATE.selectedSymbol = data[0].symbol;
        }

        // Only show rows for the selected symbol
        const filteredData = data.filter(d => d.symbol === STATE.selectedSymbol);
        const chartData = [...filteredData].reverse();
        
        this.instance.data.labels = chartData.map(d => toLocalTime(d.timestamp));
        this.instance.data.datasets[0].data = chartData.map(d => d.price);
        this.instance.update();
    }
};

// --- 2. UI MANAGER (Cards remain static) ---
const UIManager = {
    elements: {
        status: document.getElementById('connection-status'),
        time: document.getElementById('last-updated'),
        grid: document.getElementById('ticker-grid'),
        metricR2: document.getElementById('metric-r2'),
        metricRMSE: document.getElementById('metric-rmse'),
        metricDate: document.getElementById('metric-date'),
        metricRows: document.getElementById('metric-rows'),
        barFill: document.getElementById('r2-bar'),
        selector: document.getElementById('symbol-selector')
    },

    setStatus(isOnline) {
        if (isOnline) {
            this.elements.status.className = "status-pill online";
            this.elements.status.innerHTML = '<span class="dot"></span> Live System';
            this.elements.time.textContent = new Date().toLocaleTimeString();
        } else {
            this.elements.status.className = "status-pill offline";
            this.elements.status.innerHTML = '<span class="dot"></span> Connecting...';
        }
    },

    updateDropdown(data) {
        if (STATE.symbolsLoaded) return;

        const uniqueSymbols = [...new Set(data.map(item => item.symbol))];
        
        if (uniqueSymbols.length > 0) {
            this.elements.selector.innerHTML = uniqueSymbols
                .map(sym => `<option value="${sym}">${sym}</option>`)
                .join('');
            
            STATE.selectedSymbol = uniqueSymbols[0];
            STATE.symbolsLoaded = true;

            this.elements.selector.addEventListener('change', (e) => {
                STATE.selectedSymbol = e.target.value;
                // Chart will update on next tick
            });
        }
    },

    renderCards(data) {
        const uniqueStocks = {};
        data.forEach(row => {
            if (!uniqueStocks[row.symbol]) uniqueStocks[row.symbol] = row;
        });
        this.elements.grid.innerHTML = Object.values(uniqueStocks).map(stock => this.createCardHTML(stock)).join('');
    },

    createCardHTML(stock) {
        const price = parseFloat(stock.price).toFixed(2);
        const pred = parseFloat(stock.prediction).toFixed(2);
        const isBullish = pred > price;
        const colorClass = isBullish ? 'bull-text' : 'bear-text';
        const trendClass = isBullish ? 'bullish' : 'bearish';
        const arrow = isBullish ? '▲' : '▼';
        const localTime = toLocalTime(stock.timestamp);

        // --- NO "isActive" LOGIC HERE (Kept Standard) ---
        return `
            <div class="ticker-card ${trendClass}">
                <div class="symbol-header">
                    <span>${stock.symbol}</span>
                    <span style="font-size: 0.8rem; opacity: 0.5;">${localTime}</span>
                </div>
                <div class="price-row">
                    <div><div class="price-label">Current</div><div class="price-val">$${price}</div></div>
                </div>
                <div class="price-row" style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 10px;">
                    <div>
                        <div class="price-label">Forecast</div>
                        <div class="prediction-val ${colorClass}">${arrow} $${pred}</div>
                    </div>
                    <div style="text-align: right;">
                        <div class="price-label">Action</div>
                        <div class="prediction-val ${colorClass}" style="font-size: 0.9rem;">${isBullish ? 'BUY' : 'SELL'}</div>
                    </div>
                </div>
            </div>
        `;
    },

    renderMetrics(metrics) {
        if (!metrics.r2) return;
        this.elements.metricR2.innerText = metrics.r2 + "%";
        this.elements.barFill.style.width = metrics.r2 + "%";
        this.elements.metricRMSE.innerText = "$" + metrics.rmse;
        this.elements.metricDate.innerText = metrics.last_trained; 
        if (this.elements.metricRows && metrics.rows_trained) {
            this.elements.metricRows.innerText = metrics.rows_trained.toLocaleString(); 
        }
    }
};

// --- 3. MAIN APP ---
const App = {
    async init() {
        ChartManager.init();
        this.startLoop();
    },

    async fetchData() {
        try {
            const resPrices = await fetch(CONFIG.apiPrices);
            const prices = await resPrices.json();
            const resMetrics = await fetch(CONFIG.apiMetrics);
            const metrics = await resMetrics.json();

            if (Array.isArray(prices) && prices.length > 0) {
                UIManager.setStatus(true);
                
                // 1. Dropdown
                UIManager.updateDropdown(prices);

                // 2. Chart (Filtered by Dropdown)
                ChartManager.update(prices);

                // 3. Cards (Unfiltered - Show ALL)
                UIManager.renderCards(prices);
            } else {
                UIManager.setStatus(false);
            }

            if (metrics) UIManager.renderMetrics(metrics);

        } catch (error) {
            console.error(error);
            UIManager.setStatus(false);
        }
    },

    startLoop() {
        this.fetchData();
        setInterval(() => this.fetchData(), CONFIG.refreshRate);
    }
};

App.init();