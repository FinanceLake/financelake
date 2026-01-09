# Start Dashboard Script
# Quick launcher for the Streamlit web dashboard

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Real-Time Stock Insight Dashboard" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Streamlit is installed
Write-Host "Checking dependencies..." -ForegroundColor Yellow
$streamlitInstalled = pip list | Select-String "streamlit"

if (-not $streamlitInstalled) {
    Write-Host "⚠️  Streamlit not found. Installing dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt
    Write-Host ""
}

Write-Host "✅ Dependencies OK" -ForegroundColor Green
Write-Host ""
Write-Host "🚀 Starting dashboard..." -ForegroundColor Green
Write-Host "📊 Dashboard will open in your browser at http://localhost:8501" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Tips:" -ForegroundColor Yellow
Write-Host "   - Use the sidebar to navigate between pages" -ForegroundColor White
Write-Host "   - Start the pipeline from the Home page" -ForegroundColor White
Write-Host "   - Press Ctrl+C to stop the dashboard" -ForegroundColor White
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Start Streamlit
streamlit run app.py

