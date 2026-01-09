#!/bin/bash
# Start Dashboard Script
# Quick launcher for the Streamlit web dashboard

echo ""
echo "========================================"
echo "  Real-Time Stock Insight Dashboard"
echo "========================================"
echo ""

# Check if Streamlit is installed
echo "Checking dependencies..."
if ! pip list | grep -q streamlit; then
    echo "⚠️  Streamlit not found. Installing dependencies..."
    pip install -r requirements.txt
    echo ""
fi

echo "✅ Dependencies OK"
echo ""
echo "🚀 Starting dashboard..."
echo "📊 Dashboard will open in your browser at http://localhost:8501"
echo ""
echo "💡 Tips:"
echo "   - Use the sidebar to navigate between pages"
echo "   - Start the pipeline from the Home page"
echo "   - Press Ctrl+C to stop the dashboard"
echo ""
echo "========================================"
echo ""

# Start Streamlit
streamlit run app.py

