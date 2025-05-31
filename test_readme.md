# 📈 FinanceLake

**FinanceLake** is an open-source project dedicated to ingesting and analyzing financial data, particularly stock market data retrieved from Yahoo Finance using the `yfinance` library.

This repository includes a core function (`fetch_stock_data`) along with its unit tests to ensure reliability and proper functionality.

---

## 🎯 Objectives

- Retrieve stock data from Yahoo Finance.  
- Return the key columns: `Open`, `Close`, `Volume`.  
- Ensure the function's reliability through unit testing.  
- Avoid real API calls in CI/CD pipelines by using mocking.

---

## 🧪 Unit Tests

The project uses `pytest` to validate the correct behavior of the `fetch_stock_data` function.

### ✔️ What the Tests Check

- The function returns a non-empty `DataFrame`.  
- The `Open`, `Close`, and `Volume` columns are present.  
- Calls to `yfinance.Ticker().history()` are mocked during CI/CD tests.

---

## 📌 Running the Tests

```bash
pytest
```

> **Make sure you’ve installed the dependencies below.**

1. Clone the repository:

```bash
git clone https://github.com/your-username/financelake.git
cd financelake
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate      # Linux/macOS
venv\Scripts\activate         # Windows
```

3. Install the dependencies:

```bash
pip install -r requirements.txt
```

---

## 👨‍💻 Contributing

Contributions are welcome!

- Fork this repository.  
- Create a branch: `git checkout -b new-feature`.  
- Commit your changes: `git commit -m "Add a feature"`.  
- Push to your fork: `git push origin new-feature`.  
- Create a Pull Request.

