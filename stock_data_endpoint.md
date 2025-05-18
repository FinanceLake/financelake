# Backend Endpoint Documentation: Historical Market Data

## Description
This REST endpoint serves historical stock market data from the local storage layer.  
It accepts query parameters to filter by stock symbol and date range, and returns the data in JSON or CSV format.

---

## Endpoint
`GET /data/stock/{symbol}?from=YYYY-MM-DD&to=YYYY-MM-DD&format={json|csv}`

### Parameters

- **symbol** (path parameter): Stock ticker symbol, e.g. `AAPL`.
- **from** (query parameter): Start date in ISO format (`YYYY-MM-DD`).
- **to** (query parameter): End date in ISO format (`YYYY-MM-DD`).
- **format** (query parameter, optional): Response format, either `json` (default) or `csv`.

---

## Response

### JSON Structure

```json
{
  "symbol": "AAPL",
  "data": [
    {
      "date": "2024-01-01",
      "open": 150.00,
      "high": 157.00,
      "low": 149.00,
      "close": 155.00,
      "volume": 1000000
    },
    {
      "date": "2024-01-02",
      "open": 156.00,
      "high": 159.00,
      "low": 154.00,
      "close": 158.00,
      "volume": 1100000
    }
  ]
}
