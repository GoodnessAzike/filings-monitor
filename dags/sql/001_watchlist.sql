CREATE TABLE IF NOT EXISTS watchlist(
    cik INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    company TEXT NOT NULL,
    added_on DATE NOT NULL DEFAULT CURRENT_DATE
);

INSERT INTO watchlist (cik, ticker, company) VALUES
    (320193, 'AAPL', 'Apple Inc.'),
    (789019, 'MSFT', 'Microsoft Corp'),
    (19617, 'JPM', 'JPMorgan Chase & Co'),
    (1045810, 'NVDA', 'NVIDIA Corp'),
    (1018724, 'AMZN', 'Amazon.com Inc')
ON CONFLICT (cik) DO NOTHING;
