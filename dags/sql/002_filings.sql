CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    cik INTEGER NOT NULL REFERENCES watchlist (cik),
    ticker TEXT NOT NULL,
    form TEXT NOT NULL,
    filing_date DATE NOT NULL,
    report_date DATE,
    accepted_at TIMESTAMPTZ NOT NULL,
    description TEXT,
    url TEXT NOT NULL
);

-- Full rebuild every run. The raw table is small and the query is cheap,
-- so emptying and refilling is the simplest way to be safe to rerun.
TRUNCATE filings;

INSERT INTO filings (accession_number, cik, ticker, form, filing_date, report_date, accepted_at, description, url)
SELECT
    r.accession_number,
    r.cik,
    w.ticker,
    r.form,
    r.filing_date,
    r.report_date,
    r.accepted_at,
    r.description,
        -- the SEC's document address: cik, accession number without dashes, file name
    'https://www.sec.gov/Archives/edgar/data/' || r.cik || '/'
        || replace(r.accession_number, '-', '') || '/' || r.primary_document
FROM raw.filings AS r
JOIN watchlist AS w ON w.cik = r.cik
WHERE r.form IN ('10-K', '10-K/A', '10-Q', '10-Q/A', '8-K', '8-K/A');