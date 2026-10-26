import dlt
import requests
from datetime import date, datetime

USER_AGENT = "filings-monitor goodnessazike@gamil.com"


def download_filing_list(cik):
    """Download one company's filing list from the SEC and return it as a Python dictionary."""
    # The SEC wants the company number padded with zeros to 10 digits: 320193 -> 0000320193
    padded_cik = str(cik).zfill(10)
    url = "https://data.sec.gov/submissions/CIK" + padded_cik + ".json"

    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()    # stop loudly if the SEC says no

    return response.json()


def rows_from_filing_list(cik, data):
    """Turn the SEC's parallel columns into one dictionary per filing."""
    company_name = data["name"]
    recent = data["filings"]["recent"]

    # The SEC gives columns, not rows:
    #   recent["accessionNumber"] is a list of all the ids
    #   recent["form"]            is a list of all the form types
    #   recent["filingDate"]      is a list of all the dates
    # Position 0 in every list belongs to the same filing. So we walk the positions.
    how_many = len(recent["accessionNumber"])

    rows = []
    for i in range(how_many):
        # Some filings have no report date. The SEC sends "" for those; we store nothing.
        report_date = None
        if recent["reportDate"][i] != "":
            report_date = date.fromisoformat(recent["reportDate"][i])

        row = {
            "cik": cik,
            "company": company_name,
            "accession_number": recent["accessionNumber"][i],   # the SEC's unique id for this filing
            "form": recent["form"][i],                          # 10-K, 10-Q, 8-K, 4, ...
            "filing_date": date.fromisoformat(recent["filingDate"][i]),
            "report_date": report_date,
            "accepted_at": datetime.fromisoformat(recent["acceptanceDateTime"][i]),
            "primary_document": recent["primaryDocument"][i],
            "description": recent["primaryDocDescription"][i],
            "items": recent["items"][i],
        }
        rows.append(row)

    return rows


# This label tells dlt: the function below produces a table called "filings".
# primary_key + merge means: a row whose accession_number already exists gets updated,
# a new one gets inserted. Running the pipeline twice never doubles the data.
@dlt.resource(name="filings", primary_key="accession_number", write_disposition="merge")
def filings(ciks):
    for cik in ciks:
        data = download_filing_list(cik)
        rows = rows_from_filing_list(cik, data)
        for row in rows:
            yield row     # hand rows to dlt one at a time


def extract_load():
    """The task Airflow runs.""" # Imported here, not at the top, so the DAG processor does not load Airflow's # database code every 30 seconds just to read this file.
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # The hook picks up the "warehouse" connection you saved in the Airflow UI.
    hook = PostgresHook(postgres_conn_id="warehouse")

    # Step 1: which companies? Ask the watchlist table.
    ciks = []
    for record in hook.get_records("SELECT cik FROM watchlist ORDER BY cik"):
        ciks.append(record[0])
    print("watchlist:", ciks)

    # Step 2 to 4: build the dlt pipeline and run it.
    # hook.get_uri() returns the warehouse address as postgresql://user:password@host:port/db,
    # so the password stays in Airflow and never in this file.
    # dataset_name="raw" means the table is created as raw.filings.
 # Read the saved "warehouse" connection as an object with fields, and build the
    # plain address dlt's driver understands: postgresql://user:password@host:port/database
    conn = hook.get_connection("warehouse")
    credentials = {
        "host": conn.host,
        "port": conn.port,
        "database":conn.schema,
        "username":conn.login,
        "password":conn.password,
    }

    pipeline = dlt.pipeline(
        pipeline_name="sec_filings",
        destination=dlt.destinations.postgres(credentials=credentials),
        dataset_name="raw",
    )
    load_info = pipeline.run(filings(ciks))
    print(load_info)