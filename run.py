import pandas as pd
import requests
import json
import time
import csv
import os


def get_filings(cik, result_dir):
    headers = {
        "User-Agent": "Tim Robert-Fitzgerald tim.terf@gmail.com",
        "Accept-Encoding": "gzip, deflate",
    }
    url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json"
    response = requests.get(
        url,
        headers=headers
        | {
            "Host": "data.sec.gov",
        },
    )

    company_filings = response.json()
    if len(company_filings["filings"]["recent"]) == 0:
        return
    company_filings_df = pd.DataFrame(company_filings["filings"]["recent"])
    access_number = (
        company_filings_df[company_filings_df.form == "10-K"]
        .accessionNumber.values[0]
        .replace("-", "")
    )
    file_name = company_filings_df[
        company_filings_df.form == "10-K"
    ].primaryDocument.values[0]
    result_path = os.path.join(result_dir, f"{access_number}_{file_name}")
    if os.path.exists(result_path):
        return
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{access_number}/{file_name}"
    req_content = requests.get(
        url,
        headers=headers
        | {
            "Host": "www.sec.gov",
        },
    ).content.decode("utf-8")
    with open(result_path, "w") as f:
        f.write(req_content)


def read_ciks_from_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        companies = json.load(file)
    return pd.DataFrame(companies["data"], columns=companies["fields"])


# def read_ciks_from_tsv(file_path):
#     companies = {}
#     with open(file_path, 'r', encoding='utf-8') as file:
#         reader = csv.reader(file, delimiter='\t')
#         for row in reader:
#             print(row)
#             if len(row) >= 2:
#                 company_name, cik = row[0], row[1]
#                 companies[company_name] = cik
#     return companies

if __name__ == "__main__":
    file_path = "company_tickers_exchange.json"
    companies = read_ciks_from_json(file_path)
    # result_dir = os.path.join("filings", "AAPL")
    # os.makedirs(result_dir, exist_ok=True)
    # get_filings(companies[companies["ticker"] == "AAPL"].cik.values[0], result_dir)
    for index, row in companies.head(50).iterrows():
        print("Downloading", row.ticker)
        result_dir = os.path.join("filings", row.ticker)
        os.makedirs(result_dir, exist_ok=True)
        get_filings(row.cik, result_dir)
        if index > 0 and index % 5 == 0:
            time.sleep(10)
