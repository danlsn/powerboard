import os
from datetime import time
from pathlib import Path
from time import sleep

import requests
from dotenv import load_dotenv

load_dotenv('.env')


def get_site_usage(site_id, start_date, end_date, save=True):
    url = f"https://api.amber.com.au/v1/sites/{site_id}/usage?startDate={start_date}&endDate={end_date}&resolution=30"

    payload = {}
    headers = {'Accept': 'application/json', 'Authorization': f"Bearer {os.getenv('AMBER_API_TOKEN')}"}

    response = requests.request("GET", url, headers=headers, data=payload)
    print(f"RateLimit-Remaining: {response.headers.get('RateLimit-Remaining')}, RateLimit-Limit: {response.headers.get('RateLimit-Limit')}, RateLimit-Reset: {response.headers.get('RateLimit-Reset')}.")
    if response.status_code == 429:
        print(f"Rate limit reached. Sleeping for {response.headers.get('RateLimit-Reset')} seconds.")
        sleep(int(response.headers.get('RateLimit-Reset')))
        response = requests.request("GET", url, headers=headers, data=payload)

    if response.headers.get('RateLimit-Remaining') == '1':
        print(f"Rate limit reached. Sleeping for {response.headers.get('RateLimit-Reset')} seconds.")
        sleep(int(response.headers.get('RateLimit-Reset')))
    if save:
        filename = Path(f"../data/amber-extract/raw/usage/usage__{site_id}_{start_date}_{end_date}.json")
        filename.parent.mkdir(parents=True, exist_ok=True)
        print(f"Saving to {filename}")
        filename.write_bytes(response.content)
    return response


def main():
    site_ids = ['01E8RD8Q1Y2Y4YJS3J8SMKPE9Q', '01E8RD8PPXJGP5X2FMR5WZ9MP1']
    start_date = '2022-01-19'
    end_date = '2024-04-30'
    date_ranges = get_month_start_end_dates(start_date, end_date)
    for site_id in site_ids:
        for start_date, end_date in date_ranges:
            print(f"Getting usage for {site_id}, between {start_date} and {end_date}")
            get_site_usage(site_id, start_date, end_date)


import pandas as pd


def get_month_start_end_dates(start_date, end_date):
    # Create date range with monthly frequency
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

    # List to store start and end dates of each month
    month_start_end_dates = []

    # Iterate over date range
    for date in date_range:
        # Get start and end of month
        month_start = date.strftime('%Y-%m-%d')
        month_end = (date + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')

        # Append to list
        month_start_end_dates.append((month_start, month_end))

    return month_start_end_dates


if __name__ == "__main__":
    main()
