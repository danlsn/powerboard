import os
from urllib.parse import urljoin
from datetime import date
import logging
from functools import lru_cache
from pathlib import Path

from tqdm import tqdm
import pandas as pd
import pendulum
from requests import Session
import requests_cache
from dotenv import load_dotenv


from utils import get_dates_between, prepare_usage_df, parse_usage_response, get_monthly_dates_between

load_dotenv('.env')
requests_cache.install_cache("powerboard_cache", backend="sqlite", expire_after=3600)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)


class PowerBoard(Session):
    def __init__(self, api_token, **kwargs):
        super().__init__()
        self.base_url = "https://api.amber.com.au/v1"
        self.api_token = api_token
        self._set_auth_header()
        self.sites = self.get_sites()
        logger.info(f"Found {len(self.sites)} sites")

    def _set_auth_header(self):
        if not self.api_token:
            raise ValueError
        self.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "accept": "application/json"
        })

    def get_sites(self):
        sites = self.get("/sites").json()
        return {site['id']: site for site in sites}

    def site_active_dates(self, site_id, start_date=None, end_date=date.today()):
        if self.sites is None:
            self.sites = self.get_sites()
        active_from = pendulum.parse(self.sites[site_id]['activeFrom']).date()
        if start_date is None:
            start_date = active_from
        if isinstance(start_date, str):
            start_date = pendulum.parse(start_date).date()
        if isinstance(end_date, str):
            end_date = pendulum.parse(end_date).date()
        start_date = max(start_date, active_from)
        end_date = min(end_date, date.today())

        return get_dates_between(start_date, end_date)

    def get_site_usage(self, site_id, start_date=None, end_date=date.today()):
        logger.info(f"Getting usage for {site_id}, between {start_date} and {end_date}")
        active_dates = self.site_active_dates(site_id, start_date, end_date)
        date_chunks = [active_dates[i:i + 7] for i in range(0, len(active_dates), 7)]
        pbar = tqdm(date_chunks)
        for chunk in pbar:
            pbar.set_description(f"Getting usage from {chunk[0]} to {chunk[-1]}")
            params = {
                "siteId": site_id,
                "startDate": chunk[0],
                "endDate": chunk[-1]
            }
            usage = parse_usage_response(self.get(f"/sites/{site_id}/usage", params=params), site_id)
            yield from usage


    def get_usage_df(self, site_id=None, start_date=None, end_date=date.today()):
        usage = []
        sites = self.sites.keys() if site_id is None else [site_id]
        for site in sites:
            active_dates = self.site_active_dates(site)
            usage.extend(self.get_site_usage(site, start_date=start_date, end_date=end_date))
        df = pd.json_normalize(usage)
        return prepare_usage_df(df)

    def request(self, method, url, *args, **kwargs):
        if url.startswith("/"):
            url = self.base_url + url
        elif not url.startswith("http"):
            url = self.base_url + "/" + url
        return super().request(method, url, *args, **kwargs)


def main():
    with PowerBoard(os.getenv("AMBER_API_TOKEN")) as pb:
        sites = pb.sites
        start_date = '2023-09-01'
        end_date = date.today()
        df = pb.get_usage_df(start_date=start_date, end_date=end_date)
        filename = Path(f"../data/amber-extract/full/usage_{start_date}_{end_date}.csv")
        filename.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filename, index=False)
        ...


if __name__ == "__main__":
    main()
