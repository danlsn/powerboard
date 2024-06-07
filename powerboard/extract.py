import os
import logging
import re
from pathlib import Path
import pandas as pd

from utils import (
    get_monthly_dates_between,
    get_dates_between,
    get_extracted_usage_dates,
)
from client import PowerBoard

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
)
logger.addHandler(ch)


def export_monthly_usage():
    with PowerBoard(os.getenv("AMBER_API_TOKEN")) as pb:
        sites = pb.sites
        start_date = min([site["activeFrom"] for site in sites.values()])
        start_date = "2024-01-01"
        month_periods = get_monthly_dates_between(start_date)
        for start_date, end_date in month_periods:
            logger.info(f"Getting usage for {start_date} to {end_date}")
            df = pb.get_usage_df(start_date=start_date, end_date=end_date)
            filename = Path(
                f"../data/amber-extract/monthly/usage_{start_date}_{end_date}.csv"
            )
            filename.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(filename, index=False)
        ...


def export_daily_usage(strategy="incremental"):
    with PowerBoard(os.getenv("AMBER_API_TOKEN")) as pb:
        sites = pb.sites
        start_date = min([site["activeFrom"] for site in sites.values()])
        active_dates = set(get_dates_between(start_date, exclude_today=True))
        if strategy == "incremental":
            extracted_dates = set(get_extracted_usage_dates())
            active_dates = active_dates - extracted_dates
        else:
            pass

        for start_date in sorted(active_dates):
            end_date = start_date
            logger.info(f"Getting usage for {start_date}")
            df = pb.get_usage_df(start_date=start_date, end_date=end_date)
            filename = Path(
                f"../data/amber-extract/daily/usage_{start_date}_{end_date}.csv"
            )
            filename.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(filename, index=False)


if __name__ == "__main__":
    export_daily_usage(strategy=None)
