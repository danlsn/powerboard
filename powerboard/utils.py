import re
from datetime import date, timedelta
from functools import lru_cache
from decimal import Decimal
from pathlib import Path

import pendulum
from caseconverter import snakecase
import pandas as pd


def get_dates_between(start_date, end_date=date.today(), exclude_today=False):
    """Return a list of dates between d1 and d2, inclusive."""
    if isinstance(start_date, str):
        start_date = pendulum.parse(start_date).date()
    if isinstance(end_date, str):
        end_date = pendulum.parse(end_date).date()
    delta = end_date - start_date
    dates = []
    for i in range(delta.days + 1):
        dates.append(start_date + timedelta(days=i))
    if exclude_today:
        dates = dates[:-1]
    return dates


def get_monthly_dates_between(start_date, end_date=date.today()):
    if isinstance(start_date, str):
        start_date = pendulum.parse(start_date).date()
    if isinstance(end_date, str):
        end_date = pendulum.parse(end_date).date()

    period = pendulum.period(start_date, end_date)
    month_periods = []
    for dt in period.range("months"):
        month_start = (
            dt.start_of("month") if dt.start_of("month") > start_date else start_date
        )
        month_end = dt.end_of("month") if dt.end_of("month") < end_date else end_date
        month_periods.append((month_start, month_end))
    return month_periods


def parse_usage_response(response, site_id=None):
    data = response.json()
    decimal_cols = ["kwh", "perKwh", "cost", "renewables", "spotPerKwh"]
    for value in data:
        if site_id is not None:
            value["site_id"] = site_id
        for col in decimal_cols:
            value[col] = Decimal(str(value[col]))
        yield value


def prepare_usage_df(df):
    if "site_id" not in df.columns:
        df["site_id"] = df["siteId"] if "siteId" in df.columns else None

    data_types = {
        "site_id": "category",
        "channelType": "category",
        "channelIdentifier": "category",
        "type": "category",
        "descriptor": "category",
        "spikeStatus": "category",
        "quality": "category",
        "date": "object",
        "nemTime": "object",
        "startTime": "object",
        "endTime": "object",
        "duration": "timedelta64[m]",
        "kwh": "object",
        "perKwh": "object",
        "cost": "object",
        "renewables": "object",
        "spotPerKwh": "object",
    }
    df = df.astype(data_types)
    df.columns = [snakecase(col) for col in df.columns]
    df["descriptor"] = df["descriptor"].apply(lambda x: to_snakecase(x))
    df["date"] = df["date"].apply(lambda x: pendulum.parse(x).date())
    datetime_cols = ["end_time", "start_time"]
    df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime)
    df["nem_time"] = pd.to_datetime(df["nem_time"]).dt.tz_localize(None)
    df["nem_time_start"] = df["nem_time"] - df["duration"] + timedelta(seconds=1)

    df = df.sort_values(by=["nem_time", "site_id"])
    columns = [
        "date",
        "nem_time_start",
        "nem_time",
        "start_time",
        "end_time",
        "site_id",
        "channel_type",
        "channel_identifier",
        "type",
        "duration",
        "descriptor",
        "spike_status",
        "quality",
        "kwh",
        "per_kwh",
        "cost",
        "renewables",
        "spot_per_kwh",
    ]
    df = df[columns]
    return df


@lru_cache
def to_snakecase(value):
    return snakecase(value)


def get_extracted_usage_dates(path="../data/amber-extract", extract_type="daily"):
    if extract_type not in ["daily", "monthly"]:
        raise NotImplementedError(f"Extract type {extract_type} not implemented")
    path = Path(path)
    files = list(path.glob(f"{extract_type}/*.csv"))
    file_name_pat = re.compile(r"usage_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}).csv")
    dates = set()
    for file in files:
        match = file_name_pat.match(file.name)
        if match:
            start_date, end_date = match.groups()
            all_dates = get_dates_between(start_date, end_date)
            dates.update(all_dates)
    return sorted(dates)


if __name__ == "__main__":
    get_extracted_usage_dates()
