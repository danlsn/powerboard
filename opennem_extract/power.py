import csv
import re
from pathlib import Path
from collections import namedtuple

import requests
import pandas as pd
import pendulum
import icecream as ic


def parse_opennem_interval(interval):
    NemInterval = namedtuple("NemInterval", ["unit", "value"])
    pat = re.compile(r"(?P<value>\d+)(?P<unit>\w+)")
    match = pat.match(interval)
    units = {
        "m": "minutes",
        "h": "hours",
        "d": "days",
        "M": "months",
        "y": "years",
        "Q": "quarters",
    }
    if match:
        value, unit = match.groups()
        if unit == "Q":
            return NemInterval("months", int(value) * 3)
        else:
            return NemInterval(units[unit], int(value))
    else:
        raise ValueError(f"Invalid interval {interval}")


def parse_opennem_history(history):
    start = pendulum.parse(history["start"])
    end = pendulum.parse(history["last"])
    interval, value = parse_opennem_interval(history["interval"])

    date_range = pendulum.period(start=start, end=end)
    periods = date_range.range(interval, value)
    for item in zip(periods, history["data"]):
        data_point = {
            "timestamp": item[0],
            "value": item[1],
            "interval": history["interval"],
        }
        yield data_point


def parse_opennem_response_data(data: dict):
    dims = {k: data[k] for k in data.keys() if k != "history"}

    history = parse_opennem_history(data["history"])
    data = []
    for row in history:
        data.append(
            dims
            | {
                "timestamp": row["timestamp"],
                "value": row["value"],
                "interval": row["interval"],
            }
        )

    df = pd.DataFrame(data)
    return df


def get_opennem_7d_data(state="VIC1"):
    url = f"https://data.opennem.org.au/v3/stats/au/NEM/{state}/power/7d.json"
    r = requests.get(url)
    res = r.json()
    dfs = {}
    for data_point in res["data"]:
        df = parse_opennem_response_data(data_point)
        dfs[data_point["id"]] = df
        min_date = df["timestamp"].min().strftime("%Y-%m-%dT%H-%M")
        max_date = df["timestamp"].max().strftime("%Y-%m-%dT%H-%M")
        interval = df["interval"].iloc[0]
        save_df_csv(
            df,
            Path(
                f"../data/opennem-extract/7d/{data_point['id']}_{interval}_{min_date}_{max_date}.csv"
            ),
        )
        print(
            f"[{state}, 7D]: Saved {data_point['id']}_{interval}_{min_date}_{max_date}.csv"
        )
        ...
    return res


def get_opennem_daily_data(state="VIC1"):
    url = f"https://data.opennem.org.au/v3/stats/au/{state}/daily.json"
    r = requests.get(url)
    res = r.json()
    dfs = {}
    for data_point in res["data"]:
        df = parse_opennem_response_data(data_point)
        dfs[data_point["id"]] = df
        min_date = df["timestamp"].min().strftime("%Y-%m-%dT%H-%M")
        max_date = df["timestamp"].max().strftime("%Y-%m-%dT%H-%M")
        interval = df["interval"].iloc[0]
        save_df_csv(
            df,
            Path(
                f"../data/opennem-extract/daily/{data_point['id']}_{interval}_{min_date}_{max_date}.csv"
            ),
        )
        print(
            f"[{state}, DAILY]: Saved {data_point['id']}_{interval}_{min_date}_{max_date}.csv"
        )
        ...
    return res


def get_opennem_all_data(state="VIC1"):
    url = f"https://data.opennem.org.au/v3/stats/au/NEM/{state}/energy/all.json"
    r = requests.get(url)
    res = r.json()
    dfs = {}
    for data_point in res["data"]:
        df = parse_opennem_response_data(data_point)
        dfs[data_point["id"]] = df
        min_date = df["timestamp"].min().strftime("%Y-%m-%dT%H-%M")
        max_date = df["timestamp"].max().strftime("%Y-%m-%dT%H-%M")
        interval = df["interval"].iloc[0]
        save_df_csv(
            df,
            Path(
                f"../data/opennem-extract/all/{data_point['id']}_{interval}_{min_date}_{max_date}.csv"
            ),
        )
        print(
            f"[{state}, ALL]: Saved {data_point['id']}_{interval}_{min_date}_{max_date}.csv"
        )
    return dfs


def get_opennem_yearly_data(state="VIC1"):
    current_year = pendulum.now().year
    while True:
        url = f"https://data.opennem.org.au/v3/stats/au/NEM/{state}/energy/{current_year}.json"
        r = requests.get(url)
        if r.status_code != 200:
            break
        res = r.json()
        for data_point in res["data"]:
            df = parse_opennem_response_data(data_point)
            min_date = df["timestamp"].min().strftime("%Y-%m-%dT%H-%M")
            max_date = df["timestamp"].max().strftime("%Y-%m-%dT%H-%M")
            interval = df["interval"].iloc[0]
            save_df_csv(
                df,
                Path(
                    f"../data/opennem-extract/yearly/{current_year}/{data_point['id']}_{interval}_{min_date}_{max_date}.csv"
                ),
            )
            print(
                f"[{state}, YEARLY]: Saved {data_point['id']}_{interval}_{min_date}_{max_date}.csv"
            )
        current_year -= 1


def save_df_csv(df, filename):
    filename.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filename, index=False, quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    for state in ["VIC1"]:
        yearly_data = get_opennem_yearly_data(state=state)
        all_data = get_opennem_all_data(state=state)
        weekly_data = get_opennem_7d_data(state=state)
        daily_data = get_opennem_daily_data(state=state)
