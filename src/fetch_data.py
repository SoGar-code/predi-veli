#!/usr/bin/env python3
"""
Small script to extract data from the paris opendata velib API
and save it as CSV files

NB: adapted from https://github.com/lovasoa/historique-velib-opendata/
"""
import os
import datetime as dt
import pandas as pd 
import requests

from dotenv import dotenv_values

PREDIVELI_PATH = dotenv_values(".env")["PREDIVELI_PATH"]


def get_statuses():
    sapi = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    j = requests.get(sapi).json()["data"]["stations"]
    df = pd.DataFrame(j)

    # NB: the "last_reported" field is hard to exploit, so dropped as of 2022-12-29

    df = df.astype({'is_renting':bool, 'stationCode':str})
    df = df.join(df["num_bikes_available_types"].apply(lambda l: pd.Series({**l[0], **l[1]}, dtype='float64')))

    requested_cols = ["stationCode", "mechanical", "ebike"]

    # More robust code
    df = df[[col for col in df.columns if col in requested_cols]]

    df.rename({
        "mechanical": "available_mechanical",
        "ebike": "available_electrical",
    }, axis='columns', inplace=True)
    return df


def main():
    status_df = get_statuses()
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")

    df = status_df

    file_name = 'station_status_{}.csv'.format(timestamp)
    file_path = os.path.join(PREDIVELI_PATH, "data", file_name)
    df.to_csv(file_path, header=True, mode='a', date_format='%Y-%m-%dT%H:%MZ')
    print("timestamp:", timestamp)

    # TODO: integrate weather data through pyOWM - see also https://knasmueller.net/using-the-open-weather-map-api-with-python


if __name__ == "__main__":
    main()
    print("===")
