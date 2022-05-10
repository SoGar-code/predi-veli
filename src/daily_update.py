#!/usr/bin/env python3
"""
A script to run daily. Processes included
* recover station information
* collect data from previous day into a single parquet file

NB: adapted from https://github.com/lovasoa/historique-velib-opendata/
"""
import os
import datetime as dt
import pandas as pd 
import requests
from zipfile import ZipFile

def get_stations():
    sapi="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
    stations_df = pd.DataFrame(requests.get(sapi).json()["data"]["stations"])
    stations_df.set_index("stationCode", inplace=True)
    stations_df.sort_index(inplace=True)
    stations_df['station_geo'] = ["{:.5f},{:.5f}".format(s.lat,s.lon) for s in stations_df.itertuples()]
    stations_df['credit_card'] = stations_df.rental_methods.str.contains('CREDITCARD', regex=False).fillna(False)
    stations_df['station_name'] = stations_df["name"]
    return stations_df[['station_name', 'capacity', 'station_geo', 'credit_card']]


def fetch_save_stations_info():
    """ Fetch and save station info as a CSV file """
    stations_df = get_stations()
    timestamp = dt.datetime.now().strftime("%Y-%m-%d")

    df = stations_df

    file_name = 'station_info_{}.csv'.format(timestamp)
    file_path = os.path.join("data", file_name)
    df.to_csv(file_path, header=False, mode='a', date_format='%Y-%m-%dT%H:%MZ')


def get_status_df(file_path):
    """ Get dataframe from provided path """
    df = pd.read_csv(
        file_path, 
        parse_dates=[0], 
        header=None, 
        names= ["date", "station_code", "available_mechanical", "available_electrical", "operative"],
        index_col="date"
    )
    return df


def collect_statuses(date_str, zip_delete=False):
    """ Collect data from *date_str* into a single parquet file """
    data_path = os.listdir("../data")
    
    prefix_str = "station_status_{}".format(date_str)
    status_day = [os.path.join("..", "data", file_name) 
                    for file_name in data_path 
                    if file_name.startswith(prefix_str)]
    
    histo_df = pd.concat([get_status_df(file_path) for file_path in status_day], axis=0)
    
    histo_df.sort_index(inplace=True)
    
    save_path = "..\data\Summary_{}.parquet".format(date_str)

    histo_df.to_parquet(save_path)

    if zip_delete:
        zip_and_delete(status_day, date_str)


def zip_and_delete(file_list, date_str):
    """ Zip, save and delete files in list """
    file_path = os.path.join("..", "data", "save_{}.zip".format(date_str))
    
    with ZipFile(file_path, 'w') as zip:
        for file in file_list:
            zip.write(file)

    


def main():
    fetch_save_stations_info()

    yesterday = dt.datetime.now() - dt.timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    collect_statuses(date_str)


if __name__ == "__main__":
    main()
    print("=== Daily Update ran! ===")
