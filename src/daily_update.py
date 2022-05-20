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

from src.slack_notif import push_slack

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
    df.to_csv(file_path, header=True, mode='a', date_format='%Y-%m-%dT%H:%MZ')


def get_status_df(file_path, has_header=True):
    """ Get dataframe from provided path """
    if has_header:
        df = pd.read_csv(
            file_path, 
            parse_dates=["time"],
            index_col="time",
        )        

    else:
        df = pd.read_csv(
            file_path, 
            parse_dates=[0], 
            header=None, 
            names= ["date", "stationCode", "available_mechanical", "available_electrical", "operative"],
            index_col="date"
        )
    
    return df


def extract_enrich_data(file_path, has_header=True):
    """ Extract data + add a new column saving file name """
    df = get_status_df(file_path, has_header)

    file_time_str = os.path.basename(file_path)[15:-4]
    file_time = dt.datetime.strptime(file_time_str, '%Y-%m-%d_%H%M%S')
    df["file_time"] = pd.to_datetime(file_time)

    df["file_time"] = df["file_time"].dt.tz_localize(tz="Europe/Paris")

    df.reset_index(inplace=True)
    df.set_index("file_time", inplace=True)

    return df


def collect_statuses(date_str, zip_delete=False, has_header=True):
    """ Collect data from *date_str* into a single parquet file """
    data_path = os.listdir("data")
    
    prefix_str = "station_status_{}".format(date_str)
    status_day = [os.path.join("data", file_name) 
                    for file_name in data_path 
                    if file_name.startswith(prefix_str)]
    
    histo_df = pd.concat([extract_enrich_data(file_path, has_header) 
                            for file_path in status_day], axis=0)

    # Type conversion for saving with parquet
    histo_df["stationCode"] = histo_df["stationCode"].astype(str)
    
    histo_df.sort_index(inplace=True)

    file_name = "Summary_{}.parquet".format(date_str)
    save_path = os.path.join("data", file_name)

    histo_df.to_parquet(save_path, compression="brotli")

    if zip_delete:
        zip_and_delete(status_day, date_str)

    nb_rows = len(histo_df)
    slack_message(file_name, nb_rows)


def slack_message(file_name, nb_rows):
    """ Generate and push a message to Slack """
    floor_nb_rows = 120000

    msg = "Completed '{}'. Saved a total of {} rows (typical: >={})"

    if (nb_rows < floor_nb_rows):
        msg +="\n :rotating_light: <@UJ924G1A9> something wrong!"
    push_slack(msg.format(file_name, nb_rows, floor_nb_rows))


def zip_and_delete(file_list, date_str):
    """ Zip, save and delete files in list """
    file_path = os.path.join("..", "data", "save_{}.zip".format(date_str))
    
    with ZipFile(file_path, 'w') as zip:
        for file in file_list:
            zip.write(file)


def get_historique_file(input_path, has_name=False, has_code=False):
    """ Get 'historique' file """
    
    try:
        col_names = ["date", "capacity","available_mechanical","available_electrical"]
        if has_name:
            col_names.append("stationName")
        if has_code:
            col_names.append("stationCode")
        
        # In any case
        col_names += ["station_geo","operative"]
        
        df = pd.read_csv(input_path, header=None, parse_dates=[0],
            names= col_names,
            index_col="date"
           )
        
        return df
    except KeyError as e:
        msg = "Something wrong in '{}'. Error details:".format(input_path)
        print(msg)
        print(e)



def main():
    fetch_save_stations_info()

    yesterday = dt.datetime.now() - dt.timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    collect_statuses(date_str)

    return date_str


if __name__ == "__main__":
    date_str = main()
    msg = "=== Daily Update ran, processing {} ==="
    print(msg.format(date_str))
