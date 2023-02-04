"""
Data preprocessing for simple random forest model

"""

import os
from pathlib import Path
import pandas as pd

input_str = './../../data'
run_str = '../..'

save_path = Path(__file__).parent.resolve()
data_path = Path(input_str).resolve()

os.chdir(Path(run_str))

from sklearn.model_selection import train_test_split

from model.parameters import stations_montrouge, stations_paris, relevant_stations
from model.parameters import week_ends, week_days, capacity_series

"""
Collecting parquet files - daily summaries

From the "last modified" dates, it appears that the data is "regular" (*i.e.* without manual manipulations) from 2022-05-20 onwards!

As a first iteration, it seems acceptable to start with this data only! (rows noted `parquet_files[20:]` as of 2022-12-21)
"""
parquet_files = sorted(data_path.glob('*.parquet'))


def process_daily_summaries(file_path, current_stations=stations_paris):
    """ 
    Process data and return suitable dataframe, keeping only required stations 
    """
    current_df = pd.read_parquet(file_path, columns=["file_time", "stationCode", "available_mechanical", "available_electrical"])
    current_df["stationCode"] = current_df["stationCode"].astype("string")
    
    # Keep only stations of interest
    current_df = current_df[current_df["stationCode"].isin(current_stations)]
    
    return current_df


def add_time_features(aux):
    aux["local_hour"] = aux.index.hour
    aux["local_minute"] = aux.index.minute
    aux["week_day"] = aux.index.weekday
    
    if 'date' not in aux.columns:
        aux["date"] = aux.index.date
    
    return aux


def add_capacity_features(summary_df):
    # Merge with capacities
    aux = summary_df.merge(capacity_series, left_on="stationCode", right_index=True)
    
    aux["free_docks"] = aux["capacity"] - aux["available_mechanical"] - aux["available_electrical"]
    aux["free_docks_ratio"] = aux["free_docks"]/aux["capacity"]
    
    return aux


def get_annoted_data():
    
    summary_df = pd.concat(process_daily_summaries(file_path, relevant_stations) for file_path in parquet_files[20:])
    aux = add_capacity_features(summary_df)
    aux = add_time_features(aux)
    
    return aux


def get_ratios(local_hour, local_minute, stations=None, week_day=None):
    """ 
    Args:
        * stations (optional): stations to consider, provided as list of strings.
        * week_day should be a list of integers (0=Monday, 6=Sunday)
    """
    aux = get_annoted_data()

    mask = (aux["local_hour"]==local_hour)&(aux["local_minute"]==local_minute)
    
    if stations:
        mask &= (aux["stationCode"].isin(stations))
        
    if week_day:
        mask &= (aux["week_day"].isin(week_day))
    
    return aux[mask]


def get_date_data(date_data_path = None):
    """ 
    Load date_data from save
    """
    if not date_data_path:
        root_folder = Path(__file__).parent.resolve()
        date_data_path = root_folder/"date_data.csv"

    date_data_df = pd.read_csv(date_data_path)
    return date_data_df

"""
## Prepare and save dataset

Based on the processing above, generating the dataset requires only:
1. collect "start data" (*i.e.* free dock ratios at 9:00 on the date at hand)
2. collect "target data" (*i.e.* free dock ratios at 9:30 on the date at hand)
3. pivot these to get one line per date with columns for the stations under consideration
4. merge with date data
5. ensure alignment between features and targets
"""

def get_ratios_per_date(local_hour, local_minute, stations=None, week_day=None):
    aux = get_ratios(local_hour, local_minute, stations, week_days).reset_index()[["stationCode", "free_docks_ratio", "date"]]
    
    return aux.pivot(index=["date"], columns=["stationCode"])["free_docks_ratio"].sort_index()

def get_feature_matrix():

    start_data = get_ratios_per_date(9, 0, relevant_stations)

    # Load date_data
    date_data = pd.read_csv(save_path/"../date_data.csv")

    # Feature matrix
    X0 = start_data.merge(date_data, left_index=True, right_index=True)

    return X0
    

def generate_save_features_target():
    X0 = get_feature_matrix()

    # Get target 
    y0 = get_ratios_per_date(9, 30, stations_paris)

    # Check alignment of start and target data
    if (X0.index != y0.index).sum() != 0:
        raise ValueError("Misalignment of start and target data")

    X_train, X_test, y_train, y_test = train_test_split(X0, y0, test_size=0.2)

    X_train.to_csv(save_path/"features_train.csv")
    X_test.to_csv(save_path/"features_test.csv")
    y_train.to_csv(save_path/"target_train.csv")
    y_test.to_csv(save_path/"target_test.csv")

def get_train_data():
    X_train = pd.read_csv(save_path/"features_train.csv")
    y_train = pd.read_csv(save_path/"target_train.csv")

    return X_train, y_train

def get_test_data():
    X_test = pd.read_csv(save_path/"features_test.csv")
    y_test = pd.read_csv(save_path/"target_test.csv")

    return X_test, y_test

