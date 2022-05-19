#!/usr/bin/env python3

"""
Scripts for use in Prédi-Véli project

"""

def permute_cols_names(df):
    """ Permute column names (as needed between 2022-05-04 and 2022-05-10) """
    name_dict = {
        "available_mechanical": "operative", 
        "available_electrical": "available_mechanical",
        "operative": "available_electrical"
    }

    return df.rename(columns=name_dict)
