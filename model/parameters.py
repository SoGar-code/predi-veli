import pandas as pd

stations_montrouge = [
    #"Molière - République",
    "21209",
    #"Arthur Auger - Jean Jaurès",
    "21205",
    #"Marne - Germain Dardan"
    "21212",
    #"Verdier - Jean Jaurès"
    "21215"
]

stations_paris = [
    #"Place des Victoires"
    "2006",
    #"Filles Saint-Thomas - Place de la Bourse"
    "2009",
    #"Bibliothèque Nationale - Richelieu"
    "2112",
    #"Mairie du 2ème"
    "2008",
    #"Chabanais - Petits Champs"
    "2111",
]

relevant_stations = stations_montrouge + stations_paris

week_ends = [5,6]
week_days = [0, 1, 2, 3, 4]

# Capacity are computed from existing station data
# See explo_2022-12-21 for more explanations
capacity_dict = {
    "2006":24,
    "2008":28,
    "2009":58,
    "2111":30,
    "2112":17,
    "21209":30,
    "21205":26,
    "21212":22,
    "21215":33
}

capacity_series = pd.Series(data=capacity_dict, name="capacity")
