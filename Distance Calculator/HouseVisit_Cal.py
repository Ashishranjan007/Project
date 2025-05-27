# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 11:50:04 2025

@author: Ashish Ranjan
@Email: ashishranjan5323@gmail.com
"""

# !pip install geopy

import pandas as pd
from geopy.distance import geodesic

file_path = r"C:\Users\ACFL\Downloads\housevisit3.xlsx"
df = pd.read_excel(file_path)

df.head()

print(df[['Latitude', 'Longitude', 'Latitude_Branch', 'Longitude_Branch']].isna().sum())

df = df.dropna(subset=['Latitude', 'Longitude', 'Latitude_Branch', 'Longitude_Branch'])

print(df[['Latitude', 'Longitude', 'Latitude_Branch', 'Longitude_Branch']].isna().sum())

df = df.dropna(subset=['Latitude', 'Longitude', 'Latitude_Branch', 'Longitude_Branch']).copy()


def calculate_distance(row):
    coord1 = (row["Latitude"], row["Longitude"])
    coord2 = (row["Latitude_Branch"], row["Longitude_Branch"])

    return geodesic(coord1, coord2).kilometers


df["Distance (km)"] = df.apply(calculate_distance, axis=1)

df.head()

output_file = r"C:\Users\ACFL\Downloads\housevisit_result.xlsx"
df.to_excel(output_file, index=False)

print(f"✅ Final file with calculations. Output saved to {output_file}")
