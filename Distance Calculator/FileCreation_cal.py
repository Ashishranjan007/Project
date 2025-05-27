# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 11:50:04 2025

@author: Ashish Ranjan
@Email: ashishranjan5323@gmail.com
"""

# !pip install geopy

import pandas as pd
from geopy.distance import geodesic

file_path = r"C:\Users\ACFL\Downloads\filecreation3.xlsx"
df = pd.read_excel(file_path)

df.head()

print(df[['Latitude', 'Longitude', 'Latitude.1', 'Longitude.1']].isna().sum())

df = df.dropna(subset=['Latitude', 'Longitude', 'Latitude.1', 'Longitude.1'])

print(df[['Latitude', 'Longitude', 'Latitude.1', 'Longitude.1']].isna().sum())

df = df.dropna(subset=['Latitude', 'Longitude', 'Latitude.1', 'Longitude.1']).copy()


def calculate_distance(row):
    coord1 = (row["Latitude"], row["Longitude"])
    coord2 = (row["Latitude.1"], row["Longitude.1"])

    return geodesic(coord1, coord2).kilometers


df["Distance (km)"] = df.apply(calculate_distance, axis=1)

df.head()

output_file = r"C:\Users\ACFL\Downloads\file_creation_result.xlsx"
df.to_excel(output_file, index=False)

print(f"✅ Final file with calculations. Output saved to {output_file}")
