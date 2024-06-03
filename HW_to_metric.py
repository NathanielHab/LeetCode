import pandas as pd

"""
This file takes data from a publicly available csv (online).
The file is called hw_200.csv and it contains a table with columns titled
    Index: row number
    Height: an individual's height in inches
    Weight: an individual's weight in pounds

This file extracts that data, converts both columns to metric and adds a BMI column.
This file produces a new CSV called hw_200_metric.csv
"""

# URL of the CSV file
url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"

# Read the CSV file from the URL into a DataFrame
df = pd.read_csv(url)

# Rename and remove columns
df = df.rename(columns={' Height(Inches)"': 'Height (cm)', ' "Weight(Pounds)"': 'Weight (kg)'})
df = df.drop(columns=['Index'])

# Convert to Metric
df['Height (cm)'] = df['Height (cm)'] * 2.54
df['Weight (kg)'] = df['Weight (kg)'] / 2.20

# Create BMI column
df['BMI'] = df['Weight (kg)'] / ((df['Height (cm)'] / 100) ** 2)

# We only want 2 decimals
df = df.round(2)

# Sort by "BMI" in increasing order, and reset index
df = df.sort_values(by="BMI", ascending=True)
df = df.reset_index(drop=True)

# Save CSV
df.to_csv("hw_200_metric.csv")
