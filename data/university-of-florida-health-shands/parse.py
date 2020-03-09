#!/usr/bin/env python

import os
from glob import glob
import json
import pandas
import datetime
import sys

full_path = os.path.realpath(__file__)
print(os.path.dirname(full_path))
str1=os.path.dirname(full_path)
str2=str1.split('\\')
n=len(str2)
foldername= (str2[n-1])
here = os.path.dirname(os.path.abspath(__file__))
folder = os.path.basename(here)
folder = os.path.basename(here)
latest = '%s/latest' % here
year = datetime.datetime.today().year

output_data = os.path.join(here, 'data-latest.csv')

# Don't continue if we don't have latest folder
if not os.path.exists(latest):
    print('%s does not have parsed data.' % folder)
    sys.exit(0)

# Don't continue if we don't have results.json
results_json = os.path.join(latest, 'records.json')
if not os.path.exists(results_json):
    print('%s does not have results.json' % folder)
    sys.exit(1)

with open(results_json, 'r') as filey:
    results = json.loads(filey.read())

columns = ['price', 
           'description',
           'charge_type'
           ]

today = datetime.datetime.today().strftime('%Y-%m-%d')
df = pandas.DataFrame(columns=columns)

for result in results:
    filename = os.path.join(latest, result['filename'])
    if not os.path.exists(filename):
        print('%s is not found in latest folder.' % filename)
        continue

    if os.stat(filename).st_size == 0:
        print('%s is empty, skipping.' % filename)
        continue

    charge_type = 'standard'
    if "drg" in filename.lower():
        charge_type = "drg"

    print("Parsing %s" % filename)

    if filename.endswith('csv'):

        # ['BILLING_DESC', 'UNIT', 'PRICE']
        if charge_type == "standard":
            content = pandas.read_csv(filename, skiprows=4)
            for row in content.iterrows():
                idx = df.shape[0] + 1
                price = row[1]['PRICE']
                entry = [                         # charge code
                         price,                        # price
                         row[1]['BILLING_DESC']
                        ,'standard']            
                df.loc[idx,:] = entry


        # ['MSDRG', 'Average (Mean) Charges Per Discharge', 'Unnamed: 2']
        else:

            content = pandas.read_csv(filename, skiprows=3)
            for row in content.iterrows():
                idx = df.shape[0] + 1
                price = row[1]['Average (Mean) Charges Per Discharge']
                entry = [price, # price
                         row[1]['MSDRG'],
                         'standard'
                        ]            
                df.loc[idx,:] = entry


# Remove empty rows
df = df.dropna(how='all')

# Save data!
print(df.shape)
df.to_csv(output_data, encoding='utf-8', index=False)