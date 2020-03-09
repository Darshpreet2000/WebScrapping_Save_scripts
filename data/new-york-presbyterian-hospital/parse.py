#!/usr/bin/env python

import os
from glob import glob
import json
import pandas
import datetime
import sys
here = os.path.dirname(os.path.abspath(__file__))
folder = os.path.basename(here)
latest = '%s/latest' % here
year = datetime.datetime.today().year

today = datetime.datetime.today().strftime('%Y-%m-%d')
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

columns = [
           'price', 
           'description', 
           'charge_type',
             'DateUpdated'
           ]

df = pandas.DataFrame(columns=columns)

# First parse standard charges (doesn't have DRG header)
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

    if filename.endswith('xlsx'):

        # ['MS DRG', 'Average Charge per Case']
        if charge_type == "drg":
            content = pandas.read_excel(filename, skiprows=2)
             
            for row in content.iterrows():
                idx = df.shape[0] + 1
                entry = [                   # charge code
                         row[1]['Average Charge per Case'],   # price
                         None, 
                         charge_type,
                         today]
                df.loc[idx,:] = entry

        else:
            content = pandas.read_excel(filename, skiprows=2)
            # ['Category', 'IND', 'Charge Description', 'Price']
            for row in content.iterrows():
                idx = df.shape[0] + 1
                entry = [                 # charge code
                         row[1]['Price'],               # price
                         row[1]['Charge Description'],  # description
                         charge_type,
                         today]
                df.loc[idx,:] = entry


# Remove empty rows
df = df.dropna(how='all')

# Save data!
print(df.shape)
df.to_csv(output_data, encoding='utf-8', index=False)