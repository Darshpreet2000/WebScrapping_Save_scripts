#!/usr/bin/env python

import os
import requests
import json
import datetime
import shutil
from bs4 import BeautifulSoup

here = os.path.dirname(os.path.abspath(__file__))
hospital_id = os.path.basename(here)

url ='https://www.cooperhealth.org/patients-visitors/financial-matters/chargemaster'

today = datetime.datetime.today().strftime('%Y-%m-%d')
outdir = os.path.join(here, 'latest')
if not os.path.exists(outdir):
    os.mkdir(outdir)

response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')

# Each folder will have a list of records
records = []

for entry in soup.find_all('a', href=True):
    download_url = entry['href']
    if '.csv' in download_url:  
        filename =  os.path.basename(download_url.split('?')[0])  

        # We want to get the original file, not write a new one
        output_file = os.path.join(outdir, filename)
        os.system('wget -O "%s" "%s"' % (output_file, download_url))

        record = { 'hospital_id': hospital_id,
                   'filename': filename,
                   'date': today,
                   'uri': filename,
                   'name': filename,
                   'url': download_url }

        records.append(record)

# Keep json record of all files included
records_file = os.path.join(outdir, 'records.json')
with open(records_file, 'w') as filey:
    filey.write(json.dumps(records, indent=4))
