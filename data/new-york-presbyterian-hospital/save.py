import csv
import pymysql
import os
import MySQLdb
import sys
from glob import glob
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine
full_path = os.path.realpath(__file__)
print(os.path.dirname(full_path))
str1=os.path.dirname(full_path)
str2=str1.split('\\')
n =len(str2)
foldername= (str2[n-1])
here = os.path.dirname(os.path.abspath(__file__))
folder = os.path.basename(here)
year = datetime.datetime.today().year
today = datetime.datetime.today().strftime('%Y-%m-%d')
conn = pymysql.connect(host='database-1.csh4odvd9r2v.us-east-2.rds.amazonaws.com', port=3306, user='admin', passwd='', db='hospital', autocommit=True)
csv_database = create_engine('mysql+mysqldb://admin:@database-1.csh4odvd9r2v.us-east-2.rds.amazonaws.com/hospital')
print("CONNECTED")
cursor = conn.cursor()
str="DROP TABLE IF EXISTS `"+foldername+"`"
cursor.execute(str)
cursor.execute(str)
print("HERE "+today)
from datetime import datetime
dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%d-%b-%Y")
print('Current Timestamp : ', timestampStr)
start=0
file = os.path.join(sys.path[0], "data-latest.csv")
chunksize = 100000
i = 0
j = 1
for df in pd.read_csv(file, chunksize=chunksize, iterator=True):
      df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
      df.index += j
      i+=1
      df.to_sql(foldername, csv_database, if_exists='append')
      j = df.index[-1] + 1
cursor.close()
print("DONE")

 #No parsing needed here
# Remove empty rows