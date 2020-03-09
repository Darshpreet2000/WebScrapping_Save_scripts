import csv
import pymysql
import os
import MySQLdb
import sys
from glob import glob
import json
import pandas
import datetime
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
print("CONNECTED")
cursor = conn.cursor()
str="DROP TABLE IF EXISTS `"+foldername+"`"
cursor.execute(str)
str="CREATE TABLE `" +foldername + "` (price varchar(20),description varchar(100),charge_type varchar(100),DateUpdated Varchar(100));"
cursor.execute(str)
print("HERE "+today)
from datetime import datetime
dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%d-%b-%Y")
print('Current Timestamp : ', timestampStr)
strSQL = "INSERT INTO `"+ foldername +"` ( price,`description`,charge_type,DateUpdated)  VALUES(%s, %s,%s,'"+today+"');" 
start=0
with open(os.path.join(sys.path[0], "data-latest.csv"), "r") as f:
       csv_data = csv.reader(f)
       for row in csv_data:
          if start==0:
            start=start+1
            continue
          cursor.execute(strSQL, row)

cursor.close()
print("DONE")

 #No parsing needed here
# Remove empty rows