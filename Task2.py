import argparse
import hashlib
import os
from urllib.parse import urlparse
from os import listdir
from datetime import datetime
import pandas as pd
import json

#3lshan a3rf el time bta3 el execution
t = datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument("dir", help = "Please enter the path of directory")
parser.add_argument("-u", "--unix", action="store_true", dest="unixformat", default=False, help="keep the (time in) and (time out) in unix format")
args = parser.parse_args()

#hnbd2 nshof feh duplicates wla la
checksums = {}
duplicates = []

#hgyb kol el files 2li fe el directory
files = [item for item in listdir(args.dir) if (".json" in item)]

#loop bt3dy 3la list el files
for filename in files:
        #checksum =hashlib.sha256(filename.encode('utf-8')).hexdigest()
        # Append duplicate to a list if the checksum is found
        md5_hash = hashlib.md5()
        a_file = open(filename, "rb")
        content = a_file.read()
        md5_hash.update(content)
        checksum = md5_hash.hexdigest()
        a_file.close()
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

print(f"Found Duplicates: {duplicates}")

for i in duplicates:
    os.remove(i)

#load the files and start transformation  
for filename in files:
    if filename not in duplicates:
        records = [json.loads(line) for line in open(filename) if '_heartbeat_' not in json.loads(line )]
        df = pd.json_normalize(records)
        df = df.dropna()
        # to cut the web_browser from column 'a'
        new = df["a"].str.split("/", n=1, expand=True)  # Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4...
        df['web_browser'] = new[0]

        # to cut the operating_sytem from column 'a'
        new = df["a"].str.split("(", n=1, expand=True)
        new = new[1].str.split(" ", n=1, expand=True)
        new = new[0].str.split(";", n=1, expand=True)
        df['operating_sys'] = new[0]

        df['city'] = df['cy']
        df['time_zone'] = df['tz']

        # to get the website name 'www.example.com' out of column 'r'
        new = df["r"].str.split("//", n=1, expand=True)
        new = new[1].str.split("/", n=1, expand=True)
        df['from_url'] = new[0]

        # to get the website name 'www.example.com' out of column 'u'
        new = df["u"].str.split("//", n=1, expand=True)
        new = new[1].str.split("/", n=1, expand=True)
        df['to_url'] = new[0]

        df['longitude'] = df['ll'].str[0]
        df['latitude'] = df['ll'].str[1]
        if args.unixformat:
            df['time_in'] = df['t']
            df['time_out'] = df['hc']
        else:
            df['time_in'] = pd.to_datetime(df['t'],unit='s')
            df['time_out'] = pd.to_datetime(df['hc'],unit='s')
        df=df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out',]]

       
	#print number of rows transformed
        print('there are {} rows transformed from file:{}'.format(df.shape[0], args.dir+"/"+filename)) 
        
        #to save the files
        file = filename.replace('.json',' ')
        df.to_csv('D:\Task2\\target\\'+file+'.csv')
	
	
#print the execution time of the script
executionTime = (datetime.now() - t)
print("Total execution time is: {}".format(executionTime))

