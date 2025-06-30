import pandas as pd
import os
import wget
import zipfile
import shutil
from urllib.error import HTTPError

def check_format(urls):
	for url in urls:
		try:
			wget.download(url, 'data/ride_data')
		except HTTPError:
			url = url.strip('.zip')+'.csv.zip'
			wget.download(url, 'data/ride_data')

def get_data_year(year):
    prefix = 'https://s3.amazonaws.com/tripdata/' #'2020' '-citibike-tripdata.zip'
    months = [str(i) if len(str(i)) == 2 else '0'+str(i) for i in range(1, 13)]
    suffix = '-citibike-tripdata'
    dot = '.zip'
    urls = [prefix+year+month+suffix+dot for month in months]
    url = prefix+year+suffix+dot
    zip_dir = year+suffix+dot

    if not os.path.exists('data/ride_data'):
        os.makedirs('data/ride_data')
    
    if int(year) < 2024:
        wget.download(url, 'data/ride_data')

    elif int(year) == 2024:
        check_format(urls)

    elif int(year) == 2025:
        check_format(urls[0:5])    
    
    for filename in os.listdir(os.getcwd()+'/data/ride_data'):
        with zipfile.ZipFile('data/ride_data/'+filename, 'r') as zip_ref:
            zip_ref.extractall('data/ride_data')
            for subdir in zip_ref.namelist():
                if subdir.endswith('.zip') and not subdir.startswith('__MACOSX'):
                    with zipfile.ZipFile('data/ride_data/'+subdir, 'r') as sub_ref:
                        sub_ref.extractall('data/ride_data')

    df_list = []

    for filename in os.listdir(os.getcwd()+'/data/ride_data'):
        if filename.endswith('.csv'):
            df = pd.read_csv('data/ride_data/'+filename, usecols = ['rideable_type', 'started_at', 'ended_at', \
                                                           'start_lat', 'start_lng', 'end_lat', 'end_lng', \
                                                           'member_casual'])
            df = df.sample(int(df.shape[0] / 10000), random_state=1)
            df_list.append(df)

    shutil.rmtree('data/ride_data')
    return pd.concat(df_list, ignore_index = True)

def get_ride_data():
    df_list = []

    for i in range(2020, 2026):
        df = get_data_year(str(i))
        df_list.append(df)
    
    return pd.concat(df_list, ignore_index = True)
