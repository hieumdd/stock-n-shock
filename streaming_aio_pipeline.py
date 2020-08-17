#!/usr/bin/env python
# coding: utf-8

# In[22]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import requests
import zipfile
import io
import os
import re
from datetime import datetime
from tqdm import tqdm
import logging
import argparse


# In[42]:


logging.basicConfig(filename='bad_date.log', level=logging.INFO)
parser = argparse.ArgumentParser()

parser.add_argument('-s', '--start',
                    type=str,
                    help='Start Date')

args = parser.parse_args()
start_date = args.start

# In[2]:


def get_all_csv(url):
    with requests.get(url) as r:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall('csv/')


# In[3]:


base_url = 'http://images1.cafef.vn/data/{0}/CafeF.SolieuGD.{1}.zip'


# In[8]:


today = datetime.now()


# In[9]:


def to_1st_date_ranges(x):
    return x.strftime('%Y%m%d')

def to_2nd_date_ranges(x):
    return x.strftime('%d%m%Y')


# In[15]:


date_1 = to_1st_date_ranges(today)
date_2 = to_2nd_date_ranges(today)


# In[17]:


if start_date == None:
    date_1 = to_1st_date_ranges(datetime.now())
    date_2 = to_2nd_date_ranges(datetime.now())
    url = base_url.format(date_1, date_2)
else:
    date_1 = to_1st_date_ranges(datetime.strptime(start_date, '%Y-%m-%d'))
    date_2 = to_2nd_date_ranges(datetime.strptime(start_date, '%Y-%m-%d'))
    url = base_url.format(date_1, date_2)

print(url)
try:
    get_all_csv(url)
except:
    print(f'Error at {url}')
    logging.info(date_1)


# In[19]:


all_csv = os.listdir('./csv')


# In[21]:


def etl(filenames, floor):
    engine = create_engine('sqlite:///vn.db', echo=False)
    regex = re.compile(f'.+{floor}.+')
    matches = [m for m in map(regex.match, filenames) if m is not None]
    for match in tqdm(matches):
        df = pd.read_csv('./csv/' + match.group(0))
        df = df.rename(mapper={
            '<Ticker>': 'ticker',
            '<DTYYYYMMDD>': 'date',
            '<Open>': 'open',
            '<High>': 'high',
            '<Low>': 'low',
            '<Close>': 'close',
            '<Volume>': 'volume'
        }, axis=1)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df.to_sql(floor, con=engine, if_exists='append', index=False)


# In[23]:


etl(all_csv, 'HSX')
etl(all_csv, 'HNX')
etl(all_csv, 'UPCOM')


# In[38]:


for i in all_csv:
    os.remove('./csv/' + i)

