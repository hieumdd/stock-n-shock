#!/usr/bin/env python
# coding: utf-8

# In[54]:


import numpy as np
import pandas as pd
import os
import re
from sqlalchemy import create_engine
import argparse
from tqdm import tqdm


# In[55]:


all_csv = os.listdir('./csv')


# In[53]:


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


# In[56]:


etl(all_csv, 'HSX')
etl(all_csv, 'HNX')
etl(all_csv, 'UPCOM')

