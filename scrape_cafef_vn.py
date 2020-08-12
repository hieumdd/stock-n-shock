# pylint: disable=E1101
import numpy as np
import pandas as pd
import requests
import zipfile
import io
import datetime
from tqdm import tqdm

def get_all_csv(url):
    with requests.get(url) as r:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall('csv/')

base_url = 'http://images1.cafef.vn/data/{0}/CafeF.SolieuGD.{1}.zip'

date_ranges = pd.date_range('2020-01-01', '2020-07-31')

def to_1st_date_ranges(x):
    return x.strftime('%Y%m%d')

def to_2nd_date_ranges(x):
    return x.strftime('%d%m%Y')

date_ranges_1 = np.vectorize(to_1st_date_ranges)(date_ranges.date)
date_ranges_2 = np.vectorize(to_2nd_date_ranges)(date_ranges.date)

bad_date_ranges = []
for i in tqdm(np.arange(0,3)):
    url = base_url.format(date_ranges_1[i], date_ranges_2[i])
    print(url)
    try:
        get_all_csv(url)
    except:
        print(f'Error at {url}')
        bad_date_ranges.append((date_ranges_1[i], date_ranges_2[i]))