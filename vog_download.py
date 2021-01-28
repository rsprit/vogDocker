#!/usr/bin/env python
# coding: utf-8

from urllib.request import urlretrieve
import requests
import pandas as pd

def download():
    
    url = 'http://fileshare.csb.univie.ac.at/vog/latest/'
    data_path = "../data/"
    vog = requests.get(url)
    vog = vog.text
    df = pd.DataFrame(pd.read_html(vog)[0]['Name'].dropna())
    file_list = list(df['Name'][1:])    
    
    for f in file_list:
        print(f'Downloading {f} to {data_path}')
        urlretrieve(url+f, data_path+f)    
    
