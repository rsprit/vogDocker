#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
from datetime import datetime
dateTimeObj = datetime.now()

def check_version():
    '''check if versions in data directory and at fileshare are the same.'''
    
    url = "http://fileshare.csb.univie.ac.at/vog/latest/vog.species.list"
    result = requests.get(url)
    result = result.text
    latest_version = result[-4:-1]
    
    data_path = "../data/"
    species_list_df = pd.read_csv(data_path + "vog.species.list",
                              sep='\t',
                              header=0,
                              names=['SpeciesName', 'TaxonID', 'Phage', 'Source', 'Version']) \
    .assign(Phage=lambda p: p.Phage == 'phage')

    generated_version = str(species_list_df.iloc[0,4])

    if latest_version == generated_version:
        print(f'*** The latest vog_database version is {generated_version} ***')
    else:
        print(f'Warning: There are different versions of vogdb in directory {data_path} with version {generated_version} and at http://fileshare.csb.univie.ac.at/vog/latest/ with version {latest_version}.')
    







