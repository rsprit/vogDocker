#!/usr/bin/env python
# coding: utf-8

# In[11]:


#!/usr/bin/env python
# coding: utf-8

# Import Section----------------------------------------------------------------

import requests
import pandas as pd
from datetime import datetime
dateTimeObj = datetime.now()

import generate_db as gen
import check
import os
import time
import vog_download as vog

# Main--------------------------------------------------------------------------

class Monitor:

    def __init__(self):
        self.url = "http://fileshare.csb.univie.ac.at/vog/"
        self.last_data = None
        self.last_data_fname = "last_data"
    
    def exists(self): 
        '''Check if data were retrieved from fileshare before or if this is the first call.'''
        
        result = os.path.exists(self.last_data_fname)
        if not result:
            print(f"{self.last_data_fname} file does not exist. Database is created from {self.url} for the first time at {dateTimeObj}")
        return result
    
    def initiate(self):
        '''Connect to url, fetch the table of dates of modification and store the file locally.
        Download files from fileshare url.
        Execute the check script to control version equality between fileshare site and download directory.
        Execute generate_db script to build a mysql database for the first time.
        '''
           
        result = requests.get(self.url)
        
        if result.status_code != 200:
            raise ValueError('Service failed to respond')
        else:
            result = result.text
        
        data = pd.read_html(result)[0]["Last modified"].dropna()
        data.to_csv(self.last_data_fname, sep = ";", index = False)
        
        print(f'Starting download at {dateTimeObj}')
        vog.download()
        print('Download completed')
        
        check.check_version()
        gen.generate_db() 
        print(f"Last modification at {str(data.iloc[0,0])}")
    
    def get_last(self):
        '''Build a dataframe with the data that were retrieved from url.'''
                  
        return pd.read_csv(self.last_data_fname, sep = ";")
           
    def update(self, debug = False):
        """Use the modification data from the dataframe and call generate_db function if the data changed."""

        self.last_data = self.get_last()

        result = requests.get(self.url)
        if result.status_code != 200:
            raise ValueError('Service failed to respond')
        else:
            result = result.text
            
        data = pd.DataFrame(pd.read_html(result)[0]["Last modified"].dropna())         

        if not self.check_equality(self.numpy1DArrayToList(self.last_data.values), self.numpy1DArrayToList(data.values)):  
            data.to_csv(self.last_data_fname, sep = ";", index = False) # write a new last_data file
            print(f'There is a new version at {self.url} that was modified at {str(data.iloc[0,0])}.\nStarting download at {dateTimeObj}')
            vog.download()
            print('Download completed') 
            check.check_version()
            
            print('Updating database') # comment for log file
            gen.generate_db() # drop the old database and generate a new one with the latest vogdb version
            print(f"Mysql database created. {dateTimeObj}")

        else:
            check.check_version()
            print(f"No modified files at {self.url}, last modified: {str(data.iloc[-1,0])}. \nChecked at {dateTimeObj}")

        if debug:
            return self.last_data.values, data.values

    @staticmethod  
    def check_equality(old, new):
        """Check if all items in the lists old and new are identical.""" 
                  
        old = list(old) 
        new = list(new)
        for i, item in enumerate(new):
            try:
                if item != old[i]:
                    return False
            except IndexError:
                return False
        return True
    
    @staticmethod
    def numpy1DArrayToList(npArray):
        return npArray.reshape(len(npArray)).tolist()
    
    def run(self):
        '''Decides if a the database initiation script or the database update script is executed.'''
                  
        if not self.exists():
            self.initiate()
        else:
            self.update()

# Run it------------------------------------------------------------------------

if __name__ == "__main__":
    
    Monitor().run()


# In[ ]:




