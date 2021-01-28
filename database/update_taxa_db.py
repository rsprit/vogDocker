#!/usr/bin/env python
# coding: utf-8


from ete3 import NCBITaxa
from datetime import datetime
dateTimeObj = datetime.now()
ncbi = NCBITaxa()
ncbi.update_taxonomy_database()
print(f"Updating taxonomy data from https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/ at {dateTimeObj}")




