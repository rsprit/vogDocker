from ete3 import NCBITaxa
from datetime import datetime
dateTimeObj = datetime.now()
ncbi = NCBITaxa()
ncbi.update_taxonomy_database()
print(f"Updating taxonomy data at {dateTimeObj}")
