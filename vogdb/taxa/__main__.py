from datetime import datetime
from .support import ncbi_taxa_path
from ete3.ncbi_taxonomy.ncbiquery import update_db

dbfile = ncbi_taxa_path()
update_db(dbfile)
print(f"{datetime.now()}: Updated taxonomy data in {dbfile} from https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/")
