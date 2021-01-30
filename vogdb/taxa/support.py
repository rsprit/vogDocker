import os
from ete3 import NCBITaxa

def ncbi_taxa_path():
    default_dir = os.path.join(os.environ["HOME"], ".etetoolkit")
    db_dir = os.environ.get("NCBI_DATA", default_dir)
    return os.path.join(db_dir, "taxa.sqlite")

def ncbi_taxa():
    return NCBITaxa(ncbi_taxa_path())
