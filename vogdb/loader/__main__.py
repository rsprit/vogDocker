import os
import sys

from ..database import database_url
from . import load_frames, save_db_sql


data_dir = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("VOG_DATA")

if not data_dir:
    print(f"usage: {sys.argv[0]} <data directory>")
    sys.exit(2)

if data_dir[:-1] != "/":
    data_dir += "/"

vog, species, protein, member = load_frames(data_dir)

save_db_sql(database_url(), vog, species, protein, member)
