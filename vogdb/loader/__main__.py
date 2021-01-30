import os
import sys

from ..database import database_url
from .support import generate_db


data_dir = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("VOG_DATA")

if not data_dir:
    print(f"usage: {sys.argv[0]} <data directory>")
    sys.exit(2)

if data_dir[:-1] != "/":
    data_dir += "/"

generate_db(data_dir, database_url())
