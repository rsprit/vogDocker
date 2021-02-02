import os

os.environ["MYSQL_HOST"] = "localhost"
os.environ["MYSQL_USER"] = "vog"
os.environ["MYSQL_PASSWORD"] = "password"
os.environ["MYSQL_DATABASE"] = "vogdb"

from vogdb import api
import uvicorn

uvicorn.run(api, port=8000)