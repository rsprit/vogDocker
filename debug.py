import os
import uvicorn

if __name__ == '__main__':
    os.environ["MYSQL_HOST"] = "localhost"
    os.environ["MYSQL_USER"] = "vog"
    os.environ["MYSQL_PASSWORD"] = "password"
    os.environ["MYSQL_DATABASE"] = "vogdb"

    uvicorn.run("vogdb:api", port=8000, reload=True)