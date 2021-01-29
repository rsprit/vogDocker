from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

""" This module is used for establishing a connection to the MYSQL database
Note: you might need to change the MYSQL login credentials 
"""

# MySQL database connection
username = "root"
password = "password"
server = "localhost"
database = "vogdb"
SQLALCHEMY_DATABASE_URL = "mysql+pymysql://{0}:{1}@{2}/{3}".format(username, password, server, database)

# Create an engine object.
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False)

# Each instance of the SessionLocal class will be a database session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# returns a class. Later we will inherit from this class to create each of the database models or classes
Base = declarative_base()
