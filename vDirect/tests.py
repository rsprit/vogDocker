import pytest
import sys
from os import path
sys.path.append('../vogdb')
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from vogdb.database import Base
from vogdb.main import api, get_db
from vDirect.API_requests import *
from fastapi import FastAPI
from vogdb.models import Species_profile


""" Tests for API_requests.py
Test naming convention
test_MethodName_ExpectedBehavior_StateUnderTest:
e.g. test_isAdult_False_AgeLessThan18

test prefix is important for in order for the pytest to recognize it as test function. 
If there is no "test" prefix, pytest will not execute this test function.
"""

# testing mocks
# api mock
# @pytest.fixture
# def fastapi_app_mock():
"""FasAPI application set up."""
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine) # new database

def override_get_db():
    """ switch to testing database"""
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

api.dependency_overrides[get_db] = override_get_db
client = TestClient(api)

@pytest.fixture
def mock_species_profile():
    species_profile = Species_profile(
        species_name = "E.coli",
        taxon_id = 123,
        page= True,
        source= "NCBI",
        version= 1,
        protein_names = ["p1", "p2", "p3"])

    return species_profile




def test1(mock_species_profile):
    response = client.get()
#
# @pytest.fixture
# def test_vsearch_vogIds_allParameters():
#     response = vsearch(return_object='vog',
#                        format="df",
#                        id=[],
#                        pmin=23,
#                        pmax=100,
#                        smin=1,
#                        smax=50,
#                        functional_category=["XrXs"],
#                        l_stringency = True,
#                        mingGLCA = 4,
#                        phages_nonphages="phages_only")
#
#     expected = ["VOG01473", "VOG02138"]
#     assert response["id"].to_list() == expected
#
#
# def test_vsearch_speciesIds_allParameters():
#     response = vsearch(return_object="species",
#                        format="df",
#                        ids = [],
#                        name=["corona"],
#                        phage= False,
#                        source="NCBI Refseq",
#                        version="201")
#
#     expected = [11128, 290028, 1335626, 1384461, 2569586]
#     assert response["taxon_id"].to_list() == expected
#
#
# def test_vsearch_proteinIds_allParameters():
#     response = vsearch(return_object="protein",
#                        format="df",
#                        species_name = ["corona"],
#                        taxon_id = [11128, 290028, 1335626, 1384461, 2569586],
#                        VOG_id = ["VOG05566"])
#
#     expected = ["11128.NP_150082.1"]
#     assert response["id"].to_list() == expected
#
#
# def test_vsummary_vogIds_twoVogIds():
#     response = vsummary(return_object='vog', format="df", id=['VOG00001', 'VOG00002'])
#
#     expected = ['VOG00001', 'VOG00002']
#     assert response["id"].to_list() == expected
#
#
#
# def test_vsummary_speciesIds_twoTaxonIds():
#     response = vsummary(return_object='species', format="df", taxon_id=['2713308', '2591111'])
#
#     expected = [2591111, 2713308]
#     assert response["taxon_id"].to_list() == expected
#
#
# def test_vsummary_proteinIds_twoProteinIds():
#     response = vsummary(return_object="protein", format="df", id=["11128.NP_150082.1"])
#     expected = ['11128.NP_150082.1']
#     print(response)
#     assert response["id"].to_list() == expected
