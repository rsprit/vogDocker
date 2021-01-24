import os
import string
import random

import pytest
import sys
from fastapi.testclient import TestClient
import pandas as pd
from sqlalchemy.orm import sessionmaker
from os import path
sys.path.append('../tests')
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from tests import generate_test_db
sys.path.append('../vogdb')
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from vogdb.database import Base
from vogdb.main import api, get_db
from httpx import AsyncClient
import time

""" Tests for vogdb.main.py
Test naming convention
test_MethodName_ExpectedBehavior_StateUnderTest:
e.g. test_isAdult_False_AgeLessThan18

test prefix is important for in order for the pytest to recognize it as test function. 
If there is no "test" prefix, pytest will not execute this test function.
"""



@pytest.fixture(scope="session")
def get_test_client():
    # connect to test database
    engine = generate_test_db.connect_to_database()
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # create the database
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        try:
            db = TestingSessionLocal()
            yield db
        finally:
            db.close()

    api.dependency_overrides[get_db] = override_get_db
    client = TestClient(api)
    return client


#------------------------
# vSummary/vog
#------------------------

def test_vsummaryVog_vogProfiles_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00002", "VOG00234", "VOG03456"]}
    response = client.get(url="/vsummary/vog/", params=params)
    expected = ["VOG00001", "VOG00002", "VOG00234", "VOG03456"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate

    assert data["id"].to_list() == expected

def test_vsummaryVog_vogProfileFieldNames_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00002", "VOG00234", "VOG03456"]}
    response = client.get(url="/vsummary/vog/", params=params)
    expected = ['id', 'protein_count', 'species_count', 'function',
       'consensus_function', 'genomes_in_group', 'genomes_total_in_LCA',
       'ancestors', 'h_stringency', 'm_stringency', 'l_stringency',
       'proteins']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsummaryVog_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00002", "VOG00234", "VOG03456"]}

    response = client.get(url="/vsummary/vog/", params=params)
    expected_response = client.get(url="/vsummary/vog/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsummaryVog_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00002", "VOG00234", "VOG03456"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsummary/vog/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vsummaryVog_ERROR422_integers(get_test_client):
    client = get_test_client
    params = {"id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsummary/vog/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummaryVog_ERROR404_randomString(get_test_client):
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vsummary/vog/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsummaryVog_ERROR422_noParameter(get_test_client):
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vsummary/vog/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummaryVog_ERROR403_longParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vsummary/vog/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing


#------------------------
# vSummary/species
#------------------------

def test_vsummarySpecies_SpeciesProfiles_ids(get_test_client):
    client = get_test_client
    #taxon IDs are Integers
    params = {"taxon_id": [2713301, 2713308]}
    # params = {"taxon_id": ["2713301", "2713308"]}
    response = client.get(url="/vsummary/species/", params=params)
    expected = [2713301, 2713308]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate

    assert data["taxon_id"].to_list() == expected

def test_vsummarySpecies_speciesProfileFieldNames_ids(get_test_client):
    client = get_test_client
    #taxon IDs are Integers
    params = {"taxon_id": [2713301, 2713308]}
    response = client.get(url="/vsummary/species/", params=params)
    expected = ['species_name', 'taxon_id', 'phage', 'source',
       'version']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsummarySpecies_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["2713301", "2713308"]}

    response = client.get(url="/vsummary/species/", params=params)
    expected_response = client.get(url="/vsummary/species/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsummarySpecies_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["2713301", "2713308"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsummary/species/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

# but species IDS are integers
def test_vsummarySpecies_ERROR422_integers(get_test_client):
    client = get_test_client
    params = {"taxon_id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsummary/species/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummarySpecies_ERROR404_randomString(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["SOMETHING"]}
    response = client.get(url="/vsummary/species/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsummarySpecies_ERROR422_noParameter(get_test_client):
    client = get_test_client
    params = {"taxon_id": None}
    response = client.get(url="/vsummary/species/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummarySpecies_ERROR403_longParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"taxon_id": long_string }
    response = client.get(url="/vsummary/species/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing

#------------------------
# vSummary/protein
#------------------------

def test_vsummaryProtein_ProteinProfiles_ids(get_test_client):
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vsummary/protein/", params=params)
    expected = ["11128.NP_150082.1", "2301601.YP_009812740.1"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate

    assert data["id"].to_list() == expected

def test_vsummaryProtein_proteinProfileFieldNames_ids(get_test_client):
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vsummary/protein/", params=params)
    expected = ['id', 'vog_id', 'taxon_id', 'species_name']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsummaryProtein_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    response = client.get(url="/vsummary/protein/", params=params)
    expected_response = client.get(url="/vsummary/protein/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsummaryProtein_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsummary/protein/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vsummaryProtein_ERROR422_integers(get_test_client):
    client = get_test_client
    params = {"id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsummary/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummaryProtein_ERROR404_randomString(get_test_client):
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vsummary/protein/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsummaryProtein_ERROR422_noParameter(get_test_client):
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vsummary/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsummaryProtein_ERROR403_longParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vsummary/protein/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing



#------------------------
# vfetch/vog
#------------------------

# ToDo need to create extra test folder for hmm, mse stuff...
def test_vfetchVogHMM_HMMProfiles_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = ["VOG00234", "VOG00003"]

    data = response.json()

    for i in range(len(expected)):
        # searching vogid in first 50 characters
        assert any(expected[i] in hmm[0:50] for hmm in data)


def test_vfetchVogHMM_HMMFieldTypes_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = type(list())

    data = response.json()
    assert type(data) == expected


def test_vfetchVogHMM_isIdempotent_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}

    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected_response = client.get(url="/vfetch/vog/hmm/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vfetchVogHMM__ResponseUnder500ms_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    end = time.time()

    response_time = end-start
    assert response_time <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vfetchVogHMM_ERROR422_integers(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": [123132]}
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchVogHMM_ERROR404_randomString(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vfetchVogHMM_ERROR422_noParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchVogHMM_ERROR403_longParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vfetch/vog/hmm/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing


#------------------------
# vfetch/msa
#------------------------

# ToDo need to create extra test folder for hmm, mse stuff...

def test_vfetchVogMSA_MSAFieldTypes_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}
    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected = type(list())

    data = response.json()
    assert type(data) == expected


def test_vfetchVogMSA_isIdempotent_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}

    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected_response = client.get(url="/vfetch/vog/msa/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vfetchMSA_ResponseUnder500ms_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["VOG00234", "VOG00003"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vfetch/vog/msa/", params=params)
    end = time.time()

    response_time = end-start
    assert response_time <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vfetchVogMSA_ERROR422_integers(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": [123132]}
    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchVogMSA_ERROR404_randomString(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vfetchVogMSA_ERROR422_noParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchVogMSA_ERROR403_longParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vfetch/vog/msa/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing

#------------------------
# vfetch/protein/faa
#------------------------

# ToDo need to create extra test folder for hmm, mse stuff...
def test_vfetchProteinFAA_FAAProfiles_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected =["11128.NP_150082.1", "2301601.YP_009812740.1"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate

    assert data["id"].to_list() == expected

def test_vfetchProteinFAA_FAAFieldNames_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected = ["id", "seq"]

    data = response.json()
    print(data)
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    print(data)
    assert list(data.keys()) == expected


def test_vfetchProteinFAA_isIdempotent_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected_response = client.get(url="/vfetch/protein/faa/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vfetchProteinFAA_ResponseUnder500ms_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vfetch/protein/faa/", params=params)
    end = time.time()

    response_time = end-start
    assert response_time <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vfetchProteinFAA_ERROR422_integers(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": [123132]}
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchProteinFAA_ERROR404_randomString(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vfetchProteinFAA_ERROR422_noParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchProteinFAA_ERROR403_longParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vfetch/protein/faa/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing


#------------------------
# vfetch/protein/fna
#------------------------

# ToDo need to create extra test folder for hmm, mse stuff...
def test_vfetchProteinFNA_FNAProfiles_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected =["11128.NP_150082.1", "2301601.YP_009812740.1"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate

    assert data["id"].to_list() == expected

def test_vfetchProteinFNA_FNAFieldNames_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected = ["id", "seq"]

    data = response.json()
    print(data)
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    print(data)
    assert list(data.keys()) == expected


def test_vfetchProteinFNA_isIdempotent_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected_response = client.get(url="/vfetch/protein/fna/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vfetchProteinFNA_ResponseUnder500ms_ids(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["11128.NP_150082.1", "2301601.YP_009812740.1"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vfetch/protein/fna/", params=params)
    end = time.time()

    response_time = end-start
    assert response_time <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vfetchProteinFNA_ERROR422_integers(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": [123132]}
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchProteinFNA_ERROR404_randomString(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vfetchProteinFNA_ERROR422_noParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vfetchProteinFNA_ERROR403_longParameter(get_test_client):
    os.chdir("../") #need to change because of the find_vogs_hmm_by_uid() data folder
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vfetch/protein/fna/", params=params)
    expected = 403

    assert response.status_code == expected

# ToDo load testing


#------------------------
# vSearch/protein
#------------------------

def test_vsearchProtein_ProteinProfiles_TaxonIds(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["10295", "10298"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = ["10295", "10298"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    for response_id in data["id"].to_list():
        assert any(expected_val in response_id for expected_val in expected)


def test_vsearchProtein_ProteinProfiles_SpeciesIdAndTaxonId(get_test_client):
    client = get_test_client
    params = {"species_name": ["herpes"], "taxon_id": "10310"}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = ["10310"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    for response_id in data["id"].to_list():
        assert expected[0] in response_id

def test_vsearchProtein_ProteinProfiles_SpeciesIdAndTaxonIdAndVOGId(get_test_client):
    client = get_test_client
    params = {"species_name": ["lacto"], "taxon_id": "37105", "VOG_id":["VOG00001"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = ["37105"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    for response_id in data["id"].to_list():
        assert expected[0] in response_id

def test_vsearchProtein_proteinProfileFieldNames_ids(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["10295", "10298"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = ['id']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsearchProtein_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["10295", "10298"]}

    response = client.get(url="/vsearch/protein/", params=params)
    expected_response = client.get(url="/vsearch/protein/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsearchProtein_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["10295", "10298"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsearch/protein/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vsearchProtein_ERROR422_taxonIDidIntegers(get_test_client):
    client = get_test_client
    params = {"taxon_id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR404_taxonIDrandomString(get_test_client):
    client = get_test_client
    params = {"taxon_id": ["SOMETHING"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsearchProtein_ERROR422_taxonIDnoParameter(get_test_client):
    client = get_test_client
    params = {"taxon_id": None}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR403_taxonIDlongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"taxon_id": long_string }
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 403

    assert response.status_code == expected

def test_vsearchProtein_ERROR422_speciesNameIntegers(get_test_client):
    client = get_test_client
    params = {"species_name": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR404_speciesNamerandomString(get_test_client):
    client = get_test_client
    params = {"species_name": ["SOMETHING"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsearchProtein_ERROR422_speciesNamenoParameter(get_test_client):
    client = get_test_client
    params = {"species_name": None}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR403_speciesNamelongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"species_name": long_string }
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 403

    assert response.status_code == expected

def test_vsearchProtein_ERROR422_VOGIDIntegers(get_test_client):
    client = get_test_client
    params = {"VOG_id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR404_VOGIDrandomString(get_test_client):
    client = get_test_client
    params = {"VOG_id": ["SOMETHING"]}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsearchProtein_ERROR422_VOGIDnoParameter(get_test_client):
    client = get_test_client
    params = {"VOG_id": None}
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchProtein_ERROR403_VOGIDlongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"VOG_id": long_string }
    response = client.get(url="/vsearch/protein/", params=params)
    expected = 403

    assert response.status_code == expected




#------------------------
# vSearch/species
#------------------------

#FIXED TEST!!  taxon_id is a integer and it should be a string
def test_vsearchSpecies_SpeciesProfiles_Ids(get_test_client):
    client = get_test_client
    params = {"ids": [11128, 1335626, 1384461]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = [11128, 1335626, 1384461]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    print(data)
    assert sorted(data["taxon_id"].to_list()) == sorted(expected)

#FIXED TEST!!  taxon_id is a integer and it should be a string
def test_vsearchSpecies_TaxonIDs_AllParameters(get_test_client):
    client = get_test_client
    params = {"ids": [12390, 12348, 1384461],
              "name": "lacto",
              "phage": True,
              "source": "NCBI Refseq",
              "version": 201}
    response = client.get(url="/vsearch/species/", params=params)
    expected = [12390, 12348]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert sorted(data["taxon_id"].to_list()) == sorted(expected)


def test_vsearchSpecies_proteinSpeciesFieldNames_ids(get_test_client):
    client = get_test_client
    params = {"ids": ["10295", "10298"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = ['taxon_id']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsearchSpecies_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"ids": ["10295", "10298"]}

    response = client.get(url="/vsearch/species/", params=params)
    expected_response = client.get(url="/vsearch/species/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsearchSpecies_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"ids": ["10295", "10298"]}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsearch/species/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

#FIXED TEST!!  taxon_id is a integer and it should be a string
def test_vsearchSpecies_ERROR422_idsStrings(get_test_client):
    client = get_test_client
    params = {"ids": ["abcasd", "string"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#FIXED TEST!!  taxon_id is a integer and it should be a string
def test_vsearchSpecies_ERROR404_idsRandomString(get_test_client):
    client = get_test_client
    params = {"ids": ["SOMETHING"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo:# wieso ist None ein invalid parameter.....man kann ja auch ohne Species ID suchen
def test_vsearchSpeciesERROR422_idsNoParameter(get_test_client):
    client = get_test_client
    params = {"ids": None}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo:# also needs to be letters
def test_vsearchSpecies_ERROR403_IdsLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"ids": long_string }
    response = client.get(url="/vsearch/species/", params=params)
    expected = 403

    assert response.status_code == expected

# name

#ToDo: Falscher test, weil ein name kann auch Ziffern enthalten
def test_vsearchSpecies_ERROR422_nameIntegers(get_test_client):
    client = get_test_client
    params = {"name": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchSpecies_ERROR404_nameRandomString(get_test_client):
    client = get_test_client
    params = {"name": ["SOMETHING"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 404

    assert response.status_code == expected

#ToDo:# wieso ist None ein invalid parameter.....man kann ja auch ohne Species ID suchen
def test_vsearchSpeciesERROR422_nameNoParameter(get_test_client):
    client = get_test_client
    params = {"name": None}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo: just not found....
def test_vsearchSpecies_ERROR403_nameLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"name": long_string }
    response = client.get(url="/vsearch/species/", params=params)
    expected = 403

    assert response.status_code == expected

#phage

def test_vsearchSpecies_ERROR422_phageIntegers(get_test_client):
    client = get_test_client
    params = {"phage": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo:# Status code should be 422, not 404, because the parameter is not boolean
def test_vsearchSpecies_ERROR404_phageRandomString(get_test_client):
    client = get_test_client
    params = {"phage": ["SOMETHING"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo:# wieso ist None ein invalid parameter.....man kann ja auch ohne Species ID suchen
def test_vsearchSpeciesERROR422_phageNoParameter(get_test_client):
    client = get_test_client
    params = {"phage": None}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

#ToDo:# Status code should be 422, not 404, because the parameter is not boolean
def test_vsearchSpecies_ERROR403_phageLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"phage": long_string }
    response = client.get(url="/vsearch/species/", params=params)
    expected = 403

    assert response.status_code == expected

# source
#ToDo: FIXED should get not found -> 404, implicit conversion to strings??
def test_vsearchSpecies_ERROR422_sourceIntegers(get_test_client):
    client = get_test_client
    params = {"source": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 404

    assert response.status_code == expected

def test_vsearchSpecies_ERROR404_sourceRandomString(get_test_client):
    client = get_test_client
    params = {"source": ["SOMETHING"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 404

    assert response.status_code == expected

#ToDo:# wieso ist None ein invalid parameter.....man kann ja auch ohne Species ID suchen
def test_vsearchSpeciesERROR422_sourceNoParameter(get_test_client):
    client = get_test_client
    params = {"source": None}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

# ToDo: now just not found for long parameter
def test_vsearchSpecies_ERROR403_sourceLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"source": long_string }
    response = client.get(url="/vsearch/species/", params=params)
    expected = 403

    assert response.status_code == expected

# version

#FIXED: VErsion is an integer -> 404
def test_vsearchSpecies_ERROR422_versionIntegers(get_test_client):
    client = get_test_client
    params = {"version": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 404

    assert response.status_code == expected

#FIXED: VErsion is an integer -> should be 422 error
def test_vsearchSpecies_ERROR404_versionRandomString(get_test_client):
    client = get_test_client
    params = {"version": ["SOMETHING"]}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected


def test_vsearchSpeciesERROR422_versionNoParameter(get_test_client):
    client = get_test_client
    params = {"version": None}
    response = client.get(url="/vsearch/species/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchSpecies_ERROR403_versionLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"version": long_string }
    response = client.get(url="/vsearch/species/", params=params)
    expected = 403

    assert response.status_code == expected





#------------------------
# vSearch/vog
#------------------------
#ToDo change this with inclusive parameter
# taxon_id is a integer and it should be a string
def test_vsearchVOG_VOGIds_VOGIds(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00023", "VOG00234"], "union": "i"}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = ["VOG00001", "VOG00023", "VOG00234"]

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    print(data)
    assert data["id"].to_list() == expected


def test_vsearchVOG_VOGIDs_AllParameters(get_test_client): #not working atm
    client = get_test_client
    params = {"union": "i",
              "pmin": 23,
              "pmax": 100,
              "smax": 50,
              "smin": 1,
              "functional_category": "XrXs",
              "consensus_function": "",
              "mingLCA": 4,
              "maxGLCA": 100,
              "mingGLCA":5,
              "maxgGLCA":99,
              "ancestors": 5,
              "h_stringency": False ,
              "m_stringency": False,
              "l_stringenca": True,
              "virus_specific": True,
              "phages_nonphages":"phages_only",
              "proteins": "",
              "species": "",
              "tax_id": ""}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = ["VOG12390", "VOG12348"]

    data = response.json()
    print("DATAAAAAAAAAAAAAAAAAAAA")
    print(data)
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    print(sorted(data["VOG_UID"].to_list()))
    # assert sorted(data["VOG_UID"].to_list()) == sorted(expected)



def test_vsearchVOG_VOGProfileFieldNames_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00023", "VOG00234"], "inclusive": "i"}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = ['id']

    data = response.json()
    data = pd.DataFrame.from_dict(data) # converting to df so its easier to validate
    assert list(data.keys()) == expected

# ToDo test field types

def test_vsearchVOG_isIdempotent_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00023", "VOG00234"], "inclusive": "i"}

    response = client.get(url="/vsearch/vog/", params=params)
    expected_response = client.get(url="/vsearch/vog/", params=params)

    response_data = response.json()
    expected_data = expected_response.json()

    assert response_data == expected_data


def test_vsearchVOG_ResponseUnder500ms_ids(get_test_client):
    client = get_test_client
    params = {"id": ["VOG00001", "VOG00023", "VOG00234"], "inclusive": "i"}

    expected_time = 0.5
    start = time.time()
    response = client.get(url="/vsearch/vog/", params=params)
    end = time.time()

    assert end-start <= expected_time


#ToDo positiv + optional parameters e.g. sort, limit, skip...

def test_vsearchVOG_ERROR422_idsIntegers(get_test_client):
    client = get_test_client
    params = {"id": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchVOG_ERROR404_idsRandomString(get_test_client):
    client = get_test_client
    params = {"id": ["SOMETHING"]}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsearchVOGERROR422_idsNoParameter(get_test_client):
    client = get_test_client
    params = {"id": None}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 422

    assert response.status_code == expected

def test_vsearchVOG_ERROR403_IdsLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"id": long_string }
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 403

    assert response.status_code == expected

#pmin
def test_vsearchVOG_ERROR422_pminIntegers(get_test_client):
    client = get_test_client
    params = {"pmin": [657567, 123, 124124, 1123]}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 422

    assert response.status_code == expected


def test_vsearchVOG_ERROR404_pminRandomString(get_test_client):
    client = get_test_client
    params = {"pmin": ["SOMETHING"]}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 404

    assert response.status_code == expected


def test_vsearchVOGERROR422_pminNoParameter(get_test_client):
    client = get_test_client
    params = {"pmin": None}
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 422

    assert response.status_code == expected


def test_vsearchVOG_ERROR403_pminLongParameter(get_test_client):
    client = get_test_client
    letters = string.ascii_lowercase
    long_string = [''.join(random.choice(letters) for i in range(10000))]
    params = {"pmin": long_string }
    response = client.get(url="/vsearch/vog/", params=params)
    expected = 403

    assert response.status_code == expected



