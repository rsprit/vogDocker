import contextlib
import logging
from typing import Dict, Set, Optional, List

from .functionality import *
from .database import SessionLocal
from sqlalchemy.orm import Session
from fastapi import Depends, FastAPI, Query, Path, HTTPException
from fastapi.responses import PlainTextResponse
from .schemas import *
from . import models


# configuring logging
# ToDo: Take out file name to log to console, then have the docker container create a log file
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s- %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s- %(funcName)s: %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S', filename="../vogdb/vogapi.log", filemode='w')

# get logger:
log = logging.getLogger(__name__)

api = FastAPI()


@contextlib.contextmanager
def error_handling():
    try:
        yield
    except HTTPException:
        raise
    except (ValueError, KeyError, AttributeError) as e:
        log.exception("Bad request")
        raise HTTPException(400, str(e)) from e
#    except AttributeError as e:
#        log.exception("Unprocessable entity")
#        raise HTTPException(422, str(e)) from e
    except Exception as e:
        log.exception("Internal server error")
        raise HTTPException(500, str(e)) from e


# Dependency. Connect to the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.get("/", summary="Welcome")
async def root():
    return {"message": "Welcome to VOGDB-API"}


@api.get("/vsearch/species", response_model=List[Species_ID], summary="Species search")
async def search_species(
                   taxon_id: List[int] = Query(None),
                   name: List[str] = Query(None),
                   phage: Optional[bool] = Query(None),
                   source: Optional[str] = Query(None),
                   db: Session = Depends(get_db)):
    """
    This functions searches a database and returns a list of species IDs for records in that database
    which meet the search criteria.
    \f
    :return: A List of Species IDs
    """

    with error_handling():
        log.debug("Received a vsearch/species request")

        species = get_species(db, taxon_id, name, phage, source)

        if not species:
            log.debug("No Species match the search criteria.")
        else:
            log.debug("Species have been retrieved.")

        return species


@api.get("/vsummary/species", response_model=List[Species_profile], summary="Species summary")
async def get_summary_species(taxon_id: List[int] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns Species summaries for a list of taxon ids
    \f
    :param taxon_id: Taxon ID
    :param db: database session dependency
    :return: Species summary
    """

    with error_handling():
        log.debug("Received a vsummary/species GET with parameters: taxon_id = {0}".format(taxon_id))

        species_summary = find_species_by_id(db, taxon_id)

        if len(species_summary) != len(taxon_id):
            log.warning("At least one of the species was not found, or there were duplicates.\n"
                        "IDs given: {0}".format(taxon_id))
        else:
            log.info("Species summaries have been retrieved.")

        return species_summary


@api.post("/vsummary/species", response_model=List[Species_profile], summary="Species summary")
async def post_summary_species(body: List[Species_ID], db: Session = Depends(get_db)):
    """
    This function returns Species summaries for a list of taxon ids
    \f
    :param body: list of taxon ids as returned from search species
    :param db: database session dependency
    :return: list of Species profiles
    """

    return await get_summary_species([s.taxon_id for s in body], db)


@api.get("/vsearch/vog", response_model=List[VOG_UID])
async def search_vog(
               id: Optional[Set[str]] = Query(None),
               pmin: Optional[int] = Query(None),
               pmax: Optional[int] = Query(None),
               smax: Optional[int] = Query(None),
               smin: Optional[int] = Query(None),
               functional_category: Optional[Set[str]] = Query(None),
               consensus_function: Optional[Set[str]] = Query(None),
               mingLCA: Optional[int] = Query(None),
               maxgLCA: Optional[int] = Query(None),
               mingGLCA: Optional[int] = Query(None),
               maxgGLCA: Optional[int] = Query(None),
               ancestors: Optional[Set[str]] = Query(None),
               h_stringency: Optional[bool] = Query(None),
               m_stringency: Optional[bool] = Query(None),
               l_stringency: Optional[bool] = Query(None),
               virus_specific: Optional[bool] = Query(None),
               phages_nonphages: Optional[str] = Query(None),
               proteins: Optional[Set[str]] = Query(None),
               species: Optional[Set[str]] = Query(None),
               tax_id: Optional[Set[int]] = Query(None),
               union: Optional[bool] = None,
               db: Session = Depends(get_db)):
    """
    This functions searches a database and returns a list of vog unique identifiers (UIDs) for records in that database
    which meet the search criteria.
    \f
    :return: A List of VOG IDs
    """
    with error_handling():
        log.debug("Received a vsearch/vog request")

        vogs = get_vogs(db, id, pmin, pmax, smax, smin, functional_category, consensus_function,
                        mingLCA, maxgLCA, mingGLCA, maxgGLCA, ancestors, h_stringency, m_stringency, l_stringency,
                        virus_specific, phages_nonphages, proteins, species, tax_id, union)

        if not vogs:
            log.debug("No VOGs match the search criteria.")
        else:
            log.debug("VOGs have been retrieved.")
        return vogs


@api.get("/vsummary/vog", response_model=List[VOG_profile])
async def get_summary_vog(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns vog summaries for a list of unique identifiers (UIDs)
    \f
    :param id: list of VOGIDs
    :param db: database session dependency
    :return: list of VOG_profile
    """
    with error_handling():
        log.debug("Received a vsummary/vog request")

        vog_summary = find_vogs_by_uid(db, id)

        if not vog_summary:
            log.debug("No matching VOGs found")
        else:
            log.debug("VOG summaries have been retrieved.")

        return vog_summary


@api.post("/vsummary/vog", response_model=List[VOG_profile])
async def post_summary_vog(body: List[VOG_UID], db: Session = Depends(get_db)):
    """
    This function returns vog summaries for a list of unique identifiers (UIDs)
    \f
    :param body: list of VOG uids as returned from search_vog
    :param db: database session dependency
    :return: vog summary
    """
    return await get_summary_vog([vog.id for vog in body], db)


@api.get("/vfetch/vog/hmm", response_model=Dict[str,str])
async def get_fetch_vog_hmm(id: List[str] = Query(None)):
    """
    This function returns the Hidden Markov Matrix (HMM) for a list of unique identifiers (UIDs)
    \f
    :param id: VOGID
    :return: vog data (HMM profile)
    """
    with error_handling():
        log.debug("Received a vfetch/vog/hmm request")

        vog_hmm = find_vogs_hmm_by_uid(id)

        if not vog_hmm:
            log.debug("No HMM found.")
        else:
            log.debug("HMM search successful.")
        return vog_hmm


@api.post("/vfetch/vog/hmm", response_model=Dict[str,str])
async def post_fetch_vog_hmm(body: List[VOG_UID]):
    """
    This function returns the Hidden Markov Matrix (HMM) for a list of unique identifiers (UIDs)
    \f
    :param body: list of VOG uids as returned from search_vog
    :return: vog data (HMM profile)
    """
    return await get_fetch_vog_hmm([vog.id for vog in body])


@api.get("/vfetch/vog/msa", response_model=Dict[str,str])
async def get_fetch_vog_msa(id: List[str] = Query(None)):
    """
    This function returns the Multiple Sequence Alignment (MSA) for a list of unique identifiers (UIDs)
    \f
    :param id: VOGID
    :return: vog data (MSA)
    """
    with error_handling():
        log.debug("Received a vfetch/vog/msa request")

        vog_msa = find_vogs_msa_by_uid(id)

        if not vog_msa:
            log.debug("No HMM found.")
        else:
            log.debug("MSA search successful.")
        return vog_msa


@api.post("/vfetch/vog/msa", response_model=Dict[str,str])
async def post_fetch_vog_msa(body: List[VOG_UID]):
    """
    This function returns the Multiple Sequence Alignment (MSA) for VOGs
    \f
    :param body: list of VOG uids as returned from search_vog
    :return: vog data (HMM profile)
    """
    return await get_fetch_vog_msa([vog.id for vog in body])


@api.get("/vplain/vog/hmm/{id}", response_class=PlainTextResponse, summary="Get the HMM")
async def plain_vog_hmm(id: str = Path(..., title="VOG id", min_length=8, regex="^VOG\d+$")):
    """
    Get the Hidden Markov Matrix of the given VOG as plain text.
    \f
    :param vog: VOGID
    """

    with error_handling():
        try:
            return PlainTextResponse(hmm_content(id))
        except KeyError:
            raise HTTPException(404, "Not found")


@api.get("/vplain/vog/msa/{id}", response_class=PlainTextResponse, summary="Get the MSA")
async def plain_vog_msa(id: str = Path(..., title="VOG id", min_length=8, regex="^VOG\d+$")):
    """
    Get the Multiple Sequence Alignment of the given VOG as plain text.
    \f
    :param vog: VOGID
    """

    with error_handling():
        try:
            return PlainTextResponse(msa_content(id))
        except KeyError:
            raise HTTPException(404, "Not found")


@api.get("/vsearch/protein", response_model=List[ProteinID])
async def search_protein(species_name: List[str] = Query(None),
                         taxon_id: List[int] = Query(None),
                         VOG_id: List[str] = Query(None),
                         db: Session = Depends(get_db)):
    """
    This functions searches a database and returns a list of Protein IDs for records in the database
    matching the search criteria.
    \f
    :param: species_name: full or partial name of a species
    :param: taxon_id: Taxnonomy ID of a species
    :param: VOG_id: ID of the VOG(s)
    :return: A List of Protein IDs
    """
    with error_handling():
        log.debug("Received a vsearch/protein request")

        proteins = get_proteins(db, species_name, taxon_id, VOG_id)

        if not proteins:
            log.debug("No Proteins match the search criteria.")
        else:
            log.debug("Proteins have been retrieved.")

        return proteins


@api.get("/vsummary/protein", response_model=List[Protein_profile])
async def get_summary_protein(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns protein summaries for a list of Protein identifiers (pids)
    \f
    :param id: proteinID
    :param db: database session dependency
    :return: protein summary
    """
    with error_handling():
        log.debug("Received a vsummary/protein request")

        protein_summary = find_proteins_by_id(db, id)

        if not protein_summary:
            log.debug("No matching Proteins found")
        else:
            log.debug("Proteins summaries have been retrieved.")
            
        return protein_summary


@api.post("/vsummary/protein", response_model=List[Protein_profile])
async def post_summary_protein(body: List[ProteinID], db: Session = Depends(get_db)):
    """
    This function returns protein summaries for a list of Protein identifiers (pids)
    \f
    :param body: proteinIDs as returned from search_protein
    :param db: database session dependency
    :return: list of protein summaries
    """
    return await get_summary_protein([p.id for p in body], db)
