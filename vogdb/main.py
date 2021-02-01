import contextlib
import logging
from typing import Dict

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


@api.get("/")
async def root():
    return {"message": "Welcome to VOGDB-API"}


@api.get("/vsearch/species", response_model=List[Species_ID])
async def search_species(
                   ids: Optional[Set[int]] = Query(None),
                   name: Optional[str] = None,
                   phage: Optional[bool] = None,
                   source: Optional[str] = None,
                   db: Session = Depends(get_db)):
    """
    This functions searches a database and returns a list of species IDs for records in that database
    which meet the search criteria.
    \f
    :return: A List of Species IDs
    """

    with error_handling():
        log.debug("Received a vsearch/species request with parameters: {0}".format(locals()))

        species = get_species(db, ids, name, phage, source)

        if not species:
            log.debug("No Species match the search criteria.")
        else:
            log.debug("Species have been retrieved.")

        return species


@api.get("/vsummary/species", response_model=List[Species_profile])
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


@api.post("/vsummary/species", response_model=List[Species_profile])
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
               pmin: Optional[int] = None,
               pmax: Optional[int] = None,
               smax: Optional[int] = None,
               smin: Optional[int] = None,
               functional_category: Optional[Set[str]] = Query(None),
               consensus_function: Optional[Set[str]] = Query(None),
               mingLCA: Optional[int] = None,
               maxgLCA: Optional[int] = None,
               mingGLCA: Optional[int] = None,
               maxgGLCA: Optional[int] = None,
               ancestors: Optional[Set[str]] = Query(None),
               h_stringency: Optional[bool] = None,
               m_stringency: Optional[bool] = None,
               l_stringency: Optional[bool] = None,
               virus_specific: Optional[bool] = None,
               phages_nonphages: Optional[str] = None,
               proteins: Optional[Set[str]] = Query(None),
               species: Optional[Set[str]] = Query(None),
               tax_id: Optional[Set[int]] = Query(None),
               sort: Optional[str] = 'VOG_ID',
               union: Optional[bool] = None,
               db: Session = Depends(get_db)):
    """
    This functions searches a database and returns a list of vog unique identifiers (UIDs) for records in that database
    which meet the search criteria.
    \f
    :return: A List of VOG IDs
    """
    with error_handling():
        log.info("GET request vsearch/vog")
        log.debug("Received a vsearch/vog request with parameters: {0}".format(locals()))

        vogs = get_vogs(db, models.VOG_profile.id, id, pmin, pmax, smax, smin, functional_category, consensus_function,
                        mingLCA, maxgLCA, mingGLCA, maxgGLCA, ancestors, h_stringency, m_stringency, l_stringency,
                        virus_specific, phages_nonphages, proteins, species, tax_id, sort, union)

        if not vogs:
            log.info("No VOGs match the search criteria.")
        else:
            log.info("VOGs have been retrieved.")
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
        log.debug("Received a vsummary/vog get with parameters: {0}".format(id))

        vog_summary = find_vogs_by_uid(db, id)
        if not vog_summary:
            log.error("No matching VOGs found")
            raise HTTPException(status_code=404, detail="No matching VOGs found")
        else:
            log.info("VOG summaries have been retrieved.")
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
        log.info("GET request vfetch/vog/hmm")
        log.debug("Received a vfetch/vog/hmm request with parameters: {0}".format(id))

        vog_hmm = find_vogs_hmm_by_uid(id)

        if not vog_hmm:
            log.error("No HMM found.")
            raise HTTPException(status_code=404, detail="No HMM found.")
        else:
            log.info("HMM search successful.")
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
        log.info("GET request vfetch/vog/msa")
        log.debug("Received a vfetch/vog/msa request with parameters: {0}".format(id))

        vog_msa = find_vogs_msa_by_uid(id)

        if not vog_msa:
            log.error("No HMM found.")
            raise HTTPException(status_code=404, detail="No MSA found.")
        else:
            log.info("MSA search successful.")
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
async def search_protein(species_name: Optional[Set[str]] = Query(None),
                         taxon_id: Optional[Set[int]] = Query(None),
                         VOG_id: Optional[Set[str]] = Query(None),
                         sort: Optional[str] = 'ProteinID',
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
        log.info("GET request vsearch/protein")
        log.debug("Received a vsearch/protein request with parameters: {0}".format(locals()))

        proteins = get_proteins(db, models.Protein_profile.id, species_name, taxon_id, VOG_id, sort)

        if not proteins:
            log.info("No Proteins match the search criteria.")
        else:
            log.info("Proteins have been retrieved.")
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
        log.info("GET request vsummary/protein")
        log.debug("Received a vsummary/protein request with parameters: {0}".format(locals()))

        protein_summary = find_proteins_by_id(db, id)

        if not len(protein_summary) == len(id):
            log.warning("At least one of the proteins was not found, or there were duplicates.\n"
                        "IDs given: {0}".format(id))

        if not protein_summary:
            log.error("No matching Proteins found")
            raise HTTPException(status_code=404, detail="No matching Proteins found")
        else:
            log.info("Proteins summaries have been retrieved.")
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


@api.get("/vfetch/protein/faa", response_model=List[AA_seq])
async def get_fetch_protein_faa(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns Amino acid sequences for the proteins specified by the protein IDs
    \f
    :param id: ProteinID(s)
    :param db: database session dependency
    :return: Amino acid sequences for the proteins
    """
    with error_handling():
        log.info("GET request vfetch/protein/faa")
        log.debug("Received a vfetch/protein/faa request with parameters: {0}".format(locals()))

        protein_faa = find_protein_faa_by_id(db, id)
    # except Exception as exc:
    #     log.error("AA fetching not successful. IDs: {0}".format(id))
    #     log.error(exc)
    #     raise HTTPException(status_code=400, detail="Amino Acid retrieval not successful. {0}".format(exc))

        if not len(protein_faa) == len(id):
            log.warning("At least one of the proteins was not found, or there were duplicates.\n"
                        "IDs given: {0}".format(id))

        if not protein_faa:
            log.error("No Proteins found with the given IDs")
            raise HTTPException(status_code=404, detail="No Proteins found with the given IDs")
        else:
            log.info("Aminoacid sequences have been retrieved.")
        return protein_faa


@api.post("/vfetch/protein/faa", response_model=List[AA_seq])
async def post_fetch_protein_faa(body: List[ProteinID], db: Session = Depends(get_db)):
    """
    This function returns Amino acid sequences for the proteins specified by the protein IDs
    \f
    :param body: proteinIDs as returned from search_protein
    :param db: database session dependency
    :return: Amino acid sequences for the proteins
    """
    return await get_fetch_protein_faa([p.id for p in body], db)


@api.get("/vfetch/protein/fna", response_model=List[NT_seq])
async def get_fetch_protein_fna(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns Nucleotide sequences for the genes specified by the protein IDs
    \f
    :param id: ProteinID(s)
    :param db: database session dependency
    :return: Nucleotide sequences for the proteins
    """
    with error_handling():
        log.info("GET request vfetch/protein/fna")
        log.debug("Received a vfetch/protein/fna request with parameters: {0}".format(locals()))

        protein_fna = find_protein_faa_by_id(db, id)
    # except Exception as exc:
    #     log.error("NT fetching not successful. IDs: {0}".format(id))
    #     log.error(exc)
    #     raise HTTPException(status_code=400, detail="Nucleotide retrieval not successful. {0}".format(exc))

        if not len(protein_fna) == len(id):
            log.warning("At least one of the proteins was not found, or there were duplicates.\n"
                        "IDs given: {0}".format(id))

        if not protein_fna:
            log.error("No Proteins found with the given IDs")
            raise HTTPException(status_code=404, detail="No Proteins found with the given ID")
        else:
            log.info("Nucleotide sequences have been retrieved.")
        return protein_fna

@api.post("/vfetch/protein/fna", response_model=List[NT_seq])
async def post_fetch_protein_fna(body: List[ProteinID], db: Session = Depends(get_db)):
    """
    This function returns Nucleotide sequences for the genes specified by the protein IDs
    \f
    :param body: proteinIDs as returned from search_protein
    :param db: database session dependency
    :return: Nucleotide sequences for the proteins
    """
    return await get_fetch_protein_fna([p.id for p in body], db)

