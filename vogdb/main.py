import contextlib
import sys
from fastapi import HTTPException
from .functionality import *
from .database import SessionLocal
from sqlalchemy.orm import Session
from fastapi import Depends, FastAPI, Query
from .schemas import *
from . import models
import logging


# get logger:
log = logging.getLogger(__name__)

api = FastAPI()


@contextlib.contextmanager
def error_handling():
    try:
        yield
    except HTTPException:
        raise
    except (ValueError, KeyError) as e:
        log.exception("Bad request")
        raise HTTPException(400, str(e)) from e
    except AttributeError as e:
        log.exception("Unprocessable entity")
        raise HTTPException(422, str(e)) from e
    except Exception as e:
        log.exception("Internal server error")
        raise HTTPException(500, str(e)) from e


# uncomment when we have a domain
# redirected_app = HTTPToHTTPSRedirectMiddleware(api, host="example_domain.com")


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


@api.get("/vsearch/species/",
         response_model=List[Species_ID])
def search_species(db: Session = Depends(get_db),
                   ids: Optional[Set[int]] = Query(None),
                   name: Optional[str] = None,
                   phage: Optional[bool] = None,
                   source: Optional[str] = None,
                   version: Optional[int] = None,
                   sort: Optional[str] = 'SpeciesName'):
    """
    This functions searches a database and returns a list of species IDs for records in that database
    which meet the search criteria.
    :return: A List of Species IDs
    """

    with error_handling():
        log.info("GET request vsearch/species")
        log.debug("Received a vsearch/species request with parameters: {0}".format(locals()))

        species = get_species(db, models.Species_profile.taxon_id, ids, name, phage, source, version, sort)

        if not species:
            log.info("No Species match the search criteria.")
            # raise HTTPException(status_code=404, detail="No Species match the search criteria.")
        else:
            log.info("Species have been retrieved.")
        return species


@api.get("/vsummary/species/",
         response_model=List[Species_profile])
async def get_summary_species(taxon_id: Optional[List[int]] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns Species summaries for a list of taxon ids
    :param taxon_id: Taxon ID
    :param db: database session dependency
    :return: Species summary
    """

    with error_handling():
        log.info("GET request vsummary/species")
        log.debug("Received a vsummary/species request with parameters: taxon_id = {0}".format(taxon_id))

        species_summary = find_species_by_id(db, taxon_id)

        if not len(species_summary) == len(taxon_id):
            log.warning("At least one of the species was not found, or there were duplicates.\n"
                        "IDs given: {0}".format(taxon_id))

        if not species_summary:
            log.error("No matching Species found")
            raise HTTPException(status_code=404, detail="No matching Species found")
        else:
            log.info("Species summaries have been retrieved.")
        return species_summary


@api.get("/vsearch/vog/",
         response_model=List[VOG_UID])
def search_vog(db: Session = Depends(get_db),
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
               union: Optional[bool] = None):
    """
    This functions searches a database and returns a list of vog unique identifiers (UIDs) for records in that database
    which meet the search criteria.
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


@api.get("/vsummary/vog/",
         response_model=List[VOG_profile])
async def get_summary_vog(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns vog summaries for a list of unique identifiers (UIDs)
    :param id: VOGID
    :param db: database session dependency
    :return: vog summary
    """
    with error_handling():
        log.info("GET request vsummary/vog")
        log.debug("Received a vsummary/vog request with parameters: {0}".format(locals()))

        vog_summary = find_vogs_by_uid(db, id)
        if not vog_summary:
            log.error("No matching VOGs found")
            raise HTTPException(status_code=404, detail="No matching VOGs found")
        else:
            log.info("VOG summaries have been retrieved.")
        return vog_summary


@api.get("/vsearch/protein/",
         response_model=List[ProteinID])
async def search_protein(db: Session = Depends(get_db),
                         species_name: Optional[Set[str]] = Query(None),
                         taxon_id: Optional[Set[int]] = Query(None),
                         VOG_id: Optional[Set[str]] = Query(None),
                         sort: Optional[str] = 'ProteinID'):
    """
    This functions searches a database and returns a list of Protein IDs for records in the database
    matching the search criteria.
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


@api.get("/vsummary/protein/",
         response_model=List[Protein_profile])
async def get_summary_protein(id: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns protein summaries for a list of Protein identifiers (pids)
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


@api.get("/vfetch/vog/hmm")
async def fetch_vog(id: List[str] = Query(None)):
    """
    This function returns the Hidden Markov Matrix (HMM) for a list of unique identifiers (UIDs)
    :param id: VOGID
    :return: vog data (HMM profile)
    """
    with error_handling():
        log.info("GET request vfetch/vog/hmm")
        log.debug("Received a vfetch/vog/hmm request with parameters: {0}".format(locals()))

        vog_hmm = find_vogs_hmm_by_uid(id)

        if not vog_hmm:
            log.error("No HMM found.")
            raise HTTPException(status_code=404, detail="No HMM found.")
        else:
            log.info("HMM search successful.")
        return vog_hmm


@api.get("/vfetch/vog/msa")
async def fetch_vog(id: List[str] = Query(None)):
    """
    This function returns the Multiple Sequence Alignment (MSA) for a list of unique identifiers (UIDs)
    :param id: VOGID
    :param db: database session dependency
    :return: vog data (MSA)
    """
    with error_handling():
        log.info("GET request vfetch/vog/msa")
        log.debug("Received a vfetch/vog/msa request with parameters: {0}".format(locals()))

        vog_msa = find_vogs_msa_by_uid(id)

        if not vog_msa:
            log.error("No HMM found.")
            raise HTTPException(status_code=404, detail="No MSA found.")
        else:
            log.info("MSA search successful.")
        return vog_msa


@api.get("/vfetch/protein/faa",
         response_model=List[AA_seq])
async def fetch_protein_faa(db: Session = Depends(get_db), id: List[str] = Query(None)):
    """
    This function returns Amino acid sequences for the proteins specified by the protein IDs
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


@api.get("/vfetch/protein/fna",
         response_model=List[NT_seq])
async def fetch_protein_fna(db: Session = Depends(get_db), id: List[str] = Query(None)):
    """
    This function returns Nucleotide sequences for the genes specified by the protein IDs
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
