import os
import gzip

from sqlalchemy.orm import Session
from sqlalchemy import func
from . import models
from typing import Optional, Set, List
from .taxa import ncbi_taxa
import logging

# get logger:
log = logging.getLogger(__name__)

"""
Here we define all the search methods that are used for extracting the data from the database
"""

"""
Very important Note: Here we specify what columns we want to get from our query: e.g. protein_id,..,species_name
In order that this result output is gonna pass through the Pydantic validation, two criteria need to be valid:
1. the attribute type values of the returned query object (in functionality.py)  (e.g. Species_profile.species_name)
 need to match the attribute type of the Pydantic response model (in this case schemas.Species_profile.species_name)
2. The names of the  attributes the returned query object also need to be exactly the same as in the Pydantic 
response model object, so we have in query object with attribute Protein_profile.species_name
so the pydantic response model (Protein_profile) needs to have the attribute name species_name as well

if those two criteria are not fulfilled, pydantic will throw an ValidationError

"""


def get_species(db: Session,
                response_body,
                taxon_id: Optional[Set[int]],
                species_name: Optional[str],
                phage: Optional[bool],
                source: Optional[str],
                version: Optional[int],
                sort: Optional[str]):
    """
    This function searches the Species based on the given query parameters
    """
    log.info("Searching Species in the database...")

    result = db.query(response_body)
    arguments = locals()
    filters = []

    for key, value in arguments.items():  # type: str, any
        if value is not None:
            if key == "taxon_id":
                filters.append(getattr(models.Species_profile, key).in_(value))

            if key == "species_name":
                value = "%" + value + "%"
                filters.append(getattr(models.Species_profile, key).like(value))

            if key == "phage":
                filters.append(getattr(models.Species_profile, key).is_(value))

            if key == "source":
                value = "%" + value + "%"
                filters.append(getattr(models.Species_profile, key).like(value))

            if key == "version":
                filters.append(getattr(models.Species_profile, key) == value)

    result = result.filter(*filters).order_by(sort)

    return result.all()


def find_species_by_id(db: Session, ids: Optional[List[int]]):
    """
    This function returns the Species information based on the given species IDs
    """
    if ids:
        log.info("Searching Species by IDs in the database...")
        results = db.query(models.Species_profile).filter(models.Species_profile.taxon_id.in_(ids)).all()
        return results
    else:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given.")


def get_vogs(db: Session,
             response_body,
             id: Optional[Set[str]],
             pmin: Optional[int],
             pmax: Optional[int],
             smax: Optional[int],
             smin: Optional[int],
             function: Optional[Set[str]],
             consensus_function: Optional[Set[str]],
             mingLCA: Optional[int],
             maxgLCA: Optional[int],
             mingGLCA: Optional[int],
             maxgGLCA: Optional[int],
             ancestors: Optional[Set[str]],
             h_stringency: Optional[bool],
             m_stringency: Optional[bool],
             l_stringency: Optional[bool],
             virus_specific: Optional[bool],
             phages_nonphages: Optional[str],
             proteins: Optional[Set[str]],
             species: Optional[Set[str]],
             tax_id: Optional[Set[int]],
             sort: Optional[str],
             union: Optional[bool],
             ):
    """
    This function searches the VOG based on the given query parameters
    """
    log.info("Searching VOGs in the database...")

    result = db.query(response_body)
    arguments = locals()
    filters = []

    # make checks for validity of user input:
    def check_validity(pair):
        min = pair[0]
        max = pair[1]
        if (min is not None) and (max is not None):
            if max < min:
                # ToDo value error.
                raise ValueError("The provided min is greater than the provided max.")
            elif min < 0 or max < 0:
                raise ValueError("Number for min or max cannot be negative!")

    for pair in [[smin, smax], [pmin, pmax], [mingLCA, maxgLCA], [mingGLCA, maxgGLCA]]:
        check_validity(pair)

    for number in smin, smax, pmin, pmax, mingLCA, maxgLCA, mingGLCA, maxgGLCA:
        if number is not None:
            if number < 1:
                raise ValueError('Provided number: %s has to be > 0.' % number)

    # create a warning in the log file if "union" is specified but no species/taxIDs given to use the parameter
    #ToDo: What type of error here?
    if union is True:
        if species is None and tax_id is None:
            log.error("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
            raise Exception("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
        elif species is not None and len(species) < 2:
            log.error("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
            raise Exception("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
        elif tax_id is not None and len(tax_id) < 2:
            log.error("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")
            raise Exception("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")


    for key, value in arguments.items():  # type: str, any
        if value is not None:
            if key == "id":
                filters.append(getattr(models.VOG_profile, key).in_(value))

            if key == "consensus_function":
                for fct_d in value:
                    d = "%" + fct_d + "%"
                    filters.append(getattr(models.VOG_profile, key).like(d))

            if key == "function":
                for fct_d in value:
                    d = "%" + fct_d + "%"
                    filters.append(getattr(models.VOG_profile, key).like(d))

            if key == "smax":
                filters.append(getattr(models.VOG_profile, "species_count") < value + 1)

            if key == "smin":
                filters.append(getattr(models.VOG_profile, "species_count") > value - 1)

            if key == "pmax":
                filters.append(getattr(models.VOG_profile, "protein_count") < value + 1)

            if key == "pmin":
                filters.append(getattr(models.VOG_profile, "protein_count") > value - 1)

            if key == "proteins":
                for protein in value:
                    p = "%" + protein + "%"
                    filters.append(getattr(models.VOG_profile, key).like(p))

            if key == "species":
                if union is False:
                    # this is the INTERSECTION SEARCH:
                    vog_ids = db.query().with_entities(models.Protein_profile.vog_id).join(models.Species_profile). \
                        filter(models.Species_profile.species_name.in_(species)).group_by(
                        models.Protein_profile.vog_id). \
                        having(func.count(models.Species_profile.species_name) == len(species)).all()
                else:
                    # UNION SEARCH below:
                    vog_ids = db.query().with_entities(models.Protein_profile.vog_id).join(models.Species_profile). \
                        filter(models.Species_profile.species_name.in_(species)).group_by(
                        models.Protein_profile.vog_id).all()
                vog_ids = {id[0] for id in vog_ids}  # convert to set
                filters.append(getattr(models.VOG_profile, "id").in_(vog_ids))

            if key == "maxgLCA":
                filters.append(getattr(models.VOG_profile, "genomes_total_in_LCA") < value + 1)

            if key == "mingLCA":
                filters.append(getattr(models.VOG_profile, "genomes_total_in_LCA") > value - 1)

            if key == "maxgGLCA":
                filters.append(getattr(models.VOG_profile, "genomes_in_group") < value + 1)

            if key == "mingGLCA":
                filters.append(getattr(models.VOG_profile, "genomes_in_group") > value - 1)

            if key == "ancestors":
                for anc in value:
                    a = "%" + anc + "%"
                    filters.append(getattr(models.VOG_profile, key).like(a))

            if key == "h_stringency":
                filters.append(getattr(models.VOG_profile, key).is_(value))

            if key == "m_stringency":
                filters.append(getattr(models.VOG_profile, key).is_(value))

            if key == "l_stringency":
                filters.append(getattr(models.VOG_profile, key).is_(value))

            if key == "virus_specific":
                filters.append(getattr(models.VOG_profile, key).is_(value))

            if key == "phages_nonphages":
                val = "%" + value + "%"
                filters.append(getattr(models.VOG_profile, key).like(val))

            if key == "tax_id":
                ncbi = ncbi_taxa()
                try:
                    id_list = []
                    if union:
                        # UNION SEARCH:
                        for id in tax_id:
                            id_list.extend(
                                ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))
                            id_list.append(id)
                        vog_ids = db.query().with_entities(models.Protein_profile.vog_id).join(
                            models.Species_profile). \
                            filter(models.Species_profile.taxon_id.in_(id_list)).group_by(
                            models.Protein_profile.vog_id). \
                            filter(models.Species_profile.taxon_id.in_(id_list)).group_by(
                            models.Protein_profile.vog_id).all()
                        vog_ids = {id[0] for id in vog_ids}  # convert to set
                        filters.append(getattr(models.VOG_profile, "id").in_(vog_ids))
                    else:
                        # INTERSECTION SEARCH:
                        for id in tax_id:
                            id_list1 = []
                            id_list1.extend(
                                ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))
                            id_list1.append(id)
                            vog_ids = db.query().with_entities(models.Protein_profile.vog_id).join(
                                models.Species_profile). \
                                filter(models.Species_profile.taxon_id.in_(id_list1)).group_by(
                                models.Protein_profile.vog_id). \
                                filter(models.Species_profile.taxon_id.in_(id_list1)).group_by(
                                models.Protein_profile.vog_id).all()
                            vog_ids = {id[0] for id in vog_ids}  # convert to set
                            filters.append(getattr(models.VOG_profile, "id").in_(vog_ids))
                except ValueError as e:
                    raise ValueError("The provided taxonomy ID is invalid: {0}".format(id))

    result = result.filter(*filters).order_by(sort)

    return result.all()


def find_vogs_by_uid(db: Session, ids: Optional[List[str]]):
    """
    This function returns the VOG information based on the given VOG IDs
    """

    if ids:
        log.info("Searching VOGs by IDs in the database...")
        results = db.query(models.VOG_profile).filter(models.VOG_profile.id.in_(ids)).all()
        return results
    else:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given.")


def get_proteins(db: Session,
                 response_body,
                 species: Optional[Set[str]],
                 taxon_id: Optional[Set[int]],
                 vog_id: Optional[Set[str]],
                 sort: Optional[str]):
    """
    This function searches the for proteins based on the given query parameters
    """
    log.info("Searching Proteins in the database...")

    result = db.query(response_body)
    arguments = locals()
    filters = []

    for key, value in arguments.items():  # type: str, any
        if value is not None:
            if key == "species":
                s_res = []
                for s in species:
                    search = "%" + s + "%"
                    res = db.query().with_entities(models.Protein_profile.id,
                                                   models.Protein_profile.vog_id,
                                                   models.Protein_profile.taxon_id,
                                                   models.Species_profile.species_name).join(
                        models.Species_profile). \
                        filter(models.Species_profile.species_name.like(search)).all()
                    s_res.extend(res)
                s_res = {id[0] for id in s_res}  # convert to set
                filters.append(getattr(models.Protein_profile, "id").in_(s_res))

            if key == "taxon_id":
                filters.append(getattr(models.Protein_profile, key).in_(value))

            if key == "vog_id":
                filters.append(getattr(models.Protein_profile, key).in_(value))

    result = result.filter(*filters).order_by(sort)

    return result.all()


def find_proteins_by_id(db: Session, pids: Optional[List[str]]):
    """
    This function returns the Protein information based on the given Protein IDs
    """
    if pids:
        log.info("Searching Proteins by ProteinIDs in the database...")
        results = db.query().with_entities(models.Protein_profile.id,
                                           models.Protein_profile.vog_id,
                                           models.Protein_profile.taxon_id,
                                           models.Species_profile.species_name).join(models.Species_profile). \
            filter(models.Protein_profile.id.in_(pids)).all()
        return results
    else:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given")


def find_vogs_hmm_by_uid(uid):
    log.info("Searching for Hidden Markov Models (HMM) in the data files...")

    if not uid:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given")

    return [_hmm_content(id) for id in set(uid)]


def _hmm_content(uid: str) -> str:
    try:
        return _load_gzipped_file_content(uid.upper(), "hmm", ".hmm.gz")
    except FileNotFoundError:
        log.exception(f"No HMM for {uid}")
        raise ValueError(f"Invalid Id {uid}")


def find_vogs_msa_by_uid(uid):
    log.info("Searching for Multiple Sequence Alignments (MSA) in the data files...")

    if not uid:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given")

    return [_msa_content(id) for id in set(uid)]


def _msa_content(uid: str) -> str:
    try:
        return _load_gzipped_file_content(uid.upper(), "raw_algs", ".msa.gz")
    except FileNotFoundError:
        log.exception(f"No MSA for {uid}")
        raise ValueError(f"Invalid Id {uid}")


def find_protein_faa_by_id(db: Session, id: Optional[List[str]]):
    """
    This function returns the Aminoacid sequences of the proteins based on the given Protein IDs
    """
    if id:
        log.info("Searching AA sequence by ProteinIDs in the database...")
        results = db.query(models.AA_seq).filter(models.AA_seq.id.in_(id)).all()
        return results
    else:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given.")


def find_protein_fna_by_id(db: Session, id: Optional[List[str]]):
    """
    This function returns the Nucleotide sequences of the proteins based on the given Protein IDs
    """
    if id:
        log.info("Searching NT sequence by ProteinIDs in the database...")
        results = db.query(models.AA_seq).filter(models.AA_seq.id.in_(id)).all()
        return results
    else:
        log.error("No IDs were given.")
        raise ValueError("No IDs were given.")


def _load_gzipped_file_content(id: str, prefix: str, suffix: str) -> str:
    file_name = os.path.join(os.environ.get("VOG_DATA", "data"), prefix, id + suffix)
    with gzip.open(file_name, "rt") as f:
        return f.read()

